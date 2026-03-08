"""
Deezer Direct Discovery Worker.

Discovers tracks from Deezer and inserts them DIRECTLY into the main
``tracks`` collection as ``base_collected`` — no Spotify API required.

This is the key difference from the v3 Discogs/Last.fm workers which
insert into ``track_candidates`` and require candidate_match_worker
(Spotify search) to validate them. The Deezer worker is Spotify-independent,
providing continuity when Spotify is rate-limited or unavailable.

Deduplication:
  1. ISRC match → track already exists (Spotify or Deezer), bump appearance_score.
  2. Fingerprint match (title + artist_name hash) → same, bump score.
  3. Neither → new insert with spotify_id = "deezer:{deezer_track_id}".

When artist_graph_worker or candidate_match_worker later discovers the same
track via Spotify, the ISRC dedup finds the existing document and backfills
the real spotify_id via ``$set`` (bypassing ``$setOnInsert``).

Queue: ``deezer_seed_queue``
  Each item = one Deezer artist to crawl (top tracks + albums).
  Bootstrap: genre list → genre artists → populate queue.

Scale: ~55 genres × 25 artists × 50+ tracks ≈ 65K+ tracks per cycle.
Re-seeds automatically when all artists are done.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.core.config import Settings
from app.db.collections import DEEZER_SEED_QUEUE_COL, TRACKS_COL
from app.models.track import TrackDocument, TrackStatus, ArtistRef, AlbumRef
from app.services.deezer import DeezerClient
from app.utils.rate_limiter import RateLimiter
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID, LOCK_TIMEOUT_SECONDS
from datetime import timedelta

logger = structlog.get_logger(__name__)

# CIS/Central Asia/MENA artist search queries to explicitly seed the Deezer BFS.
# The global genre seed covers ~1400 mostly Western artists; these queries
# inject CIS artists at bootstrap so BFS reaches the region within the first cycle.
_CIS_SEED_QUERIES: List[str] = [
    # Russian (Latin + Cyrillic)
    "русский рэп", "русская поп", "русский рок", "шансон", "русский хип-хоп",
    "russian rap", "russian pop", "russian rock",
    # Ukrainian
    "українська поп", "ukrainian pop",
    # Kazakh
    "казахская музыка", "kazakh music", "kazakh pop",
    # Uzbek
    "o'zbek pop", "uzbek music",
    # Azerbaijani / Armenian / Georgian
    "azerbaijani music", "armenian pop", "georgian music",
    # Central Asia broad
    "central asian music",
]


def _deezer_fingerprint(title: str, artist_name: str, duration_s: int) -> str:
    """
    Fingerprint for Deezer tracks (uses artist NAME, not Spotify ID).

    Duration bucketed to ±2 s to absorb minor differences.
    """
    import re
    text = re.sub(
        r"\s*[\(\[](remaster(ed)?|live|radio edit|explicit|clean|version|edit|mix|remix).*?[\)\]]",
        "",
        title,
        flags=re.IGNORECASE,
    )
    norm_title = text.lower().strip()
    norm_artist = artist_name.lower().strip()
    bucket = round((duration_s * 1000) / 2000) * 2000
    raw = f"{norm_title}|{norm_artist}|{bucket}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class DeezerDirectWorker(BaseWorker):
    """
    Discovers tracks from Deezer and inserts them directly into the pipeline.

    Queue: ``deezer_seed_queue`` (one document per Deezer artist)
    Output: ``tracks`` (status=base_collected)
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._deezer: Optional[DeezerClient] = None

    async def on_startup(self) -> None:
        self._deezer = DeezerClient(self.settings)
        await self._bootstrap_queue()
        logger.info("deezer_direct_worker_started")

    async def on_shutdown(self) -> None:
        if self._deezer:
            await self._deezer.aclose()

    # ── Seeding ───────────────────────────────────────────────────────────────

    async def _bootstrap_queue(self) -> None:
        """
        Seed ``deezer_seed_queue`` on first run (idempotent via $setOnInsert).

        Strategy:
        1. Fetch global chart → insert top 100 chart tracks directly.
        2. Fetch all Deezer genres → for each genre, fetch top artists.
        3. Insert one queue item per artist (processed=False).

        On subsequent runs (all processed), resets items to processed=False
        so the crawl repeats with fresh data.
        """
        col = self.db[DEEZER_SEED_QUEUE_COL]

        unprocessed = await col.count_documents({"processed": False})
        if unprocessed > 0:
            logger.info("deezer_queue_has_pending", count=unprocessed)
            return

        total = await col.count_documents({})
        if total > 0:
            # All done — reset for next cycle
            await col.update_many({}, {"$set": {"processed": False, "locked_at": None}})
            reset_count = await col.count_documents({"processed": False})
            logger.info("deezer_queue_reset_for_next_cycle", count=reset_count)
            return

        # First run: insert global chart directly, then seed artist queue
        await self._insert_chart_tracks()
        await self._seed_from_genres()
        await self._seed_cis_artists()

    async def _insert_chart_tracks(self) -> None:
        """Insert global Deezer chart directly (no queue item needed)."""
        assert self._deezer is not None
        tracks = await self._deezer.get_chart_tracks(limit=100)
        if not tracks:
            return
        inserted = 0
        for t in tracks:
            if await self._upsert_deezer_track(t, album_data=t.get("album")):
                inserted += 1
        logger.info("deezer_chart_inserted", inserted=inserted, total=len(tracks))
        await self.increment_stat("deezer_chart_tracks_inserted", inserted)

    async def _seed_cis_artists(self) -> None:
        """
        Search Deezer for CIS/Central Asia artists and add them directly to the queue.

        The global genre seed populates ~1400 mostly Western artists; this step
        explicitly injects CIS artists at bootstrap so the BFS graph reaches the
        region within the first processing cycle instead of depth 3–4.
        """
        assert self._deezer is not None
        col = self.db[DEEZER_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        ops: List[Any] = []

        for query in _CIS_SEED_QUERIES:
            artists = await self._deezer.search_artists(query, limit=25)
            for artist in artists:
                artist_id = artist.get("id")
                if not artist_id:
                    continue
                ops.append(UpdateOne(
                    {"artist_id": artist_id},
                    {"$setOnInsert": {
                        "artist_id": artist_id,
                        "artist_name": artist.get("name", ""),
                        "genre_id": None,
                        "genre_name": f"cis_seed:{query[:30]}",
                        "processed": False,
                        "locked_at": None,
                        "locked_by": None,
                        "created_at": now,
                        "updated_at": now,
                    }},
                    upsert=True,
                ))

        if not ops:
            return

        try:
            result = await col.bulk_write(ops, ordered=False)
            logger.info(
                "deezer_cis_artists_seeded",
                seeded=result.upserted_count,
                total_candidates=len(ops),
            )
        except BulkWriteError as bwe:
            logger.info(
                "deezer_cis_artists_seeded",
                seeded=bwe.details.get("nUpserted", 0),
            )

    async def _seed_from_genres(self) -> None:
        """Fetch genre list, collect all genre artists, bulk-insert into queue."""
        assert self._deezer is not None
        genres = await self._deezer.get_genres()
        if not genres:
            logger.warning("deezer_no_genres_returned")
            return

        col = self.db[DEEZER_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        ops: List[Any] = []

        for genre in genres:
            genre_id = genre.get("id")
            genre_name = genre.get("name", "")
            if not genre_id or genre_id == 0:  # 0 = "All" pseudo-genre
                continue

            artists = await self._deezer.get_genre_artists(genre_id)
            for artist in artists:
                artist_id = artist.get("id")
                if not artist_id:
                    continue
                ops.append(UpdateOne(
                    {"artist_id": artist_id},
                    {"$setOnInsert": {
                        "artist_id": artist_id,
                        "artist_name": artist.get("name", ""),
                        "genre_id": genre_id,
                        "genre_name": genre_name,
                        "processed": False,
                        "locked_at": None,
                        "locked_by": None,
                        "created_at": now,
                        "updated_at": now,
                    }},
                    upsert=True,
                ))

        if not ops:
            logger.warning("deezer_no_artists_seeded")
            return

        try:
            result = await col.bulk_write(ops, ordered=False)
            logger.info(
                "deezer_queue_bootstrapped",
                seeded=result.upserted_count,
                total_candidates=len(ops),
            )
        except BulkWriteError as bwe:
            logger.info(
                "deezer_queue_bootstrapped",
                seeded=bwe.details.get("nUpserted", 0),
            )

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        col = self.db[DEEZER_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        batch = []

        for _ in range(self.settings.deezer_batch_size):
            doc = await col.find_one_and_update(
                {
                    "processed": False,
                    "$or": [
                        {"locked_at": None},
                        {"locked_at": {"$lt": cutoff}},
                    ],
                },
                {"$set": {
                    "locked_at": now,
                    "locked_by": WORKER_INSTANCE_ID,
                    "updated_at": now,
                }},
                return_document=True,
            )
            if doc is None:
                break
            batch.append(doc)

        return batch

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            await self._process_artist(item)

    async def _process_artist(self, item: Dict[str, Any]) -> None:
        assert self._deezer is not None
        col = self.db[DEEZER_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        artist_id: int = item["artist_id"]
        artist_name: str = item.get("artist_name", "")

        try:
            inserted_total = 0

            # 1. Top tracks (fast: 1 API call)
            top_tracks = await self._deezer.get_artist_top_tracks(
                artist_id, limit=self.settings.deezer_top_tracks_limit
            )
            for t in top_tracks:
                if await self._upsert_deezer_track(t, album_data=t.get("album")):
                    inserted_total += 1

            # 2. Albums (optional: controlled by deezer_crawl_albums setting)
            if self.settings.deezer_crawl_albums:
                albums_inserted = await self._crawl_artist_albums(artist_id)
                inserted_total += albums_inserted

            # BFS: enqueue related artists so the crawl expands beyond
            # the initial genre seed (~1400 artists → potentially millions).
            related_enqueued = await self._enqueue_related_artists(artist_id)

            await col.update_one(
                {"artist_id": artist_id},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

            await self.increment_stat("deezer_artists_processed", 1)
            await self.increment_stat("deezer_tracks_inserted", inserted_total)
            await self.increment_stat("total_discovered", inserted_total)
            await self.increment_stat("total_base_collected", inserted_total)

            logger.info(
                "deezer_artist_done",
                artist_id=artist_id,
                artist_name=artist_name,
                tracks_inserted=inserted_total,
                related_enqueued=related_enqueued,
            )

        except Exception as exc:
            logger.error(
                "deezer_artist_failed",
                artist_id=artist_id,
                artist_name=artist_name,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"artist_id": artist_id},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

    async def _crawl_artist_albums(self, artist_id: int) -> int:
        """Fetch all artist albums and insert their tracks. Returns inserted count."""
        assert self._deezer is not None
        inserted = 0
        index = 0
        limit = 25
        max_albums = self.settings.deezer_max_albums_per_artist

        while True:
            page = await self._deezer.get_artist_albums(artist_id, limit=limit, index=index)
            albums = page.get("data", [])
            if not albums:
                break

            for album in albums:
                album_id = album.get("id")
                if not album_id:
                    continue
                tracks = await self._deezer.get_album_tracks(album_id)
                for t in tracks:
                    if await self._upsert_deezer_track(t, album_data=album):
                        inserted += 1

            total = page.get("total", 0)
            index += limit
            if index >= total or index >= max_albums * limit:
                break

        return inserted

    # ── BFS expansion ────────────────────────────────────────────────────────

    async def _enqueue_related_artists(self, artist_id: int) -> int:
        """
        Fetch Deezer related artists and add new ones to ``deezer_seed_queue``.

        Uses ``$setOnInsert`` so already-queued artists are not re-inserted.
        This is the BFS expansion step: the initial ~1400 genre-seed artists
        each contribute ~20 related artists, growing the crawl to 20k+.

        Unlike Spotify's ``/related-artists`` (deprecated Nov 2024), Deezer
        still supports this endpoint.
        """
        assert self._deezer is not None
        related = await self._deezer.get_artist_related(artist_id)
        if not related:
            return 0

        col = self.db[DEEZER_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        enqueued = 0

        for rel in related:
            rel_id = rel.get("id")
            if not rel_id:
                continue
            try:
                result = await col.update_one(
                    {"artist_id": rel_id},
                    {"$setOnInsert": {
                        "artist_id": rel_id,
                        "artist_name": rel.get("name", ""),
                        "genre_id": None,
                        "genre_name": "",
                        "processed": False,
                        "locked_at": None,
                        "locked_by": None,
                        "created_at": now,
                        "updated_at": now,
                    }},
                    upsert=True,
                )
                if result.upserted_id is not None:
                    enqueued += 1
            except Exception:
                pass

        return enqueued

    # ── Track upsert ──────────────────────────────────────────────────────────

    async def _upsert_deezer_track(
        self,
        raw: Dict[str, Any],
        album_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Upsert a Deezer track into the main tracks collection.

        Deduplication:
          - ISRC (sparse unique index) → preferred
          - title+artist_name fingerprint (sparse unique index) → fallback

        ``spotify_id`` is set to ``"deezer:{track_id}"`` as a placeholder.
        When artist_graph_worker or candidate_match_worker finds the real
        Spotify ID for this track, it backfills ``spotify_id`` via ``$set``
        (see the backfill logic in those workers).

        Returns True if a new document was inserted.
        """
        track_id = raw.get("id")
        title = (raw.get("title") or raw.get("title_short") or "").strip()
        if not track_id or not title:
            return False

        artist_raw = raw.get("artist") or {}
        artist_name = (artist_raw.get("name") or "").strip()
        if not artist_name:
            return False

        duration_s: int = raw.get("duration") or 0
        duration_ms: int = duration_s * 1000
        isrc: Optional[str] = raw.get("isrc") or None
        explicit: bool = raw.get("explicit_lyrics") or False

        # Use Deezer artist ID as placeholder spotify_id for ArtistRef
        deezer_artist_id = artist_raw.get("id", 0)
        artist_ref = ArtistRef(
            spotify_id=f"deezer:{deezer_artist_id}",
            name=artist_name,
        )

        # Album
        album_ref: Optional[AlbumRef] = None
        if album_data and album_data.get("id"):
            album_ref = AlbumRef(
                spotify_id=f"deezer_album:{album_data['id']}",
                name=album_data.get("title") or album_data.get("name") or "",
                release_date=album_data.get("release_date"),
                images=[{"url": album_data["cover"]}] if album_data.get("cover") else [],
            )

        # Fingerprint (title + artist_name based — no Spotify ID available)
        fp = _deezer_fingerprint(title, artist_name, duration_s)
        placeholder_spotify_id = f"deezer:{track_id}"

        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        insert_doc: Dict[str, Any] = {
            "spotify_id": placeholder_spotify_id,
            "isrc": isrc,
            "fingerprint": fp,
            "name": title,
            "artists": [{"spotify_id": artist_ref.spotify_id, "name": artist_ref.name}],
            "album": album_ref.model_dump() if album_ref else None,
            "popularity": raw.get("rank", 0) // 10000,  # Deezer rank → approximate popularity
            "duration_ms": duration_ms,
            "explicit": explicit,
            "markets_count": 0,
            "markets": [],
            "status": TrackStatus.BASE_COLLECTED.value,
            # NOTE: appearance_score intentionally omitted here — $inc handles
            # both insert (0+1=1) and update (+1). MongoDB disallows the same
            # path in both $setOnInsert and $inc simultaneously.
            "version_album_ids": [],
            "youtube_searched": False,
            "musicbrainz_enriched": False,
            "language_detected": False,
            "transliteration_done": False,
            "mb_priority": 0,
            "quality_score": 0.0,
            "regional_score": 0.0,
            "artist_followers": 0,
            "retry_count": 0,
            "error_log": [],
            "locked_at": None,
            "locked_by": None,
        }

        # Prefer ISRC for dedup; fall back to fingerprint
        if isrc:
            filter_q: Dict[str, Any] = {"isrc": isrc}
        else:
            filter_q = {"fingerprint": fp}

        update_ops: Dict[str, Any] = {
            "$setOnInsert": {**insert_doc, "created_at": now},
            "$inc": {"appearance_score": 1},
            "$set": {"updated_at": now},
        }

        try:
            result = await col.update_one(filter_q, update_ops, upsert=True)
            new_doc = result.upserted_id is not None

            # Backfill spotify_id if existing doc has a deezer: or ext: placeholder
            # and we're inserting from an authoritative Spotify source.
            # (This branch is for Deezer — we never backfill here, only Spotify
            # workers do. But we do ensure the placeholder is set on insert.)

            return new_doc

        except Exception as exc:
            err_str = str(exc).lower()
            if "duplicate key" in err_str:
                # Concurrent insert race — ignore
                return False
            logger.error(
                "deezer_track_upsert_error",
                track_id=track_id,
                title=title,
                error=str(exc),
            )
            return False
