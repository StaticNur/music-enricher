"""
Yandex Music Discovery Worker (v7).

CIS-focused direct discovery — the best source for Russian, Kazakh,
Belarusian, Uzbek, Ukrainian and other post-Soviet music.

Phase 1 (bootstrap):
    Fetches top chart tracks for each CIS country (ru, kz, by, uz, am, az,
    ge, ua, md) → inserts directly into tracks as base_collected.
    Artists from chart tracks are queued for full discography crawl.

Phase 2 (continuous):
    Processes yandex_seed_queue: fetches artist's full discography
    (paginated), inserts new tracks, enqueues similar artists (BFS).

Deduplication: fingerprint(title, artist_name, duration_ms bucket).
No ISRC available from Yandex Music — fingerprint-only dedup.

Placeholder spotify_id: "yandex:{track_id}".
When artist_graph_worker / candidate_match_worker finds the same track
via Spotify, it backfills the real spotify_id.

Requirements:
    YANDEX_MUSIC_TOKEN — OAuth token (free Yandex account, see docs).
    Worker is idle (no-op) if token is not set.

Queue  : yandex_seed_queue
Output : tracks (status=base_collected)
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.core.config import Settings
from app.db.collections import YANDEX_SEED_QUEUE_COL, TRACKS_COL
from app.models.track import TrackStatus
from app.services.yandex_music import YandexMusicClient, CIS_CHART_COUNTRIES
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID, LOCK_TIMEOUT_SECONDS

logger = structlog.get_logger(__name__)

# How many tracks to fetch per artist iteration (pagination page size).
_PAGE_SIZE = 50
# Max pages per artist to prevent runaway on artists with huge catalogs.
_MAX_PAGES = 20
# Batch size: artists claimed per polling iteration.
_ARTIST_BATCH_SIZE = 5


def _yandex_fingerprint(title: str, artist_name: str, duration_ms: Optional[int]) -> str:
    """
    Fingerprint for Yandex Music tracks (artist name-based, no Spotify ID).
    Duration bucketed to ±1s to absorb minor rip differences.
    """
    norm_title = re.sub(
        r"\s*[\(\[](remaster(ed)?|live|radio edit|explicit|clean|version|edit|mix|remix).*?[\)\]]",
        "", title, flags=re.IGNORECASE,
    ).lower().strip()
    norm_artist = artist_name.lower().strip()
    bucket = round((duration_ms or 0) / 2000) * 2000
    return hashlib.sha256(f"{norm_title}|{norm_artist}|{bucket}".encode()).hexdigest()


class YandexWorker(BaseWorker):
    """
    Discovers tracks from Yandex Music and inserts them directly into the pipeline.

    Queue: yandex_seed_queue (one document per Yandex artist ID)
    Output: tracks (status=base_collected)
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._yandex: Optional[YandexMusicClient] = None

    async def on_startup(self) -> None:
        self._yandex = YandexMusicClient(self.settings)
        if not self.settings.yandex_music_token:
            logger.warning(
                "yandex_music_token_missing",
                message=(
                    "YANDEX_MUSIC_TOKEN is not set — worker will be idle. "
                    "Get a free token from a Yandex account to enable CIS discovery."
                ),
            )
            return
        await self._bootstrap()
        logger.info("yandex_worker_started")

    async def on_shutdown(self) -> None:
        if self._yandex:
            await self._yandex.aclose()

    # ── Bootstrap ─────────────────────────────────────────────────────────────

    async def _bootstrap(self) -> None:
        """
        Seed yandex_seed_queue from CIS charts.

        - If unprocessed items exist: nothing to do.
        - If all processed: reset for next cycle.
        - If empty: fetch charts for all CIS countries → insert tracks
          directly + populate artist queue from chart artists.
        """
        col = self.db[YANDEX_SEED_QUEUE_COL]

        unprocessed = await col.count_documents({"processed": False})
        if unprocessed > 0:
            logger.info("yandex_queue_has_pending", count=unprocessed)
            return

        total = await col.count_documents({})
        if total > 0:
            await col.update_many({}, {"$set": {"processed": False, "locked_at": None}})
            reset_count = await col.count_documents({"processed": False})
            logger.info("yandex_queue_reset_for_next_cycle", count=reset_count)
            return

        # First run
        await self._seed_from_charts()

    async def _seed_from_charts(self) -> None:
        """
        Fetch CIS country charts → insert tracks + enqueue artists.

        Charts give us the "hits" immediately while the artist queue
        builds up BFS coverage of the long tail.
        """
        assert self._yandex is not None
        countries = self.settings.yandex_music_chart_countries_list or CIS_CHART_COUNTRIES

        inserted_total = 0
        artist_seen: Dict[int, str] = {}  # artist_id → name

        for country in countries:
            if self._yandex.is_banned:
                logger.warning("yandex_chart_seed_aborted", reason="client banned mid-bootstrap")
                break
            tracks = await self._yandex.get_chart(country)
            logger.info("yandex_chart_fetched", country=country, tracks=len(tracks))

            for t in tracks:
                if await self._upsert_track(t):
                    inserted_total += 1
                for a in (t.get("artists") or []):
                    if a.get("id") and a["id"] not in artist_seen:
                        artist_seen[a["id"]] = a.get("name", "")

        logger.info(
            "yandex_charts_seeded",
            tracks_inserted=inserted_total,
            artists_queued=len(artist_seen),
            countries=len(countries),
        )
        await self.increment_stat("yandex_chart_tracks_inserted", inserted_total)

        # Enqueue chart artists for full discography crawl
        if artist_seen:
            enqueued = await self._enqueue_artists(
                [{"id": aid, "name": name} for aid, name in artist_seen.items()]
            )
            logger.info("yandex_artists_enqueued_from_charts", count=enqueued)

    # ── Queue management ──────────────────────────────────────────────────────

    async def _enqueue_artists(self, artists: List[Dict[str, Any]]) -> int:
        """Bulk-upsert artists into yandex_seed_queue. Returns newly inserted count."""
        col = self.db[YANDEX_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        ops = []
        for a in artists:
            artist_id = a.get("id")
            if not artist_id:
                continue
            ops.append(UpdateOne(
                {"artist_id": artist_id},
                {"$setOnInsert": {
                    "artist_id": artist_id,
                    "artist_name": a.get("name", ""),
                    "processed": False,
                    "locked_at": None,
                    "locked_by": None,
                    "created_at": now,
                    "updated_at": now,
                }},
                upsert=True,
            ))
        if not ops:
            return 0
        try:
            result = await col.bulk_write(ops, ordered=False)
            return result.upserted_count
        except BulkWriteError as bwe:
            return bwe.details.get("nUpserted", 0)

    async def claim_batch(self) -> List[Dict[str, Any]]:
        if not self.settings.yandex_music_token:
            return []
        if self._yandex and self._yandex.is_banned:
            logger.warning(
                "yandex_worker_halted",
                reason="client is banned or rate-limited — no further requests will be sent",
            )
            return []

        col = self.db[YANDEX_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        batch = []

        for _ in range(_ARTIST_BATCH_SIZE):
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
        assert self._yandex is not None
        col = self.db[YANDEX_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        artist_id: int = item["artist_id"]
        artist_name: str = item.get("artist_name", "")

        try:
            inserted_total = 0

            # Paginate through full discography
            for page in range(_MAX_PAGES):
                if self._yandex.is_banned:
                    logger.warning("yandex_artist_aborted_banned", artist_id=artist_id)
                    return
                tracks = await self._yandex.get_artist_tracks(
                    artist_id, page=page, page_size=_PAGE_SIZE
                )
                if not tracks:
                    break
                for t in tracks:
                    if await self._upsert_track(t):
                        inserted_total += 1
                if len(tracks) < _PAGE_SIZE:
                    break

            # BFS: enqueue similar artists
            if self._yandex.is_banned:
                return
            info = await self._yandex.get_artist_brief_info(artist_id)
            enqueued = 0
            if info and info.get("similar"):
                enqueued = await self._enqueue_artists(info["similar"])

            await col.update_one(
                {"artist_id": artist_id},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

            await self.increment_stat("yandex_artists_processed", 1)
            await self.increment_stat("yandex_tracks_inserted", inserted_total)
            await self.increment_stat("total_discovered", inserted_total)
            await self.increment_stat("total_base_collected", inserted_total)

            logger.info(
                "yandex_artist_done",
                artist_id=artist_id,
                artist_name=artist_name,
                tracks_inserted=inserted_total,
                similar_enqueued=enqueued,
            )

        except Exception as exc:
            logger.error(
                "yandex_artist_failed",
                artist_id=artist_id,
                artist_name=artist_name,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"artist_id": artist_id},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

    # ── Track upsert ──────────────────────────────────────────────────────────

    async def _upsert_track(self, t: Dict[str, Any]) -> bool:
        """
        Upsert a Yandex Music track into the main tracks collection.

        Uses fingerprint deduplication (no ISRC available from Yandex Music).
        Returns True if a new document was inserted.
        """
        track_id = t.get("id")
        title = (t.get("title") or "").strip()
        if not track_id or not title:
            return False

        artists_raw = t.get("artists") or []
        if not artists_raw:
            return False
        artist_name = (artists_raw[0].get("name") or "").strip()
        if not artist_name:
            return False

        duration_ms: Optional[int] = t.get("duration_ms")

        fp = _yandex_fingerprint(title, artist_name, duration_ms)
        placeholder_spotify_id = f"yandex:{track_id}"

        album_data = t.get("album") or {}
        album_dict: Optional[Dict[str, Any]] = None
        if album_data.get("id"):
            cover_uri = album_data.get("cover_uri") or ""
            cover_url = (
                f"https://{cover_uri.replace('%%', '200x200')}" if cover_uri else ""
            )
            album_dict = {
                "spotify_id": f"yandex_album:{album_data['id']}",
                "name": album_data.get("title") or "",
                "release_date": str(album_data["year"]) if album_data.get("year") else None,
                "images": [{"url": cover_url}] if cover_url else [],
            }

        artists_list = [
            {"spotify_id": f"yandex_artist:{a['id']}", "name": a["name"]}
            for a in artists_raw
            if a.get("id") and a.get("name")
        ]

        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        insert_doc: Dict[str, Any] = {
            "spotify_id": placeholder_spotify_id,
            "isrc": None,
            "fingerprint": fp,
            "name": title,
            "artists": artists_list,
            "album": album_dict,
            "popularity": 0,
            "duration_ms": duration_ms or 0,
            "explicit": False,
            "markets_count": 0,
            "markets": [],
            "status": TrackStatus.BASE_COLLECTED.value,
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

        try:
            result = await col.update_one(
                {"fingerprint": fp},
                {
                    "$setOnInsert": {**insert_doc, "created_at": now},
                    "$inc": {"appearance_score": 1},
                    "$set": {"updated_at": now},
                },
                upsert=True,
            )
            return result.upserted_id is not None

        except Exception as exc:
            if "duplicate key" in str(exc).lower():
                return False
            logger.error(
                "yandex_upsert_error",
                track_id=track_id,
                title=title,
                error=str(exc),
            )
            return False
