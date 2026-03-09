"""
iTunes / Apple Music Discovery Worker (v6).

Two-phase discovery strategy, both fully Spotify-independent:

Phase 1 — Bootstrap (runs once per cycle):
    Fetches Apple Music RSS chart feeds for 40+ countries (most-played +
    hot-tracks), inserts ~5K–15K unique chart tracks directly into the
    pipeline as base_collected.  Near-instant, no queue needed.

Phase 2 — Continuous artist search:
    Reads unique artist names aggregated from the tracks collection and
    searches iTunes for each one (up to 200 songs per artist).  Results
    are bulk-upserted into tracks.

    From 50K+ unique artists already in DB:
        50K artists × 200 results × ~30% new ≈ 3M potential new tracks
        At 8 rps / batch_size=10: completes in ~1.5 hours per cycle.

    When the queue is exhausted it automatically re-aggregates artist
    names (including any added by other workers since the last cycle)
    and restarts.

No authentication required.  Horizontally scalable.

Queue  : itunes_seed_queue  (one doc per artist name)
Output : tracks (status=base_collected, spotify_id="itunes:{trackId}")
"""
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.core.config import Settings
from app.db.collections import ITUNES_SEED_QUEUE_COL, TRACKS_COL
from app.models.track import AlbumRef, ArtistRef, TrackStatus
from app.services.apple_music import AppleMusicClient, CHART_COUNTRIES, CHART_TYPES
from app.utils.deduplication import compute_candidate_fingerprint, normalize_text
from app.workers.base import BaseWorker, LOCK_TIMEOUT_SECONDS, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)


class ItunesWorker(BaseWorker):
    """
    Discovers tracks via iTunes Search API and Apple Music RSS charts.

    Queue : itunes_seed_queue (artist names from existing tracks)
    Output: tracks (status=base_collected)
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._apple: Optional[AppleMusicClient] = None

    async def on_startup(self) -> None:
        self._apple = AppleMusicClient(self.settings)
        await self._bootstrap()
        logger.info("itunes_worker_started")

    async def on_shutdown(self) -> None:
        if self._apple:
            await self._apple.aclose()

    # ── Bootstrap ─────────────────────────────────────────────────────────────

    async def _bootstrap(self) -> None:
        """
        Seed the iTunes queue and ingest RSS charts.

        Called on every startup:
        - If unprocessed items exist → nothing to do, return.
        - Otherwise (first run OR queue exhausted) →
            1. Ingest latest RSS charts directly into tracks.
            2. Aggregate artist names from tracks → insert into queue.
            3. Reset any already-processed items to pending (catches new
               artists added by other workers since the last cycle).
        """
        col = self.db[ITUNES_SEED_QUEUE_COL]

        unprocessed = await col.count_documents({"processed": False})
        if unprocessed > 0:
            logger.info("itunes_queue_has_pending", count=unprocessed)
            return

        logger.info("itunes_queue_empty_refreshing")

        # 1. Fresh chart data (fast — RSS, no rate limit)
        await self._ingest_rss_charts()

        # 2. Add any artist names not yet in the queue
        await self._seed_from_db_artists()

        # 3. Reset previously-processed items so the cycle restarts
        result = await col.update_many(
            {"processed": True},
            {"$set": {"processed": False, "locked_at": None}},
        )
        total = await col.count_documents({"processed": False})
        logger.info("itunes_queue_ready", total=total, reset=result.modified_count)

    async def _ingest_rss_charts(self) -> None:
        """Pull Apple Music RSS charts and bulk-upsert into tracks."""
        assert self._apple is not None
        ops: List[UpdateOne] = []

        for country in CHART_COUNTRIES:
            for chart_type in CHART_TYPES[:2]:  # most-played + hot-tracks
                results = await self._apple.get_rss_chart(
                    country, chart_type, limit=100
                )
                for r in results:
                    op = self._rss_to_upsert_op(r)
                    if op is not None:
                        ops.append(op)

        if not ops:
            return

        inserted = await self._bulk_upsert(ops)
        if inserted:
            await self.increment_stat("itunes_rss_inserted", inserted)
            await self.increment_stat("total_discovered", inserted)
            await self.increment_stat("total_base_collected", inserted)
        logger.info("itunes_rss_done", ops=len(ops), new=inserted)

    async def _seed_from_db_artists(self) -> None:
        """
        Aggregate unique artist names from ``tracks`` and insert into queue.

        Uses ``$setOnInsert`` so existing queue items are not touched.
        """
        col = self.db[ITUNES_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        ops: List[UpdateOne] = []

        pipeline = [
            {"$unwind": "$artists"},
            {"$group": {"_id": "$artists.name"}},
            {"$match": {"_id": {"$ne": None}, "_id": {"$not": {"$regex": "^$"}}}},
            {"$limit": self.settings.itunes_seed_artist_limit},
        ]
        async for doc in self.db[TRACKS_COL].aggregate(pipeline):
            name: str = doc["_id"] or ""
            if len(name) < 2:
                continue
            # Normalize name as the queue key so that "The Weeknd" / "WEEKND" / "Weeknd"
            # all collapse to one queue item. iTunes Search API is case-insensitive,
            # so querying with the lowercased name returns the same results.
            # Existing queue items keyed on the raw name are still processed normally
            # (claim_batch queries on processed=False, not on this field).
            norm_key = normalize_text(name)
            if not norm_key:
                continue
            ops.append(UpdateOne(
                {"artist_name": norm_key},
                {"$setOnInsert": {
                    "artist_name": norm_key,
                    "processed": False,
                    "locked_at": None,
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
                "itunes_artists_seeded",
                new=result.upserted_count,
                total=len(ops),
            )
        except BulkWriteError as bwe:
            logger.info(
                "itunes_artists_seeded",
                new=bwe.details.get("nUpserted", 0),
            )

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        col = self.db[ITUNES_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        batch = []

        for _ in range(self.settings.itunes_batch_size):
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
        assert self._apple is not None
        col = self.db[ITUNES_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        threshold = self.settings.itunes_skip_if_tracks_gte

        all_ops: List[UpdateOne] = []
        skipped = 0
        skipped_ids: set = set()

        for item in batch:
            artist_name: str = item.get("artist_name", "")

            # ── Pre-check: skip iTunes API call if artist already well-covered ──
            # Case-insensitive match so "WEEKND" and "Weeknd" both count against
            # the same artist's track total. The artist_name_idx supports regex scans.
            existing = await self.db[TRACKS_COL].count_documents(
                {"artists.name": {"$regex": f"^{re.escape(artist_name)}$", "$options": "i"}},
                limit=threshold,  # stop counting at threshold — fast
            )
            if existing >= threshold:
                skipped += 1
                skipped_ids.add(item["_id"])
                logger.debug(
                    "itunes_artist_skipped",
                    artist=artist_name,
                    existing_tracks=existing,
                )
                await col.update_one(
                    {"_id": item["_id"]},
                    {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
                )
                continue

            # ── Artist not well-covered → search iTunes ────────────────────────
            results = await self._apple.search_artist_tracks(artist_name)
            for raw in results:
                op = self._itunes_to_upsert_op(raw)
                if op is not None:
                    all_ops.append(op)

        # Single bulk write for all tracks from the non-skipped artists
        inserted = await self._bulk_upsert(all_ops)

        # Mark remaining (non-skipped) items processed
        processed_ids = [item["_id"] for item in batch if item["_id"] not in skipped_ids]
        if processed_ids:
            await col.update_many(
                {"_id": {"$in": processed_ids}},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

        if inserted:
            await self.increment_stat("itunes_tracks_inserted", inserted)
            await self.increment_stat("total_discovered", inserted)
            await self.increment_stat("total_base_collected", inserted)

        logger.info(
            "itunes_batch_done",
            artists=len(batch),
            skipped=skipped,
            candidates=len(all_ops),
            new=inserted,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _bulk_upsert(self, ops: List[UpdateOne]) -> int:
        """Execute a bulk write and return the count of newly inserted docs."""
        if not ops:
            return 0
        try:
            result = await self.db[TRACKS_COL].bulk_write(ops, ordered=False)
            return result.upserted_count
        except BulkWriteError as bwe:
            # ordered=False: partial success is normal (duplicate key on
            # fingerprint or spotify_id when two workers process overlapping
            # iTunes results simultaneously).
            details = bwe.details
            n_inserted = details.get("nUpserted", 0)
            n_errors = len(details.get("writeErrors", []))
            if n_errors:
                # Log a sample error so we can diagnose unexpected failures
                sample = details.get("writeErrors", [{}])[0]
                logger.warning(
                    "itunes_bulk_write_partial_errors",
                    n_inserted=n_inserted,
                    n_errors=n_errors,
                    sample_code=sample.get("code"),
                    sample_msg=str(sample.get("errmsg", ""))[:120],
                )
            return n_inserted
        except Exception as exc:
            logger.error("itunes_bulk_write_error", error=str(exc))
            return 0

    def _itunes_to_upsert_op(self, raw: Dict[str, Any]) -> Optional[UpdateOne]:
        """Convert an iTunes search result dict to a MongoDB upsert operation."""
        track_id = raw.get("trackId")
        title = (raw.get("trackName") or "").strip()
        artist_name = (raw.get("artistName") or "").strip()
        if not track_id or not title or not artist_name:
            return None

        duration_ms: int = raw.get("trackTimeMillis") or 0
        itunes_artist_id = raw.get("artistId") or 0
        fp = compute_candidate_fingerprint(title, artist_name, duration_ms or None)

        album_ref: Optional[AlbumRef] = None
        collection_id = raw.get("collectionId")
        if collection_id:
            album_ref = AlbumRef(
                spotify_id=f"itunes_album:{collection_id}",
                name=raw.get("collectionName") or "",
                release_date=(raw.get("releaseDate") or "")[:10] or None,
                images=(
                    [{"url": raw["artworkUrl100"]}]
                    if raw.get("artworkUrl100") else []
                ),
            )

        now = datetime.now(timezone.utc)
        insert_doc = _base_track_doc(
            spotify_id=f"itunes:{track_id}",
            fingerprint=fp,
            name=title,
            artist_spotify_id=f"itunes:{itunes_artist_id}",
            artist_name=artist_name,
            album=album_ref,
            duration_ms=duration_ms,
            explicit=raw.get("trackExplicitness") == "explicit",
        )

        return UpdateOne(
            {"fingerprint": fp},
            {
                "$setOnInsert": {**insert_doc, "created_at": now},
                "$inc": {"appearance_score": 1},
                "$set": {"updated_at": now},
            },
            upsert=True,
        )

    def _rss_to_upsert_op(self, raw: Dict[str, Any]) -> Optional[UpdateOne]:
        """Convert an Apple RSS chart entry to a MongoDB upsert operation."""
        itunes_id = raw.get("id")
        title = (raw.get("name") or "").strip()
        artist_name = (raw.get("artistName") or "").strip()
        if not itunes_id or not title or not artist_name:
            return None

        # RSS has no duration — fingerprint without duration bucket
        fp = compute_candidate_fingerprint(title, artist_name, None)
        now = datetime.now(timezone.utc)
        insert_doc = _base_track_doc(
            spotify_id=f"itunes:{itunes_id}",
            fingerprint=fp,
            name=title,
            artist_spotify_id=f"itunes:rss:{artist_name}",
            artist_name=artist_name,
            album=None,
            duration_ms=0,
            explicit=False,
        )

        return UpdateOne(
            {"fingerprint": fp},
            {
                "$setOnInsert": {**insert_doc, "created_at": now},
                "$inc": {"appearance_score": 1},
                "$set": {"updated_at": now},
            },
            upsert=True,
        )


# ── Module-level helper (avoids repeating the full doc dict twice) ────────────

def _base_track_doc(
    *,
    spotify_id: str,
    fingerprint: str,
    name: str,
    artist_spotify_id: str,
    artist_name: str,
    album: Optional[AlbumRef],
    duration_ms: int,
    explicit: bool,
) -> Dict[str, Any]:
    return {
        "spotify_id": spotify_id,
        "fingerprint": fingerprint,
        "name": name,
        "artists": [{"spotify_id": artist_spotify_id, "name": artist_name}],
        "album": album.model_dump() if album else None,
        "popularity": 0,
        "duration_ms": duration_ms,
        "explicit": explicit,
        "markets_count": 0,
        "markets": [],
        "status": TrackStatus.BASE_COLLECTED.value,
        "version_album_ids": [],
        "appearance_score": 1,
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
