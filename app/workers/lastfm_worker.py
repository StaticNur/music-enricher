"""
Last.fm Discovery Worker.

Self-seeding: on first run it populates ``lastfm_seed_queue`` with one item
per (tag, method) pair. Subsequent runs process those items, fetching top
tracks page by page and inserting new candidates into ``track_candidates``.

Deduplication within ``track_candidates`` is enforced by a unique index on
``candidate_fingerprint`` — duplicate key errors are silently ignored.
Candidates are later matched to Spotify tracks by ``candidate_match_worker``.

State machine for queue items:
    pending → processing → (page += 1) → pending  (next iteration)
                                        → done     (exhausted or max_pages)
                                        → failed   (too many errors)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.core.config import Settings
from app.db.collections import LASTFM_SEED_QUEUE_COL, TRACK_CANDIDATES_COL
from app.models.candidate import (
    CandidateDocument,
    CandidateSource,
    LastFmSeedItem,
    QueueStatus,
)
from app.services.lastfm import LastFmClient
from app.utils.deduplication import compute_candidate_fingerprint
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)

# ── Seed data ─────────────────────────────────────────────────────────────────

LASTFM_TAGS: List[str] = [
    "pop", "hip-hop", "rap", "rock", "indie", "electronic",
    "metal", "jazz", "classical", "latin", "k-pop", "j-pop",
    "arabic", "afrobeats", "reggaeton", "bollywood", "soundtrack",
    "lofi", "ambient", "house", "techno",
    # Extended coverage
    "alternative", "country", "folk", "r&b", "soul", "blues",
    "punk", "dance", "edm", "trap", "drill", "grime",
    "reggae", "dancehall", "afropop", "bossanova", "samba",
    "gospel", "world music", "new age", "chillout",
    "singer-songwriter", "indie pop", "art rock", "progressive rock",
    "heavy metal", "death metal", "black metal", "thrash metal",
    "drum and bass", "dubstep", "trance", "deep house", "techno",
    "psychedelic", "shoegaze", "post-rock", "math rock",
    "neo-soul", "trap music", "phonk", "synthwave", "vaporwave",
]

# Seed items: (tag, method). Empty tag means the global chart.
_SEED_ITEMS: List[Dict[str, Any]] = [
    {"tag": "", "method": "chart.getTopTracks"},
]
for _tag in LASTFM_TAGS:
    _SEED_ITEMS.append({"tag": _tag, "method": "tag.getTopTracks"})


class LastFmWorker(BaseWorker):
    """
    Discovers tracks from Last.fm top-track charts and genre tags.

    Self-seeds ``lastfm_seed_queue`` on first run, then processes items
    with per-page pagination state persisted in MongoDB for crash safety.
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._lastfm: Optional[LastFmClient] = None

    async def on_startup(self) -> None:
        if not self.settings.lastfm_api_key:
            logger.warning(
                "lastfm_api_key_missing",
                message="LASTFM_API_KEY not set — worker will have no results",
            )
        self._lastfm = LastFmClient(self.settings)
        await self._bootstrap_queue()
        logger.info("lastfm_worker_started")

    async def on_shutdown(self) -> None:
        if self._lastfm:
            await self._lastfm.aclose()

    async def _bootstrap_queue(self) -> None:
        """Populate lastfm_seed_queue if empty (idempotent via $setOnInsert)."""
        col = self.db[LASTFM_SEED_QUEUE_COL]
        existing = await col.count_documents({})
        if existing > 0:
            logger.info("lastfm_queue_already_seeded", count=existing)
            return

        now = datetime.now(timezone.utc)
        ops = []
        for item in _SEED_ITEMS:
            seed = LastFmSeedItem(
                tag=item["tag"],
                method=item["method"],
                max_pages=self.settings.lastfm_max_pages,
            )
            doc = seed.to_mongo()
            doc["created_at"] = now
            doc["updated_at"] = now
            ops.append(UpdateOne(
                {"tag": item["tag"], "method": item["method"]},
                {"$setOnInsert": doc},
                upsert=True,
            ))

        if ops:
            result = await col.bulk_write(ops, ordered=False)
            logger.info("lastfm_queue_bootstrapped", inserted=result.upserted_count)

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """Claim one seed queue item at a time (pagination handled internally)."""
        col = self.db[LASTFM_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        doc = await col.find_one_and_update(
            {
                "$or": [
                    {"status": QueueStatus.PENDING.value},
                    self.stale_lock_query(),
                ]
            },
            {
                "$set": {
                    "status": QueueStatus.PROCESSING.value,
                    "locked_at": now,
                    "locked_by": WORKER_INSTANCE_ID,
                    "updated_at": now,
                }
            },
            return_document=True,
        )
        return [doc] if doc else []

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            await self._process_item(item)

    async def _process_item(self, item: Dict[str, Any]) -> None:
        tag: str = item["tag"]
        method: str = item["method"]
        page: int = item.get("page", 1)
        max_pages: int = item.get("max_pages", self.settings.lastfm_max_pages)
        col = self.db[LASTFM_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)

        try:
            assert self._lastfm is not None

            # Fetch one page from Last.fm
            if method == "chart.getTopTracks":
                raw_tracks = await self._lastfm.get_chart_top_tracks(page=page)
            else:
                raw_tracks = await self._lastfm.get_tag_top_tracks(tag=tag, page=page)

            if not raw_tracks:
                # Empty result — tag exhausted
                await col.update_one(
                    {"tag": tag, "method": method},
                    {"$set": {
                        "status": QueueStatus.DONE.value,
                        "locked_at": None,
                        "updated_at": now,
                    }},
                )
                logger.info("lastfm_tag_exhausted", tag=tag, method=method, final_page=page)
                return

            # Insert candidates
            inserted, dupes = await self._insert_candidates(raw_tracks, tag)

            next_page = page + 1
            if next_page > max_pages:
                new_status = QueueStatus.DONE.value
                logger.info(
                    "lastfm_tag_done",
                    tag=tag, method=method, final_page=page,
                    candidates_inserted=inserted,
                )
            else:
                new_status = QueueStatus.PENDING.value

            await col.update_one(
                {"tag": tag, "method": method},
                {"$set": {
                    "status": new_status,
                    "page": next_page,
                    "locked_at": None,
                    "updated_at": now,
                }},
            )

            await self.increment_stat("lastfm_candidates_inserted", inserted)
            logger.debug(
                "lastfm_page_processed",
                tag=tag, page=page, inserted=inserted, duplicates=dupes,
            )

        except Exception as exc:
            logger.error(
                "lastfm_item_failed",
                tag=tag, method=method, page=page,
                error=str(exc), exc_info=True,
            )
            retry_count = item.get("retry_count", 0) + 1
            new_status = (
                QueueStatus.FAILED.value
                if retry_count >= self.settings.worker_retry_limit
                else QueueStatus.PENDING.value
            )
            await col.update_one(
                {"tag": tag, "method": method},
                {"$set": {
                    "status": new_status,
                    "retry_count": retry_count,
                    "locked_at": None,
                    "updated_at": now,
                }},
            )

    async def _insert_candidates(
        self, raw_tracks: List[Dict[str, Any]], tag: str
    ) -> tuple[int, int]:
        """
        Parse Last.fm tracks, deduplicate, and bulk-upsert into track_candidates.

        Returns ``(inserted_count, duplicate_count)``.
        """
        assert self._lastfm is not None
        col = self.db[TRACK_CANDIDATES_COL]
        now = datetime.now(timezone.utc)
        ops = []

        for raw in raw_tracks:
            parsed = LastFmClient.parse_track(raw)
            if parsed is None:
                continue

            fp = compute_candidate_fingerprint(
                parsed["title"], parsed["artist"], parsed.get("duration_ms")
            )
            candidate = CandidateDocument(
                source=CandidateSource.LASTFM,
                title=parsed["title"],
                artist=parsed["artist"],
                duration_ms=parsed.get("duration_ms"),
                candidate_fingerprint=fp,
            )
            doc = candidate.to_mongo()
            doc["created_at"] = now
            doc["updated_at"] = now

            ops.append(UpdateOne(
                {"candidate_fingerprint": fp},
                {"$setOnInsert": doc},
                upsert=True,
            ))

        if not ops:
            return 0, 0

        try:
            result = await col.bulk_write(ops, ordered=False)
            inserted = result.upserted_count
            dupes = len(ops) - inserted
            return inserted, dupes
        except BulkWriteError as bwe:
            # Count real inserts vs duplicate-key skips
            inserted = bwe.details.get("nUpserted", 0)
            dupes = len(ops) - inserted
            return inserted, dupes
