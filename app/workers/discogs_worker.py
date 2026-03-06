"""
Discogs Discovery Worker.

Self-seeding: on first run populates ``discogs_seed_queue`` with one item per
genre style. Each item tracks the current pagination page so the worker can
resume after a crash.

Per page, the worker:
1. Calls ``database/search?type=release&style=STYLE&page=PAGE`` (1 API call).
2. For each release result, calls ``releases/{id}`` to get the tracklist.
3. Inserts individual tracks as candidates.

Strict 1 req/s rate limit is enforced via the token-bucket limiter in
``DiscogsClient``. **This worker must run as a single replica.**

State machine for queue items:
    pending → processing → (page += 1) → pending   (next page)
                                        → done      (exhausted or max_pages)
                                        → failed    (too many errors)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.core.config import Settings
from app.db.collections import DISCOGS_SEED_QUEUE_COL, TRACK_CANDIDATES_COL
from app.models.candidate import (
    CandidateDocument,
    CandidateSource,
    DiscogsSeedItem,
    QueueStatus,
)
from app.services.discogs import DiscogsClient
from app.utils.deduplication import compute_candidate_fingerprint
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)

# ── Seed data ─────────────────────────────────────────────────────────────────

DISCOGS_STYLES: List[str] = [
    "hip hop", "rap", "trap", "lo-fi",
    "rock", "indie rock", "alternative rock", "punk", "hard rock",
    "pop", "synth-pop", "dream pop", "electropop",
    "electronic", "house", "techno", "trance", "drum and bass",
    "ambient", "idm", "experimental", "industrial",
    "metal", "heavy metal", "death metal", "black metal", "thrash",
    "jazz", "soul", "funk", "blues", "r&b", "neo soul",
    "classical", "contemporary classical", "orchestral",
    "country", "folk", "bluegrass", "americana",
    "reggae", "dancehall", "dub", "ska",
    "latin", "cumbia", "salsa", "bachata", "bossa nova",
    "afrobeat", "highlife", "afropop",
    "k-pop", "j-pop", "city pop",
    "gospel", "spiritual",
    "new age", "meditation",
    "grunge", "post-rock", "shoegaze", "math rock",
    "prog rock", "prog metal", "doom metal",
    "swing", "bebop", "smooth jazz", "free jazz",
    "disco", "funk soul", "motown",
    "singer-songwriter", "acoustic",
    # CIS / Eastern European
    "russian pop", "russian rock", "chanson", "bard",
    "soviet pop", "osteuropaeische musik",
    "ukrainian folk", "georgian folk", "balkan",
    # Central Asia
    "uzbek pop", "kazakh pop", "central asian folk",
    # MENA
    "arabic pop", "khaleeji", "raï", "turkish pop", "anatolian rock",
    "shaabi", "mahraganat",
    # Extended world
    "bollywood", "filmi", "bhangra",
    "fado", "flamenco", "tango",
]


class DiscogsWorker(BaseWorker):
    """
    Discovers tracks from Discogs releases by genre style.

    Self-seeds ``discogs_seed_queue`` on first run.
    Fetches release search results and release tracklists page by page.
    Inserts track candidates into ``track_candidates``.

    Must run as a single replica due to the strict 1 req/s Discogs rate limit.
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._discogs: Optional[DiscogsClient] = None

    async def on_startup(self) -> None:
        if not self.settings.discogs_token:
            logger.warning(
                "discogs_token_missing",
                message="DISCOGS_TOKEN not set — rate limit will be 25 req/min",
            )
        self._discogs = DiscogsClient(self.settings)
        await self._bootstrap_queue()
        logger.info("discogs_worker_started")

    async def on_shutdown(self) -> None:
        if self._discogs:
            await self._discogs.aclose()

    async def _bootstrap_queue(self) -> None:
        """Populate discogs_seed_queue if no pending items remain.

        On first run: inserts all genre style items.
        On subsequent runs (all items done): resets them to pending page 1
        so all styles are re-crawled in the next cycle.
        """
        col = self.db[DISCOGS_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)

        pending = await col.count_documents({"status": QueueStatus.PENDING.value})
        if pending > 0:
            logger.info("discogs_queue_has_pending", count=pending)
            return

        # Reset exhausted styles back to page 1 for another cycle
        reset = await col.update_many(
            {"status": QueueStatus.DONE.value},
            {"$set": {"status": QueueStatus.PENDING.value, "page": 1, "updated_at": now}},
        )
        if reset.modified_count > 0:
            logger.info("discogs_queue_reset_for_next_cycle", count=reset.modified_count)
            return

        # Collection is empty — seed from scratch
        ops = []
        for style in DISCOGS_STYLES:
            seed = DiscogsSeedItem(
                style=style,
                max_pages=self.settings.discogs_max_pages,
            )
            doc = seed.to_mongo()
            doc["created_at"] = now
            doc["updated_at"] = now
            ops.append(UpdateOne(
                {"style": style},
                {"$setOnInsert": doc},
                upsert=True,
            ))

        if ops:
            result = await col.bulk_write(ops, ordered=False)
            logger.info("discogs_queue_bootstrapped", inserted=result.upserted_count)

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """Claim one seed queue item at a time (pagination handled internally)."""
        col = self.db[DISCOGS_SEED_QUEUE_COL]
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
        style: str = item["style"]
        page: int = item.get("page", 1)
        per_page: int = item.get("per_page", 50)
        max_pages: int = item.get("max_pages", self.settings.discogs_max_pages)
        col = self.db[DISCOGS_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)

        try:
            assert self._discogs is not None

            # 1. Search releases for this style + page
            search_resp = await self._discogs.search_releases(
                style=style, page=page, per_page=per_page
            )

            if not search_resp:
                await self._mark_done(col, style, now)
                return

            results = search_resp.get("results") or []
            pagination = search_resp.get("pagination") or {}
            total_pages = pagination.get("pages", 0)

            # 2. For each release result, fetch tracklist and create candidates
            total_inserted = 0
            for result in results:
                parsed = DiscogsClient.parse_search_result(result)
                if parsed is None:
                    continue
                artist_fallback, release_title, release_id = parsed

                # Fetch full release to get the tracklist
                release_doc = await self._discogs.get_release(release_id)
                if not release_doc:
                    continue

                tracks = DiscogsClient.parse_tracklist(release_doc, artist_fallback)
                if not tracks:
                    # Fallback: create one candidate from the release itself
                    tracks = [{
                        "title": release_title,
                        "artist": artist_fallback,
                        "duration_ms": None,
                        "discogs_release_id": release_id,
                    }]

                inserted, _ = await self._insert_candidates(tracks)
                total_inserted += inserted

            # 3. Advance page or mark done
            next_page = page + 1
            exhausted = next_page > max_pages or next_page > total_pages > 0 or not results

            if exhausted:
                await self._mark_done(col, style, now)
                logger.info(
                    "discogs_style_done",
                    style=style, final_page=page, candidates_inserted=total_inserted,
                )
            else:
                await col.update_one(
                    {"style": style},
                    {"$set": {
                        "status": QueueStatus.PENDING.value,
                        "page": next_page,
                        "locked_at": None,
                        "locked_by": None,
                        "updated_at": now,
                    }},
                )

            await self.increment_stat("discogs_candidates_inserted", total_inserted)
            logger.debug(
                "discogs_page_processed",
                style=style, page=page, inserted=total_inserted,
            )

        except Exception as exc:
            logger.error(
                "discogs_item_failed",
                style=style, page=page,
                error=str(exc), exc_info=True,
            )
            retry_count = item.get("retry_count", 0) + 1
            new_status = (
                QueueStatus.FAILED.value
                if retry_count >= self.settings.worker_retry_limit
                else QueueStatus.PENDING.value
            )
            await col.update_one(
                {"style": style},
                {"$set": {
                    "status": new_status,
                    "retry_count": retry_count,
                    "locked_at": None,
                    "locked_by": None,
                    "updated_at": now,
                }},
            )

    async def _mark_done(self, col: Any, style: str, now: datetime) -> None:
        await col.update_one(
            {"style": style},
            {"$set": {
                "status": QueueStatus.DONE.value,
                "locked_at": None,
                "locked_by": None,
                "updated_at": now,
            }},
        )

    async def _insert_candidates(
        self, tracks: List[Dict[str, Any]]
    ) -> tuple[int, int]:
        """
        Bulk-upsert track candidates into ``track_candidates``.

        Returns ``(inserted_count, duplicate_count)``.
        """
        col = self.db[TRACK_CANDIDATES_COL]
        now = datetime.now(timezone.utc)
        ops = []

        for track in tracks:
            title = (track.get("title") or "").strip()
            artist = (track.get("artist") or "").strip()
            if not title or not artist:
                continue

            fp = compute_candidate_fingerprint(
                title, artist, track.get("duration_ms")
            )
            candidate = CandidateDocument(
                source=CandidateSource.DISCOGS,
                title=title,
                artist=artist,
                duration_ms=track.get("duration_ms"),
                discogs_release_id=track.get("discogs_release_id"),
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
            return inserted, len(ops) - inserted
        except BulkWriteError as bwe:
            inserted = bwe.details.get("nUpserted", 0)
            return inserted, len(ops) - inserted
