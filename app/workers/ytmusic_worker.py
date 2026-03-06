"""
YouTube Music Discovery Worker.

Self-seeding: on first run populates ``ytmusic_seed_queue`` with search
queries and country chart entries. Each queue item is processed exactly once
(no pagination — YTMusic returns a fixed result set per query).

The primary value of this worker is that it records ``youtube_video_id``
alongside each candidate. When ``candidate_match_worker`` later matches the
candidate to a Spotify track, the video ID is stored on the track document,
enabling future download via yt-dlp.

State machine for queue items:
    pending → processing → done | failed
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.core.config import Settings
from app.db.collections import YTMUSIC_SEED_QUEUE_COL, TRACK_CANDIDATES_COL
from app.models.candidate import (
    CandidateDocument,
    CandidateSource,
    YtMusicSeedItem,
    QueueStatus,
)
from app.services.ytmusic import YtMusicClient
from app.utils.deduplication import compute_candidate_fingerprint
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)

# ── Seed data ─────────────────────────────────────────────────────────────────

# Country codes for chart discovery
YTMUSIC_CHART_COUNTRIES: List[str] = [
    "US", "GB", "AU", "CA", "NZ",          # English-speaking
    "ZA", "NG", "GH", "KE",                # Africa
    "IN", "PK", "BD",                       # South Asia
    "KR", "JP", "TW", "HK",                # East Asia
    "BR", "MX", "AR", "CO", "CL", "PE",    # Latin America
    "FR", "DE", "ES", "IT", "NL", "SE",    # Europe
    "TR", "SA", "AE", "EG", "MA",          # MENA
    "ID", "MY", "TH", "PH", "VN", "SG",   # Southeast Asia
    "RU", "UA", "PL", "CZ",               # Eastern Europe
]

# Search queries for additional discovery (broad + genre-specific)
YTMUSIC_SEARCH_QUERIES: List[str] = [
    # Broad popularity
    "top songs 2024", "best songs 2023", "popular songs 2022",
    "viral hits 2024", "trending music 2024", "new music 2024",
    "top 100 songs", "most streamed songs", "global hits 2024",
    "summer hits 2024", "party hits 2024", "road trip songs",
    "workout music", "chill playlist", "study music playlist",
    # Genre-specific
    "best pop songs 2024", "top hip hop 2024", "best rnb songs",
    "top electronic music", "best indie songs", "top rock songs",
    "best latin songs", "top k-pop songs", "best country songs",
    "top dance songs", "best jazz songs", "top classical music",
    "best reggaeton", "top afrobeats", "bollywood hits 2024",
    "best metal songs", "top punk songs", "best folk music",
    "top soul songs", "best funk music", "reggae hits",
    "trap music", "lofi hip hop", "chillhop beats",
    "best alternative songs", "top indie pop", "synthwave playlist",
    "phonk music", "sad songs", "love songs playlist",
    "motivational songs", "acoustic songs", "best covers 2024",
    # Regional
    "arabic pop songs", "russian pop music", "korean rnb",
    "japanese city pop", "nigerian afrobeats", "mexican corridos",
    "brazilian funk", "indian pop songs", "turkish pop music",
]


class YtMusicWorker(BaseWorker):
    """
    Discovers tracks from YouTube Music search results and country charts.

    Self-seeds ``ytmusic_seed_queue`` on first run. Each item is processed
    once (no pagination). Records ``youtube_video_id`` per candidate.
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._ytmusic: Optional[YtMusicClient] = None

    async def on_startup(self) -> None:
        self._ytmusic = YtMusicClient(self.settings)
        await self._bootstrap_queue()
        logger.info("ytmusic_worker_started")

    async def on_shutdown(self) -> None:
        pass  # YtMusicClient has no async resources to close

    async def _bootstrap_queue(self) -> None:
        """Populate ytmusic_seed_queue if empty (idempotent via $setOnInsert)."""
        col = self.db[YTMUSIC_SEED_QUEUE_COL]
        existing = await col.count_documents({})
        if existing > 0:
            logger.info("ytmusic_queue_already_seeded", count=existing)
            return

        now = datetime.now(timezone.utc)
        ops = []

        for country in YTMUSIC_CHART_COUNTRIES:
            seed = YtMusicSeedItem(query_type="chart", query=country)
            doc = seed.to_mongo()
            doc["created_at"] = now
            doc["updated_at"] = now
            ops.append(UpdateOne(
                {"query_type": "chart", "query": country},
                {"$setOnInsert": doc},
                upsert=True,
            ))

        for query in YTMUSIC_SEARCH_QUERIES:
            seed = YtMusicSeedItem(query_type="search", query=query)
            doc = seed.to_mongo()
            doc["created_at"] = now
            doc["updated_at"] = now
            ops.append(UpdateOne(
                {"query_type": "search", "query": query},
                {"$setOnInsert": doc},
                upsert=True,
            ))

        if ops:
            result = await col.bulk_write(ops, ordered=False)
            logger.info("ytmusic_queue_bootstrapped", inserted=result.upserted_count)

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """Claim up to batch_size seed queue items."""
        col = self.db[YTMUSIC_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        batch = []

        for _ in range(self.settings.batch_size):
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
            if doc is None:
                break
            batch.append(doc)

        return batch

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            await self._process_item(item)

    async def _process_item(self, item: Dict[str, Any]) -> None:
        query_type: str = item["query_type"]
        query: str = item["query"]
        col = self.db[YTMUSIC_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)

        try:
            assert self._ytmusic is not None

            if query_type == "chart":
                raw_songs = await self._fetch_chart(query)
            else:
                raw_songs = await self._ytmusic.search_songs(query, limit=40)

            inserted, dupes = await self._insert_candidates(raw_songs)

            await col.update_one(
                {"query_type": query_type, "query": query},
                {"$set": {
                    "status": QueueStatus.DONE.value,
                    "locked_at": None,
                    "updated_at": now,
                }},
            )

            await self.increment_stat("ytmusic_candidates_inserted", inserted)
            logger.debug(
                "ytmusic_item_processed",
                query_type=query_type, query=query,
                inserted=inserted, duplicates=dupes,
            )

        except Exception as exc:
            logger.error(
                "ytmusic_item_failed",
                query_type=query_type, query=query,
                error=str(exc), exc_info=True,
            )
            retry_count = item.get("retry_count", 0) + 1
            new_status = (
                QueueStatus.FAILED.value
                if retry_count >= self.settings.worker_retry_limit
                else QueueStatus.PENDING.value
            )
            await col.update_one(
                {"query_type": query_type, "query": query},
                {"$set": {
                    "status": new_status,
                    "retry_count": retry_count,
                    "locked_at": None,
                    "updated_at": now,
                }},
            )

    async def _fetch_chart(self, country: str) -> List[Dict[str, Any]]:
        """Fetch chart songs for a country and return raw song dicts."""
        assert self._ytmusic is not None
        charts = await self._ytmusic.get_charts(country=country)
        return YtMusicClient.extract_chart_songs(charts)

    async def _insert_candidates(
        self, raw_songs: List[Dict[str, Any]]
    ) -> tuple[int, int]:
        """
        Parse YTMusic results and bulk-upsert into track_candidates.

        Returns ``(inserted_count, duplicate_count)``.
        """
        assert self._ytmusic is not None
        col = self.db[TRACK_CANDIDATES_COL]
        now = datetime.now(timezone.utc)
        ops = []

        for raw in raw_songs:
            parsed = YtMusicClient.parse_song(raw)
            if parsed is None:
                continue

            fp = compute_candidate_fingerprint(
                parsed["title"], parsed["artist"], parsed.get("duration_ms")
            )
            candidate = CandidateDocument(
                source=CandidateSource.YTMUSIC,
                title=parsed["title"],
                artist=parsed["artist"],
                duration_ms=parsed.get("duration_ms"),
                youtube_video_id=parsed.get("youtube_video_id"),
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
