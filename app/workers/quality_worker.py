"""
Quality Scoring Worker.

Polls ``tracks`` for documents with status=lyrics_added, computes
a composite quality score, and marks the track as either:
- ``enriched``     — score ≥ quality_threshold
- ``filtered_out`` — score < quality_threshold

Quality formula:
    0.40 * normalized_popularity
    0.30 * normalized_artist_followers (looked up from ``artists`` collection)
    0.20 * normalized_appearance_score
    0.10 * normalized_markets_count
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import Settings
from app.db.collections import TRACKS_COL, ARTISTS_COL
from app.models.track import TrackStatus
from app.utils.scoring import compute_quality_score, compute_quality_score_with_regional
from app.utils.regional import compute_regional_score
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)


class QualityWorker(BaseWorker):
    """
    Calculates quality scores and makes the final pass/fail decision.

    State transitions:
        tracks: lyrics_added → enriched | filtered_out
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        # LRU-style in-memory artist follower cache (artist_id → followers)
        self._artist_cache: Dict[str, int] = {}
        self._cache_max = 10_000

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        batch = []

        cursor = col.find(
            {
                "$or": [
                    # Accept both lyrics_added (normal flow) and audio_features_added
                    # (direct path when lyrics_worker hasn't reached the track yet).
                    # At 10M+ scale Genius enrichment takes weeks — quality scoring
                    # must not be gated on it.
                    {"status": {"$in": [
                        TrackStatus.LYRICS_ADDED.value,
                        TrackStatus.AUDIO_FEATURES_ADDED.value,
                    ]}},
                    self.stale_lock_query(),
                ]
            }
        ).limit(self.settings.batch_size)

        async for doc in cursor:
            batch.append(doc)

        if not batch:
            return []

        ids = [doc["spotify_id"] for doc in batch]
        await col.update_many(
            {"spotify_id": {"$in": ids}},
            {
                "$set": {
                    "status": "processing",
                    "locked_at": now,
                    "locked_by": WORKER_INSTANCE_ID,
                    "updated_at": now,
                }
            },
        )
        return batch

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        # Pre-fetch all needed artist followers in bulk
        artist_ids = list(
            {
                a["spotify_id"]
                for doc in batch
                for a in doc.get("artists", [])[:1]  # only primary artist
                if "spotify_id" in a
                and a["spotify_id"] not in self._artist_cache
            }
        )
        if artist_ids:
            await self._prefetch_artist_followers(artist_ids)

        tasks = [self._score_track(doc) for doc in batch]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _prefetch_artist_followers(self, artist_ids: List[str]) -> None:
        """Batch load artist followers from the ``artists`` collection."""
        col = self.db[ARTISTS_COL]
        cursor = col.find(
            {"spotify_id": {"$in": artist_ids}},
            {"spotify_id": 1, "followers": 1, "_id": 0},
        )
        async for doc in cursor:
            self._artist_cache[doc["spotify_id"]] = doc.get("followers", 0)

        # Manage cache size — simple eviction of oldest half when full
        if len(self._artist_cache) > self._cache_max:
            keys = list(self._artist_cache.keys())
            for k in keys[: self._cache_max // 2]:
                del self._artist_cache[k]

    async def _score_track(self, doc: Dict[str, Any]) -> None:
        sid: str = doc["spotify_id"]
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        try:
            # Look up primary artist followers
            artists: List[dict] = doc.get("artists", [])
            primary_artist_id = artists[0].get("spotify_id", "") if artists else ""
            artist_followers = self._artist_cache.get(primary_artist_id, 0)

            # v2: regional scoring
            regional_score_val = 0.0
            if self.settings.regional_boost_enabled:
                artist_country: Optional[str] = (
                    (doc.get("regions") or {}).get("artist_country")
                    if doc.get("regions") else None
                )
                regional_score_val = compute_regional_score(
                    markets=doc.get("markets", []),
                    language=doc.get("language"),
                    artist_country=artist_country,
                    target_regions=self.settings.target_regions_list,
                )
                score = compute_quality_score_with_regional(
                    popularity=doc.get("popularity", 0),
                    artist_followers=artist_followers,
                    appearance_score=doc.get("appearance_score", 0),
                    markets_count=doc.get("markets_count", 0),
                    regional_score=regional_score_val,
                    regional_boost_weight=self.settings.regional_boost_weight,
                )
            else:
                score = compute_quality_score(
                    popularity=doc.get("popularity", 0),
                    artist_followers=artist_followers,
                    appearance_score=doc.get("appearance_score", 0),
                    markets_count=doc.get("markets_count", 0),
                )

            if score >= self.settings.quality_threshold:
                new_status = TrackStatus.ENRICHED.value
                stat_field = "total_enriched"
            else:
                new_status = TrackStatus.FILTERED_OUT.value
                stat_field = "total_filtered_out"

            await col.update_one(
                {"spotify_id": sid},
                {
                    "$set": {
                        "status": new_status,
                        "quality_score": score,
                        "regional_score": regional_score_val,
                        "artist_followers": artist_followers,
                        "updated_at": now,
                        "locked_at": None,
                        "locked_by": None,
                    }
                },
            )

            logger.debug(
                "track_scored",
                spotify_id=sid,
                score=score,
                status=new_status,
            )
            await self.increment_stat(stat_field)

        except Exception as exc:
            logger.error(
                "quality_scoring_failed",
                spotify_id=sid,
                error=str(exc),
                exc_info=True,
            )
            retry = doc.get("retry_count", 0) + 1
            new_status = (
                TrackStatus.FAILED.value
                if retry >= self.settings.worker_retry_limit
                else TrackStatus.AUDIO_FEATURES_ADDED.value  # safe fallback for both input statuses
            )
            await col.update_one(
                {"spotify_id": sid},
                {
                    "$set": {
                        "status": new_status.value
                        if hasattr(new_status, "value")
                        else new_status,
                        "retry_count": retry,
                        "locked_at": None,
                        "locked_by": None,
                        "updated_at": now,
                    },
                    "$push": {"error_log": str(exc)[:200]},
                },
            )
