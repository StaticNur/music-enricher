"""
Audio Features Worker.

Polls ``tracks`` for documents with status=base_collected, fetches
Spotify audio features in batches of up to 100, and updates each
track's ``audio_features`` subdocument.

If the audio-features endpoint returns 403 (deprecated/restricted API tier),
the worker gracefully skips the enrichment and still advances the status
so the pipeline does not stall.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import Settings
from app.db.collections import TRACKS_COL
from app.models.track import TrackStatus, AudioFeatures
from app.services.spotify import SpotifyClient
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)

# Maximum IDs per Spotify audio-features batch request
AUDIO_FEATURES_BATCH_SIZE = 100


class AudioFeaturesWorker(BaseWorker):
    """
    Enriches tracks with Spotify audio analysis features.

    State transitions:
        tracks: base_collected → audio_features_added
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._spotify: Optional[SpotifyClient] = None
        self._api_available: bool = True  # Set False after first 403

    async def on_startup(self) -> None:
        self._spotify = SpotifyClient(self.settings)
        logger.info("audio_features_worker_started")

    async def on_shutdown(self) -> None:
        if self._spotify:
            await self._spotify.aclose()

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Claim up to ``batch_size`` tracks with status=base_collected.

        Batch size capped at AUDIO_FEATURES_BATCH_SIZE (100) to align
        with the Spotify batch endpoint limit.
        """
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        effective_batch = min(self.settings.batch_size, AUDIO_FEATURES_BATCH_SIZE)
        batch = []

        # Use find() + bulk update for efficiency (claim many at once)
        cursor = col.find(
            {
                "$or": [
                    {"status": TrackStatus.BASE_COLLECTED.value},
                    self.stale_lock_query(),
                ]
            }
        ).limit(effective_batch)

        async for doc in cursor:
            batch.append(doc)

        if not batch:
            return []

        ids = [doc["spotify_id"] for doc in batch]
        await col.update_many(
            {
                "spotify_id": {"$in": ids},
                "$or": [
                    {"status": TrackStatus.BASE_COLLECTED.value},
                    self.stale_lock_query(),
                ],
            },
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
        """
        Fetch audio features for the whole batch in one API call,
        then update each track document individually.
        """
        assert self._spotify is not None
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        spotify_ids = [doc["spotify_id"] for doc in batch]

        # Fetch audio features (handles 403 gracefully)
        features_list: List[Optional[Dict]] = []
        if self._api_available:
            features_list = await self._spotify.get_audio_features(spotify_ids)
            if all(f is None for f in features_list) and features_list:
                # All None — likely 403/deprecated
                logger.warning("audio_features_api_unavailable")
                self._api_available = False
        else:
            features_list = [None] * len(batch)

        # Build id → features mapping
        features_map: Dict[str, Optional[Dict]] = {}
        for sid, feat in zip(spotify_ids, features_list):
            features_map[sid] = feat

        # Update each track
        for doc in batch:
            sid = doc["spotify_id"]
            raw_feat = features_map.get(sid)

            audio_features: Optional[AudioFeatures] = None
            if raw_feat:
                try:
                    audio_features = AudioFeatures(
                        danceability=raw_feat.get("danceability"),
                        energy=raw_feat.get("energy"),
                        key=raw_feat.get("key"),
                        loudness=raw_feat.get("loudness"),
                        mode=raw_feat.get("mode"),
                        speechiness=raw_feat.get("speechiness"),
                        acousticness=raw_feat.get("acousticness"),
                        instrumentalness=raw_feat.get("instrumentalness"),
                        liveness=raw_feat.get("liveness"),
                        valence=raw_feat.get("valence"),
                        tempo=raw_feat.get("tempo"),
                        time_signature=raw_feat.get("time_signature"),
                        duration_ms=raw_feat.get("duration_ms"),
                    )
                except Exception as exc:
                    logger.warning(
                        "audio_features_parse_error",
                        spotify_id=sid,
                        error=str(exc),
                    )

            update_doc: Dict[str, Any] = {
                "status": TrackStatus.AUDIO_FEATURES_ADDED.value,
                "updated_at": now,
                "locked_at": None,
                "locked_by": None,
            }
            if audio_features:
                update_doc["audio_features"] = audio_features.model_dump()

            try:
                await col.update_one(
                    {"spotify_id": sid},
                    {"$set": update_doc},
                )
            except Exception as exc:
                logger.error(
                    "audio_features_update_failed",
                    spotify_id=sid,
                    error=str(exc),
                )
                # Revert to base_collected so it can be retried
                retry = doc.get("retry_count", 0) + 1
                new_status = (
                    TrackStatus.FAILED.value
                    if retry >= self.settings.worker_retry_limit
                    else TrackStatus.BASE_COLLECTED.value
                )
                await col.update_one(
                    {"spotify_id": sid},
                    {
                        "$set": {
                            "status": new_status,
                            "retry_count": retry,
                            "locked_at": None,
                            "locked_by": None,
                        },
                        "$push": {"error_log": str(exc)[:200]},
                    },
                )
                continue

        features_found = sum(1 for f in features_list if f is not None)
        logger.info(
            "audio_features_batch_done",
            total=len(batch),
            features_found=features_found,
        )
        await self.increment_stat("audio_features_added", features_found)
        await self.increment_stat(
            "audio_features_unavailable", len(batch) - features_found
        )
