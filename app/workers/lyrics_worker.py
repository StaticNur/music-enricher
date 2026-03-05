"""
Lyrics Worker.

Polls ``tracks`` for documents with status=audio_features_added,
searches Genius for matching lyrics, and stores the result in the
``lyrics`` subdocument.

If no confident match is found, ``lyrics`` remains null but status
still advances to ``lyrics_added`` so the pipeline does not stall.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import Settings
from app.db.collections import TRACKS_COL
from app.models.track import TrackStatus, LyricsData
from app.services.genius import GeniusClient, LyricsResult
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)

# Max concurrent Genius requests per batch
LYRICS_CONCURRENCY = 5


class LyricsWorker(BaseWorker):
    """
    Enriches tracks with lyrics from Genius.

    State transitions:
        tracks: audio_features_added → lyrics_added
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._genius: Optional[GeniusClient] = None
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(LYRICS_CONCURRENCY)

    async def on_startup(self) -> None:
        self._genius = GeniusClient(self.settings)
        logger.info("lyrics_worker_started")

    async def on_shutdown(self) -> None:
        if self._genius:
            await self._genius.aclose()

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        batch = []

        cursor = col.find(
            {
                "$or": [
                    {"status": TrackStatus.AUDIO_FEATURES_ADDED.value},
                    self.stale_lock_query(),
                ]
            }
        ).limit(self.settings.batch_size)

        async for doc in cursor:
            batch.append(doc)

        if not batch:
            return []

        ids = [doc["spotify_id"] for doc in batch]
        await self.db[TRACKS_COL].update_many(
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
        tasks = [self._process_track(doc) for doc in batch]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_track(self, doc: Dict[str, Any]) -> None:
        """Fetch lyrics for one track with concurrency control."""
        async with self._semaphore:
            await self._fetch_and_store_lyrics(doc)

    async def _fetch_and_store_lyrics(self, doc: Dict[str, Any]) -> None:
        sid: str = doc["spotify_id"]
        track_name: str = doc.get("name", "")
        artists: List[dict] = doc.get("artists", [])
        artist_name: str = artists[0].get("name", "") if artists else ""
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        try:
            assert self._genius is not None
            result: Optional[LyricsResult] = await self._genius.get_lyrics(
                track_name, artist_name
            )

            lyrics_doc: Optional[Dict] = None
            if result is not None:
                lyrics_data = LyricsData(
                    text=result.text,
                    language=result.language,
                    genius_url=result.genius_url,
                    genius_song_id=result.genius_song_id,
                    confidence_score=result.confidence_score,
                    fetched_at=result.fetched_at,
                )
                lyrics_doc = lyrics_data.model_dump()

            await col.update_one(
                {"spotify_id": sid},
                {
                    "$set": {
                        "status": TrackStatus.LYRICS_ADDED.value,
                        "lyrics": lyrics_doc,
                        "updated_at": now,
                        "locked_at": None,
                    }
                },
            )

            found = result is not None
            logger.debug(
                "lyrics_processed",
                track=track_name,
                artist=artist_name,
                found=found,
                confidence=result.confidence_score if result else 0.0,
            )

            if found:
                await self.increment_stat("lyrics_added")
            else:
                await self.increment_stat("lyrics_not_found")

        except Exception as exc:
            logger.error(
                "lyrics_fetch_failed",
                spotify_id=sid,
                track=track_name,
                error=str(exc),
                exc_info=True,
            )
            retry = doc.get("retry_count", 0) + 1
            new_status = (
                TrackStatus.FAILED.value
                if retry >= self.settings.worker_retry_limit
                else TrackStatus.AUDIO_FEATURES_ADDED.value
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
                        "updated_at": now,
                    },
                    "$push": {"error_log": str(exc)[:200]},
                },
            )
