"""
YouTube Enrichment Worker.

Supplementary pass that attaches a YouTube ``video_id`` to tracks that
don't already have one.

For each track it searches YouTube Music for
``"{primary_artist} {title} official audio"`` (filter="songs") and
scores the returned result with rapidfuzz title + artist similarity.
A match is accepted when the combined score ≥ ``ytmusic_video_match_confidence``
(default 0.65 — lower than the Spotify candidate threshold because
YouTube Music titles often include extra suffixes).

State tracking:
    ``youtube_searched: bool`` is set ``True`` after every attempt (found or
    not) so the worker never re-visits the same track.  The flag is added to
    ``TrackDocument`` in v4.

Worker pattern:
    - Supplementary (does NOT change ``status``).
    - Optimistic locking via ``locked_at`` on the tracks collection.
    - Horizontally scalable.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from rapidfuzz import fuzz

from app.core.config import Settings
from app.db.collections import TRACKS_COL
from app.models.track import TrackStatus, YoutubeData
from app.services.ytmusic import YtMusicClient
from app.utils.deduplication import normalize_text
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID, LOCK_TIMEOUT_SECONDS

logger = structlog.get_logger(__name__)

# Statuses eligible for YouTube enrichment
_ELIGIBLE_STATUSES = [
    TrackStatus.BASE_COLLECTED.value,
    TrackStatus.AUDIO_FEATURES_ADDED.value,
    TrackStatus.LYRICS_ADDED.value,
    TrackStatus.ENRICHED.value,
]


class YoutubeEnrichmentWorker(BaseWorker):
    """
    Attaches YouTube video IDs to tracks that don't have one yet.

    Reads from ``tracks`` (``youtube_searched != True``).
    Searches YTMusic for ``artist title official audio``.
    Writes ``youtube`` sub-document and sets ``youtube_searched=True``.
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._ytmusic: Optional[YtMusicClient] = None

    async def on_startup(self) -> None:
        self._ytmusic = YtMusicClient(self.settings)
        logger.info("youtube_enrichment_worker_started")

    async def on_shutdown(self) -> None:
        pass  # YtMusicClient has no async resources to close

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Claim tracks that haven't been searched yet.

        Uses ``locked_at`` on the tracks collection for stale-lock recovery.
        """
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        batch = []

        for _ in range(self.settings.batch_size):
            doc = await col.find_one_and_update(
                {
                    "youtube_searched": {"$ne": True},
                    "youtube": {"$exists": False},   # no video attached yet
                    "status": {"$in": _ELIGIBLE_STATUSES},
                    "$or": [
                        {"locked_at": None},
                        {"locked_at": {"$lt": cutoff}},
                    ],
                },
                {
                    "$set": {
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
        for track in batch:
            await self._process_track(track)

    async def _process_track(self, track: Dict[str, Any]) -> None:
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        track_id = track["_id"]

        title: str = track.get("name") or ""
        artists_raw = track.get("artists") or []
        artist: str = artists_raw[0].get("name") if artists_raw else ""

        try:
            assert self._ytmusic is not None

            youtube_data: Optional[YoutubeData] = None

            if title and artist:
                youtube_data = await self._search_video(artist, title)

            update: Dict[str, Any] = {
                "youtube_searched": True,
                "locked_at": None,
                "locked_by": None,
                "updated_at": now,
            }
            if youtube_data is not None:
                update["youtube"] = youtube_data.model_dump(mode="python")
                await self.increment_stat("youtube_videos_attached", 1)
                logger.debug(
                    "youtube_video_attached",
                    track_name=title,
                    artist=artist,
                    video_id=youtube_data.video_id,
                    confidence=youtube_data.confidence,
                )

            await col.update_one(
                {"_id": track_id},
                {"$set": update},
            )

        except Exception as exc:
            logger.error(
                "youtube_enrichment_failed",
                title=title, artist=artist,
                error=str(exc), exc_info=True,
            )
            await col.update_one(
                {"_id": track_id},
                {"$set": {"locked_at": None, "locked_by": None, "updated_at": now}},
            )

    # ── Search logic ──────────────────────────────────────────────────────────

    async def _search_video(
        self, artist: str, title: str
    ) -> Optional[YoutubeData]:
        """
        Search YTMusic and return a ``YoutubeData`` if confident enough.

        Scoring: same 60 % title / 40 % artist formula as candidate_match_worker,
        but with the lower ``ytmusic_video_match_confidence`` threshold.
        """
        assert self._ytmusic is not None
        raw = await self._ytmusic.search_track_video(artist, title)
        if not raw:
            return None

        video_id: Optional[str] = raw.get("videoId") or None
        if not video_id:
            return None

        result_title = normalize_text(raw.get("title") or "")
        result_artists = raw.get("artists") or []
        result_artist = normalize_text(
            result_artists[0].get("name", "") if result_artists else ""
        )

        norm_title = normalize_text(title)
        norm_artist = normalize_text(artist)

        # Use max(token_sort, token_set) for better recall:
        # token_set handles "The Weeknd" vs "Weeknd", remastered suffix variants, etc.
        title_sim = max(
            fuzz.token_sort_ratio(norm_title, result_title),
            fuzz.token_set_ratio(norm_title, result_title),
        ) / 100.0
        artist_sim = max(
            fuzz.token_sort_ratio(norm_artist, result_artist),
            fuzz.token_set_ratio(norm_artist, result_artist),
        ) / 100.0
        confidence = 0.6 * title_sim + 0.4 * artist_sim

        if confidence < self.settings.ytmusic_video_match_confidence:
            return None

        # Parse duration from result
        from app.services.ytmusic import _parse_duration_string
        duration_ms: Optional[int] = None
        dur_sec = raw.get("duration_seconds")
        if dur_sec is not None:
            try:
                duration_ms = int(dur_sec) * 1000
            except (TypeError, ValueError):
                pass
        if duration_ms is None:
            duration_ms = _parse_duration_string(raw.get("duration") or "")

        # Channel name
        channel: Optional[str] = None
        channel_raw = raw.get("artists") or []
        if channel_raw and isinstance(channel_raw[0], dict):
            channel = channel_raw[0].get("name")

        return YoutubeData(
            video_id=video_id,
            confidence=round(confidence, 3),
            source="ytmusic_search",
            duration_ms=duration_ms,
            channel=channel,
        )
