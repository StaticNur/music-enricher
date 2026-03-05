"""
MusicBrainz Enrichment Worker.

Polls ``tracks`` for documents where ``musicbrainz_enriched = False``
and either:
  - quality_score ≥ settings.musicbrainz_priority_threshold
  - OR mb_priority > 0  (regional tracks, always processed regardless of score)

Claims are sorted by ``mb_priority DESC`` so Central Asia tracks are
processed before CIS, before MENA, before general catalog.

After a successful match, updates the track with:
- ``musicbrainz``          → MusicBrainzData subdocument
- ``musicbrainz_confidence_score``
- ``musicbrainz_enriched = True``
- ``language`` / ``script`` if not already set (MB is authoritative)
- ``regions.artist_country`` if found
- ``mb_priority`` update if artist_country reveals regional affiliation

Respects the strict MusicBrainz 1 req/s policy via the rate limiter.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import Settings
from app.db.collections import TRACKS_COL
from app.models.track import TrackStatus
from app.services.musicbrainz import MusicBrainzClient, MBRecordingMatch
from app.utils.regional import classify_regions, compute_mb_priority
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)


class MusicBrainzWorker(BaseWorker):
    """
    Supplementary enrichment worker for MusicBrainz metadata.

    Does NOT change the primary ``status`` field — it operates
    orthogonally to the main pipeline and sets ``musicbrainz_enriched``
    as its own completion flag.

    Priority ordering (MongoDB sort):
        mb_priority DESC → claimed first
        created_at ASC   → FIFO within same priority tier
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._mb: Optional[MusicBrainzClient] = None

    async def on_startup(self) -> None:
        self._mb = MusicBrainzClient(self.settings)
        logger.info(
            "musicbrainz_worker_started",
            priority_threshold=self.settings.musicbrainz_priority_threshold,
        )

    async def on_shutdown(self) -> None:
        if self._mb:
            await self._mb.aclose()

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Claim tracks for MusicBrainz enrichment.

        Eligibility:
        - musicbrainz_enriched = False (not yet done)
        - AND (quality_score ≥ threshold OR mb_priority > 0)
        - AND status NOT IN [discovered, base_collected]  ← at least AF stage

        Sorted by mb_priority DESC to process regional tracks first.
        """
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        threshold = self.settings.musicbrainz_priority_threshold

        eligible_statuses = [
            TrackStatus.AUDIO_FEATURES_ADDED.value,
            TrackStatus.LYRICS_ADDED.value,
            TrackStatus.ENRICHED.value,
            TrackStatus.FILTERED_OUT.value,
        ]

        filter_q: Dict[str, Any] = {
            "musicbrainz_enriched": {"$ne": True},
            "status": {"$in": eligible_statuses},
            "$or": [
                {"quality_score": {"$gte": threshold}},
                {"mb_priority": {"$gt": 0}},
            ],
        }

        batch: List[Dict[str, Any]] = []
        cursor = (
            col.find(filter_q)
            .sort([("mb_priority", -1), ("created_at", 1)])
            .limit(self.settings.batch_size)
        )
        async for doc in cursor:
            batch.append(doc)

        if not batch:
            return []

        ids = [doc["spotify_id"] for doc in batch]
        await col.update_many(
            {"spotify_id": {"$in": ids}},
            {
                "$set": {
                    "locked_at": now,
                    "locked_by": WORKER_INSTANCE_ID,
                    "updated_at": now,
                }
            },
        )
        return batch

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process tracks one-at-a-time — MusicBrainz is 1 req/s."""
        for doc in batch:
            if not self._mb:
                break
            await self._enrich_track(doc)

    async def _enrich_track(self, doc: Dict[str, Any]) -> None:
        sid: str = doc["spotify_id"]
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        track_name: str = doc.get("name", "")
        artists: List[dict] = doc.get("artists", [])
        artist_name: str = artists[0].get("name", "") if artists else ""
        duration_ms: int = doc.get("duration_ms", 0)
        isrc: Optional[str] = doc.get("isrc")

        try:
            assert self._mb is not None
            match: Optional[MBRecordingMatch] = await self._mb.find_best_match(
                track_name=track_name,
                artist_name=artist_name,
                duration_ms=duration_ms,
                isrc=isrc,
            )

            update: Dict[str, Any] = {
                "musicbrainz_enriched": True,
                "locked_at": None,
                "updated_at": now,
            }

            if match:
                mb_doc = {
                    "mbid": match.mbid,
                    "canonical_title": match.canonical_title,
                    "aliases": match.aliases,
                    "composers": match.composers,
                    "lyricists": match.lyricists,
                    "first_release_date": match.first_release_date,
                    "release_countries": match.release_countries,
                    "labels": match.labels,
                    "language": match.language,
                    "script": match.script,
                    "tags": match.tags,
                }
                update["musicbrainz"] = mb_doc
                update["musicbrainz_confidence_score"] = match.confidence

                # Update language/script only if not already set from a more
                # reliable source (lyrics langdetect took priority earlier)
                if not doc.get("language") and match.language:
                    update["language"] = match.language
                    update["language_source"] = "musicbrainz"
                    update["language_detected"] = True

                if not doc.get("script") and match.script:
                    update["script"] = match.script

                # Fetch artist country and update regional data
                artist_country: Optional[str] = await self._fetch_artist_country(match)

                # Update regional classification with new data.
                # classify_regions() already embeds artist_country and
                # artist_origin_region in the returned dict, so we must NOT
                # also set update["regions.artist_country"] separately —
                # that would create a MongoDB path conflict with update["regions"].
                markets = doc.get("markets", [])
                language_for_region = update.get("language") or doc.get("language")
                regions = classify_regions(
                    markets=markets,
                    language=language_for_region,
                    artist_country=artist_country,
                    mb_release_countries=match.release_countries,
                )
                update["regions"] = regions

                # Recalculate mb_priority with enriched data
                new_priority = compute_mb_priority(
                    regions=regions,
                    language=language_for_region,
                    markets=markets,
                )
                if new_priority > doc.get("mb_priority", 0):
                    update["mb_priority"] = new_priority

                logger.info(
                    "mb_enriched",
                    spotify_id=sid,
                    mbid=match.mbid,
                    confidence=match.confidence,
                    language=match.language,
                    release_countries=match.release_countries[:5],
                )
                await self.increment_stat("mb_enriched")
            else:
                update["musicbrainz_confidence_score"] = 0.0
                logger.debug("mb_no_match", spotify_id=sid, track=track_name)
                await self.increment_stat("mb_no_match")

            await col.update_one({"spotify_id": sid}, {"$set": update})

        except Exception as exc:
            logger.error(
                "mb_enrichment_failed",
                spotify_id=sid,
                error=str(exc),
                exc_info=True,
            )
            retry = doc.get("retry_count", 0) + 1
            if retry >= self.settings.worker_retry_limit:
                # Give up — mark as enriched=True with no data so it's not
                # retried endlessly
                await col.update_one(
                    {"spotify_id": sid},
                    {
                        "$set": {
                            "musicbrainz_enriched": True,
                            "locked_at": None,
                            "updated_at": now,
                        },
                        "$push": {"error_log": f"MB: {str(exc)[:200]}"},
                    },
                )
            else:
                await col.update_one(
                    {"spotify_id": sid},
                    {
                        "$set": {
                            "retry_count": retry,
                            "locked_at": None,
                            "updated_at": now,
                        },
                        "$push": {"error_log": f"MB: {str(exc)[:200]}"},
                    },
                )

    async def _fetch_artist_country(
        self, match: MBRecordingMatch
    ) -> Optional[str]:
        """
        Fetch artist country from MusicBrainz using the first composer or
        recording artist relation (best-effort).
        """
        assert self._mb is not None
        # We can only get artist country if we have artist MBID from the detail.
        # The MBRecordingMatch doesn't store artist MBID currently, so we
        # do a quick search — but this costs an extra API call.
        # For now, derive country from release_countries (majority vote).
        if not match.release_countries:
            return None

        from app.utils.regional import (
            CENTRAL_ASIA_COUNTRIES, CIS_COUNTRIES, MENA_COUNTRIES
        )
        # Prefer the most specific regional country
        for priority_set in [CENTRAL_ASIA_COUNTRIES, CIS_COUNTRIES, MENA_COUNTRIES]:
            for c in match.release_countries:
                if c.upper() in priority_set:
                    return c.upper()
        return match.release_countries[0] if match.release_countries else None


def _region_from_country(country: Optional[str]) -> Optional[str]:
    if not country:
        return None
    from app.utils.regional import (
        CENTRAL_ASIA_COUNTRIES, CIS_COUNTRIES, MENA_COUNTRIES
    )
    c = country.upper()
    if c in CENTRAL_ASIA_COUNTRIES:
        return "central_asia"
    if c in CIS_COUNTRIES:
        return "cis"
    if c in MENA_COUNTRIES:
        return "mena"
    return None
