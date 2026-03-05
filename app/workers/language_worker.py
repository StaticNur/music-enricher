"""
Language Detection Worker.

Runs orthogonally to the main pipeline — does NOT change ``status``.
Sets ``language``, ``script``, ``language_detected``, ``regions``,
and ``mb_priority`` on tracks where ``language_detected = False``.

Priority order for language source:
1. MusicBrainz data (if already enriched) — most authoritative
2. Genius lyrics text (if available) — reliable for known lyrics
3. Track name / artist name script detection — last resort

Script detection is always applied (even if language detection fails)
because knowing a track uses Cyrillic/Arabic is useful for:
- transliteration_worker routing
- MusicBrainz query normalization
- UI display

Eligibility: ANY status stage after base_collected, language_detected=False.
This covers new tracks AND existing enriched tracks that pre-date v2.
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
from app.utils.regional import classify_regions, compute_mb_priority
from app.utils.transliteration import detect_script, normalize_text
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)

# Statuses eligible for language detection (all except initial discovered)
_ELIGIBLE_STATUSES = [
    TrackStatus.BASE_COLLECTED.value,
    TrackStatus.AUDIO_FEATURES_ADDED.value,
    TrackStatus.LYRICS_ADDED.value,
    TrackStatus.ENRICHED.value,
    TrackStatus.FILTERED_OUT.value,
]

# Batch concurrency for langdetect (CPU-bound but fast)
_LANG_CONCURRENCY = 10


class LanguageWorker(BaseWorker):
    """
    Detects language and script for all tracks that haven't been processed yet.

    Sets:
    - ``language``          ISO 639-1 code
    - ``script``            Script name (cyrillic, arabic, latin, etc.)
    - ``language_source``   Where the language was determined
    - ``language_detected`` True once this worker has processed the track
    - ``regions``           Regional classification (CIS / Central Asia / MENA)
    - ``mb_priority``       MusicBrainz priority based on region/language
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._semaphore = asyncio.Semaphore(_LANG_CONCURRENCY)

    async def on_startup(self) -> None:
        logger.info("language_worker_started")

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Claim tracks where language hasn't been detected yet.

        Also reclaims stale locks (standard pattern).
        Prioritizes: lyrics_added/enriched first (most data available).
        """
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        batch: List[Dict[str, Any]] = []

        cursor = (
            col.find(
                {
                    "language_detected": {"$ne": True},
                    "status": {"$in": _ELIGIBLE_STATUSES},
                }
            )
            # Project only fields we need to minimize data transfer
            .sort([("status", -1)])  # lyrics_added first
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
        tasks = [self._detect_language(doc) for doc in batch]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _detect_language(self, doc: Dict[str, Any]) -> None:
        async with self._semaphore:
            await self._do_detect(doc)

    async def _do_detect(self, doc: Dict[str, Any]) -> None:
        sid: str = doc["spotify_id"]
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        try:
            language, script, source = self._resolve_language_and_script(doc)

            markets: List[str] = doc.get("markets", [])
            artist_country: Optional[str] = (
                (doc.get("regions") or {}).get("artist_country")
                if doc.get("regions") else None
            )
            mb_release_countries: Optional[List[str]] = (
                (doc.get("musicbrainz") or {}).get("release_countries")
                if doc.get("musicbrainz") else None
            )

            regions = classify_regions(
                markets=markets,
                language=language,
                artist_country=artist_country,
                mb_release_countries=mb_release_countries,
            )

            mb_priority = compute_mb_priority(
                regions=regions,
                language=language,
                markets=markets,
            )

            update: Dict[str, Any] = {
                "language_detected": True,
                "regions": regions,
                "locked_at": None,
                "updated_at": now,
            }
            if language:
                update["language"] = language
                update["language_source"] = source
            if script:
                update["script"] = script

            # Only increase mb_priority, never decrease
            if mb_priority > doc.get("mb_priority", 0):
                update["mb_priority"] = mb_priority

            await col.update_one({"spotify_id": sid}, {"$set": update})

            logger.debug(
                "language_detected",
                spotify_id=sid,
                language=language,
                script=script,
                source=source,
                regions_cis=regions.get("cis"),
                regions_ca=regions.get("central_asia"),
                regions_mena=regions.get("mena"),
            )

        except Exception as exc:
            logger.error(
                "language_detection_failed",
                spotify_id=sid,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"spotify_id": sid},
                {"$set": {
                    "language_detected": True,  # Mark done to prevent retry loops
                    "locked_at": None,
                    "updated_at": now,
                }},
            )

    def _resolve_language_and_script(
        self, doc: Dict[str, Any]
    ) -> tuple[Optional[str], Optional[str], str]:
        """
        Resolve language, script, and source for a track document.

        Returns:
            (language_code, script_name, source_label)
        """
        # 1. MusicBrainz — most authoritative
        mb = doc.get("musicbrainz") or {}
        if mb.get("language") and doc.get("musicbrainz_enriched"):
            return mb["language"], mb.get("script"), "musicbrainz"

        # 2. Genius lyrics text
        lyrics = doc.get("lyrics") or {}
        lyrics_text: str = lyrics.get("text") or ""
        if len(lyrics_text) >= self.settings.language_min_text_len:
            lang = self._detect_lang_from_text(lyrics_text)
            if lang:
                script = self._detect_script_from_text(lyrics_text)
                return lang, script, "lyrics"

        # 3. Track name script detection (can determine script even without language)
        name: str = doc.get("name", "")
        artists: List[dict] = doc.get("artists", [])
        combined = " ".join(filter(None, [name] + [a.get("name", "") for a in artists[:2]]))

        script = self._detect_script_from_text(combined) if combined else None

        # Attempt language detection from title (less reliable, short text)
        lang = None
        if combined and len(combined) >= 4:
            lang = self._detect_lang_from_text(combined)

        return lang, script, "title_detection"

    @staticmethod
    def _detect_lang_from_text(text: str) -> Optional[str]:
        """Detect language using langdetect. Returns ISO 639-1 or None."""
        if not text:
            return None
        try:
            from langdetect import detect, LangDetectException
            return detect(text[:1000])
        except Exception:
            return None

    @staticmethod
    def _detect_script_from_text(text: str) -> Optional[str]:
        """Detect dominant Unicode script."""
        if not text:
            return None
        script = detect_script(text)
        if script in ("unknown", "mixed"):
            return None
        return script
