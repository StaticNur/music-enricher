"""
Transliteration Worker.

Processes tracks where:
- ``transliteration_done = False``
- ``script`` is one of: cyrillic, arabic, georgian, armenian
- ``language`` is known (set by language_worker)

Generates three fields:
- ``normalized_name``        — Unicode NFKC + stripped + lowercased
- ``transliterated_name``    — Script → Latin transliteration
- ``normalized_artist_name`` — Same normalization for primary artist

These fields are used by:
1. MusicBrainz worker — sends transliterated name to MB search for better recall
2. Genius worker — fallback query for Cyrillic-named tracks
3. Deduplication — cross-script matching (e.g. "Dua Lipa" == "Дуа Липа")
4. Analytics / export — Latin representations for downstream processing

Tracks already in Latin script get ``normalized_name`` only
(no transliteration needed).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import Settings
from app.db.collections import TRACKS_COL
from app.utils.transliteration import (
    normalize_text,
    transliterate_track_name,
    detect_script,
)
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)

# Scripts that require transliteration
_TRANSLITERABLE_SCRIPTS = {"cyrillic", "arabic", "georgian", "armenian"}

# Concurrency for CPU-bound transliteration
_TRANSLIT_CONCURRENCY = 20


class TransliterationWorker(BaseWorker):
    """
    Generates normalized and transliterated name fields for non-Latin tracks.

    Does NOT change ``status`` — operates orthogonally to the main pipeline.
    Sets ``transliteration_done = True`` on completion.
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._semaphore = asyncio.Semaphore(_TRANSLIT_CONCURRENCY)

    async def on_startup(self) -> None:
        logger.info("transliteration_worker_started")

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Claim tracks that need transliteration.

        Two sub-cases:
        A) Non-Latin script → transliteration + normalization needed
        B) Latin or undetermined script → normalization only (quick pass)

        We handle both in the same worker. Case B is implicitly handled
        for all tracks not yet touched.
        """
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        batch: List[Dict[str, Any]] = []

        cursor = (
            col.find(
                {
                    "transliteration_done": {"$ne": True},
                    "normalized_name": None,      # not yet normalized at all
                }
            )
            .sort([("mb_priority", -1)])           # regional tracks first
            .limit(self.settings.batch_size)
        )
        async for doc in cursor:
            batch.append(doc)

        if not batch:
            return []

        ids = [doc["spotify_id"] for doc in batch]
        await col.update_many(
            {"spotify_id": {"$in": ids}},
            {"$set": {
                "locked_at": now,
                "locked_by": WORKER_INSTANCE_ID,
                "updated_at": now,
            }},
        )
        return batch

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        tasks = [self._process_track(doc) for doc in batch]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_track(self, doc: Dict[str, Any]) -> None:
        async with self._semaphore:
            await self._do_transliterate(doc)

    async def _do_transliterate(self, doc: Dict[str, Any]) -> None:
        sid: str = doc["spotify_id"]
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        try:
            name: str = doc.get("name", "")
            artists: List[dict] = doc.get("artists", [])
            primary_artist = artists[0].get("name", "") if artists else ""
            language: Optional[str] = doc.get("language")

            # Use stored script or re-detect from name
            script: Optional[str] = doc.get("script")
            if not script and name:
                detected = detect_script(name)
                script = detected if detected not in ("unknown", "latin", "mixed") else None

            # Normalized name (always generated)
            normalized_name = normalize_text(name)
            normalized_artist = normalize_text(primary_artist)

            # Transliteration (only for non-Latin scripts)
            transliterated_name: Optional[str] = None
            if script in _TRANSLITERABLE_SCRIPTS:
                transliterated_name = transliterate_track_name(
                    name=name,
                    script=script,
                    language=language,
                )

            update: Dict[str, Any] = {
                "normalized_name": normalized_name,
                "normalized_artist_name": normalized_artist,
                "transliteration_done": True,
                "locked_at": None,
                "updated_at": now,
            }
            if transliterated_name:
                update["transliterated_name"] = transliterated_name

            await col.update_one({"spotify_id": sid}, {"$set": update})

            logger.debug(
                "transliteration_done",
                spotify_id=sid,
                script=script,
                language=language,
                has_transliteration=transliterated_name is not None,
            )

        except Exception as exc:
            logger.error(
                "transliteration_failed",
                spotify_id=sid,
                error=str(exc),
                exc_info=True,
            )
            # Mark done to prevent infinite retry loop
            await col.update_one(
                {"spotify_id": sid},
                {"$set": {
                    "transliteration_done": True,
                    "locked_at": None,
                    "updated_at": now,
                }},
            )
