"""
Shazam Chart Discovery Worker (v8).

Fetches top-200 chart tracks for 77 countries from Shazam's public API
and inserts them directly into the ``tracks`` collection as ``base_collected``.

Key advantages over candidate-based workers (Last.fm, YTMusic, Discogs):
  - Direct insertion (no candidate_match_worker hop needed).
  - ISRC available for ~95% of chart tracks → reliable deduplication.
  - Spotify-independent: no Spotify API calls at all.
  - Covers 77 countries including CIS, MENA, East/South/SE Asia.

Queue: ``shazam_seed_queue``
  One document per country. Each document has ``next_run_at`` — when to
  re-fetch that country's chart. Default cycle: 24 hours.
  On startup the queue is bootstrapped (idempotent); all countries are
  immediately due (next_run_at = epoch) so the first cycle starts instantly.

Scale: 2 replicas × 1.5 rps = 3 countries/sec → full 77-country cycle in ~26s.
Per cycle: 77 countries × ≤200 tracks = up to 15 400 tracks (deduped via ISRC).
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.core.config import Settings
from app.db.collections import SHAZAM_SEED_QUEUE_COL, TRACKS_COL
from app.models.track import ArtistRef, TrackStatus
from app.services.shazam import ShazamClient
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID, LOCK_TIMEOUT_SECONDS

logger = structlog.get_logger(__name__)

# 77 countries with Shazam chart coverage.
# Ordered: major markets first, then regional markets.
_SHAZAM_COUNTRIES: Dict[str, str] = {
    # North America & Oceania
    "US": "United States",
    "GB": "United Kingdom",
    "AU": "Australia",
    "CA": "Canada",
    "NZ": "New Zealand",
    # Western Europe
    "DE": "Germany",
    "FR": "France",
    "IT": "Italy",
    "ES": "Spain",
    "NL": "Netherlands",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "BE": "Belgium",
    "CH": "Switzerland",
    "AT": "Austria",
    "PT": "Portugal",
    "IE": "Ireland",
    # Eastern Europe
    "PL": "Poland",
    "RO": "Romania",
    "HU": "Hungary",
    "CZ": "Czech Republic",
    "SK": "Slovakia",
    "BG": "Bulgaria",
    "RS": "Serbia",
    "HR": "Croatia",
    "SI": "Slovenia",
    "GR": "Greece",
    # CIS
    "RU": "Russia",
    "UA": "Ukraine",
    "BY": "Belarus",
    "KZ": "Kazakhstan",
    "UZ": "Uzbekistan",
    "AZ": "Azerbaijan",
    "GE": "Georgia",
    "AM": "Armenia",
    "MD": "Moldova",
    # MENA
    "TR": "Turkey",
    "AE": "United Arab Emirates",
    "SA": "Saudi Arabia",
    "EG": "Egypt",
    "MA": "Morocco",
    "DZ": "Algeria",
    "TN": "Tunisia",
    "LB": "Lebanon",
    "KW": "Kuwait",
    "QA": "Qatar",
    "IL": "Israel",
    "IQ": "Iraq",
    "JO": "Jordan",
    "PK": "Pakistan",
    # East Asia
    "JP": "Japan",
    "KR": "South Korea",
    "CN": "China",
    "TW": "Taiwan",
    "HK": "Hong Kong",
    # South Asia
    "IN": "India",
    "BD": "Bangladesh",
    "LK": "Sri Lanka",
    # Southeast Asia
    "TH": "Thailand",
    "VN": "Vietnam",
    "ID": "Indonesia",
    "MY": "Malaysia",
    "PH": "Philippines",
    "SG": "Singapore",
    # Latin America
    "BR": "Brazil",
    "MX": "Mexico",
    "AR": "Argentina",
    "CO": "Colombia",
    "CL": "Chile",
    "PE": "Peru",
    "VE": "Venezuela",
    "EC": "Ecuador",
    # Africa
    "ZA": "South Africa",
    "NG": "Nigeria",
    "KE": "Kenya",
    "GH": "Ghana",
    "ET": "Ethiopia",
}

# Epoch used to mark items as "due immediately" on first bootstrap
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _shazam_fingerprint(title: str, artist_name: str) -> str:
    """
    Fingerprint for Shazam tracks (no duration available).

    Strips common suffixes before hashing to improve dedup across variants.
    Used only as fallback when ISRC is absent.
    """
    text = re.sub(
        r"\s*[\(\[](remaster(ed)?|live|radio edit|explicit|clean|version|edit|mix|remix).*?[\)\]]",
        "",
        title,
        flags=re.IGNORECASE,
    )
    norm_title = text.lower().strip()
    norm_artist = artist_name.lower().strip()
    # No duration → bucket = 0 (acceptable: ISRC covers 95% of chart tracks)
    raw = f"{norm_title}|{norm_artist}|shazam"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class ShazamWorker(BaseWorker):
    """
    Fetches Shazam chart tracks for 77 countries and inserts them directly
    into the ``tracks`` collection (no Spotify API required).

    Queue: shazam_seed_queue (one document per country)
    Output: tracks (status=base_collected)
    Cycle: every ``shazam_cycle_hours`` hours (default 24)
    """

    def __init__(
        self, db: AsyncIOMotorDatabase, settings: Settings  # type: ignore[type-arg]
    ) -> None:
        super().__init__(db, settings)
        self._shazam: Optional[ShazamClient] = None

    async def on_startup(self) -> None:
        self._shazam = ShazamClient(self.settings)
        await self._bootstrap_queue()
        logger.info(
            "shazam_worker_started",
            countries=len(_SHAZAM_COUNTRIES),
            cycle_hours=self.settings.shazam_cycle_hours,
        )

    async def on_shutdown(self) -> None:
        if self._shazam:
            await self._shazam.aclose()

    # ── Bootstrap ─────────────────────────────────────────────────────────────

    async def _bootstrap_queue(self) -> None:
        """
        Seed shazam_seed_queue with one document per country (idempotent).

        Uses $setOnInsert so existing documents are not overwritten.
        All countries start with next_run_at = epoch so they are immediately due.
        """
        col = self.db[SHAZAM_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)

        ops = [
            UpdateOne(
                {"country_code": code},
                {
                    "$setOnInsert": {
                        "country_code": code,
                        "country_name": name,
                        "next_run_at": _EPOCH,
                        "last_run_at": None,
                        "locked_at": None,
                        "locked_by": None,
                        "tracks_last_run": 0,
                        "runs_completed": 0,
                        "created_at": now,
                    }
                },
                upsert=True,
            )
            for code, name in _SHAZAM_COUNTRIES.items()
        ]

        try:
            result = await col.bulk_write(ops, ordered=False)
            if result.upserted_count:
                logger.info(
                    "shazam_queue_bootstrapped", inserted=result.upserted_count
                )
        except BulkWriteError as bwe:
            inserted = bwe.details.get("nUpserted", 0)
            if inserted:
                logger.info("shazam_queue_bootstrapped", inserted=inserted)

    # ── Queue claiming ─────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        col = self.db[SHAZAM_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        batch: List[Dict[str, Any]] = []

        # Process one country at a time per iteration
        doc = await col.find_one_and_update(
            {
                "next_run_at": {"$lte": now},
                "$or": [
                    {"locked_at": None},
                    {"locked_at": {"$lt": stale_cutoff}},
                ],
            },
            {
                "$set": {
                    "locked_at": now,
                    "locked_by": WORKER_INSTANCE_ID,
                }
            },
            sort=[("next_run_at", 1)],  # process most-overdue first
            return_document=True,
        )
        if doc:
            batch.append(doc)

        return batch

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            await self._process_country(item)

    async def _process_country(self, item: Dict[str, Any]) -> None:
        assert self._shazam is not None
        col = self.db[SHAZAM_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)

        country_code: str = item["country_code"]
        country_name: str = item.get("country_name", country_code)
        runs_completed: int = item.get("runs_completed", 0)

        try:
            tracks = await self._shazam.get_chart_tracks(country_code)

            inserted = 0
            for t in tracks:
                if await self._upsert_track(t):
                    inserted += 1

            cycle_hours = self.settings.shazam_cycle_hours
            next_run = now + timedelta(hours=cycle_hours)

            await col.update_one(
                {"country_code": country_code},
                {
                    "$set": {
                        "locked_at": None,
                        "locked_by": None,
                        "last_run_at": now,
                        "next_run_at": next_run,
                        "tracks_last_run": inserted,
                        "runs_completed": runs_completed + 1,
                    }
                },
            )

            if tracks:
                await self.increment_stat("shazam_tracks_inserted", inserted)
                await self.increment_stat("total_discovered", inserted)
                await self.increment_stat("total_base_collected", inserted)

            logger.info(
                "shazam_country_done",
                country=country_code,
                country_name=country_name,
                chart_size=len(tracks),
                inserted=inserted,
                run=runs_completed + 1,
                next_run_in_hours=cycle_hours,
            )

        except Exception as exc:
            logger.error(
                "shazam_country_failed",
                country=country_code,
                error=str(exc),
                exc_info=True,
            )
            # Release lock without rescheduling — will be retried on next poll
            await col.update_one(
                {"country_code": country_code},
                {"$set": {"locked_at": None, "locked_by": None}},
            )

    # ── Track upsert ──────────────────────────────────────────────────────────

    async def _upsert_track(self, raw: Dict[str, Any]) -> bool:
        """
        Upsert a Shazam chart track into the main tracks collection.

        Deduplication priority:
          1. ISRC (sparse unique index) — preferred, covers ~95% of chart tracks
          2. Fingerprint (title + artist hash) — fallback for missing ISRC

        ``spotify_id`` is set to ``"shazam:{key}"`` as a placeholder.
        Real Spotify ID is backfilled when artist_graph_worker or
        candidate_match_worker encounters the same track via Spotify/Deezer.

        Returns True if a new document was inserted.
        """
        title: str = raw.get("title", "").strip()
        artist: str = raw.get("artist", "").strip()
        shazam_key: str = raw.get("shazam_key", "").strip()
        isrc: Optional[str] = raw.get("isrc") or None

        if not title or not artist or not shazam_key:
            return False

        placeholder_id = f"shazam:{shazam_key}"
        fp = _shazam_fingerprint(title, artist)

        artist_ref = ArtistRef(
            spotify_id=f"shazam_artist:{shazam_key}",
            name=artist,
        )

        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        insert_doc: Dict[str, Any] = {
            "spotify_id": placeholder_id,
            "fingerprint": fp,
            "name": title,
            "artists": [{"spotify_id": artist_ref.spotify_id, "name": artist_ref.name}],
            "album": None,
            "popularity": 0,
            "duration_ms": 0,
            "explicit": False,
            "markets_count": 0,
            "markets": [],
            "status": TrackStatus.BASE_COLLECTED.value,
            "version_album_ids": [],
            "youtube_searched": False,
            "musicbrainz_enriched": False,
            "language_detected": False,
            "transliteration_done": False,
            "mb_priority": 0,
            "quality_score": 0.0,
            "regional_score": 0.0,
            "artist_followers": 0,
            "retry_count": 0,
            "error_log": [],
            "locked_at": None,
            "locked_by": None,
        }
        if isrc:
            insert_doc["isrc"] = isrc

        # ISRC preferred; fall back to fingerprint
        filter_q: Dict[str, Any] = {"isrc": isrc} if isrc else {"fingerprint": fp}

        try:
            result = await col.update_one(
                filter_q,
                {
                    "$setOnInsert": {**insert_doc, "created_at": now},
                    "$inc": {"appearance_score": 1},
                    "$set": {"updated_at": now},
                },
                upsert=True,
            )
            return result.upserted_id is not None

        except Exception as exc:
            if "duplicate key" in str(exc).lower():
                return False
            logger.error(
                "shazam_track_upsert_error",
                shazam_key=shazam_key,
                title=title,
                error=str(exc),
            )
            return False
