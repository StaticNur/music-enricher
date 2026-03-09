"""
SoundCloud Discovery Worker (v9).

Discovers independent/underground music from SoundCloud and inserts
directly into ``tracks`` collection as ``base_collected``.

SoundCloud is the dominant platform for independent artists — hip-hop,
electronic, lo-fi, hyperpop, drill, and more — genres underrepresented
in Deezer/iTunes catalog.

Requires client_id (auto-discovered from SoundCloud web JS, or set
SOUNDCLOUD_CLIENT_ID in .env). Worker is idle (no-op) if client_id
cannot be discovered after 3 attempts.

Queue: soundcloud_seed_queue
  Item types:
  - {"item_type": "genre_chart", "item_id": "{genre}:{kind}", "genre": str, "kind": str}
  - {"item_type": "search", "item_id": query, "query": str}

Uses next_run_at pattern (same as Shazam) with 12-hour cycle.

Scale: 1 replica (SoundCloud throttles aggressively).
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
from app.db.collections import SOUNDCLOUD_SEED_QUEUE_COL, TRACKS_COL
from app.models.track import ArtistRef, TrackStatus
from app.services.soundcloud import SoundCloudClient
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID, LOCK_TIMEOUT_SECONDS

logger = structlog.get_logger(__name__)

# Epoch used to mark items as "due immediately" on first bootstrap
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# SoundCloud genres available for chart fetching.
_SC_GENRES: List[str] = [
    "all-music", "hiphoprap", "electronic", "rock", "pop", "danceedm",
    "rnbsoul", "alternative", "latin", "classical", "country", "jazz",
    "folk", "metal", "triphop", "world", "piano", "acoustic",
    "ambient", "dancehall", "reggae", "reggaeton", "techno", "house",
    "trance", "drum-and-bass", "dubstep", "trap",
]

# Search queries to discover independent/underground artists from all regions.
_SC_SEARCH_QUERIES: List[str] = [
    "underground hip hop", "lo-fi", "hyperpop", "phonk",
    "uk drill", "afrobeats", "amapiano", "afropop",
    "russian rap", "arabic trap", "turkish rap", "persian rap",
    "k-indie", "j-indie", "chinese trap", "hindi rap",
    "dark ambient", "experimental", "vaporwave", "synthwave",
]

# Maximum number of client_id discovery attempts before giving up.
_MAX_DISCOVERY_ATTEMPTS = 3


def _soundcloud_fingerprint(title: str, artist: str, duration_ms: int) -> str:
    """
    Fingerprint for SoundCloud tracks (no ISRC available).

    Strips bracketed suffixes, lowercases, and buckets duration to ±2s.
    """
    norm_title = re.sub(
        r"\s*[\(\[](.*?)[\)\]]", "", title, flags=re.IGNORECASE
    ).lower().strip()
    norm_artist = artist.lower().strip()
    bucket = round(duration_ms / 2000) * 2000
    raw = f"{norm_title}|{norm_artist}|{bucket}|soundcloud"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class SoundCloudWorker(BaseWorker):
    """
    Discovers tracks from SoundCloud and inserts them directly into the pipeline.

    Queue: ``soundcloud_seed_queue`` (genre chart and search items)
    Output: ``tracks`` (status=base_collected)
    Cycle: every ``soundcloud_cycle_hours`` hours (default 12)
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._client: Optional[SoundCloudClient] = None

    async def on_startup(self) -> None:
        self._client = SoundCloudClient(self.settings)

        # Discover client_id fresh from JS bundles (up to _MAX_DISCOVERY_ATTEMPTS tries)
        available = False
        for attempt in range(1, _MAX_DISCOVERY_ATTEMPTS + 1):
            available = await self._client.ensure_client_id()
            if available:
                break
            logger.warning(
                "soundcloud_client_id_discovery_attempt_failed",
                attempt=attempt,
                max_attempts=_MAX_DISCOVERY_ATTEMPTS,
            )

        if not available:
            logger.warning("soundcloud_worker_idle_no_client_id")
            return

        await self._bootstrap_queue()
        logger.info(
            "soundcloud_worker_started",
            cycle_hours=self.settings.soundcloud_cycle_hours,
        )

    async def on_shutdown(self) -> None:
        if self._client:
            await self._client.aclose()

    # ── Seeding ───────────────────────────────────────────────────────────────

    async def _bootstrap_queue(self) -> None:
        """
        Seed soundcloud_seed_queue with genre chart + search items (idempotent).

        Uses next_run_at=epoch so all items are immediately due.
        """
        col = self.db[SOUNDCLOUD_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        ops: List[Any] = []

        # Genre chart items: trending + top for each genre
        for genre in _SC_GENRES:
            for kind in ("trending", "top"):
                item_id = f"{genre}:{kind}"
                ops.append(UpdateOne(
                    {"item_type": "genre_chart", "item_id": item_id},
                    {"$setOnInsert": {
                        "item_type": "genre_chart",
                        "item_id": item_id,
                        "genre": genre,
                        "kind": kind,
                        "next_run_at": _EPOCH,
                        "last_run_at": None,
                        "locked_at": None,
                        "locked_by": None,
                        "tracks_last_run": 0,
                        "runs_completed": 0,
                        "created_at": now,
                    }},
                    upsert=True,
                ))

        # Search query items
        for query in _SC_SEARCH_QUERIES:
            item_id = f"search:{query}"
            ops.append(UpdateOne(
                {"item_type": "search", "item_id": item_id},
                {"$setOnInsert": {
                    "item_type": "search",
                    "item_id": item_id,
                    "query": query,
                    "next_run_at": _EPOCH,
                    "last_run_at": None,
                    "locked_at": None,
                    "locked_by": None,
                    "tracks_last_run": 0,
                    "runs_completed": 0,
                    "created_at": now,
                }},
                upsert=True,
            ))

        if not ops:
            return

        try:
            result = await col.bulk_write(ops, ordered=False)
            if result.upserted_count:
                logger.info("soundcloud_queue_bootstrapped", inserted=result.upserted_count)
        except BulkWriteError as bwe:
            inserted = bwe.details.get("nUpserted", 0)
            if inserted:
                logger.info("soundcloud_queue_bootstrapped", inserted=inserted)

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        If worker has no client_id, return empty batch (idle mode).

        Otherwise claim one item that is due (next_run_at <= now) and not locked.
        """
        if not self._client_id_available:
            # Re-attempt discovery periodically
            if self._client is not None:
                ok = await self._client.ensure_client_id()
                if ok:
                    self._client_id_available = True
                    await self._bootstrap_queue()
                    logger.info("soundcloud_client_id_recovered")
                else:
                    return []
            else:
                return []

        col = self.db[SOUNDCLOUD_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)

        doc = await col.find_one_and_update(
            {
                "next_run_at": {"$lte": now},
                "$or": [
                    {"locked_at": None},
                    {"locked_at": {"$lt": stale_cutoff}},
                ],
            },
            {"$set": {
                "locked_at": now,
                "locked_by": WORKER_INSTANCE_ID,
            }},
            sort=[("next_run_at", 1)],  # process most-overdue first
            return_document=True,
        )

        return [doc] if doc else []

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            item_type = item.get("item_type", "")
            if item_type == "genre_chart":
                await self._process_genre_chart(item)
            elif item_type == "search":
                await self._process_search(item)
            else:
                await self._release_item(item)

    async def _process_genre_chart(self, item: Dict[str, Any]) -> None:
        assert self._client is not None
        col = self.db[SOUNDCLOUD_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        genre: str = item["genre"]
        kind: str = item.get("kind", "trending")
        runs_completed: int = item.get("runs_completed", 0)

        try:
            tracks = await self._client.get_chart_tracks(genre=genre, kind=kind, limit=200)
            inserted = 0
            for t in tracks:
                if await self._upsert_track(t):
                    inserted += 1

            cycle_hours = self.settings.soundcloud_cycle_hours
            next_run = now + timedelta(hours=cycle_hours)

            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {
                    "locked_at": None,
                    "locked_by": None,
                    "last_run_at": now,
                    "next_run_at": next_run,
                    "tracks_last_run": inserted,
                    "runs_completed": runs_completed + 1,
                }},
            )

            if tracks:
                await self.increment_stat("soundcloud_tracks_inserted", inserted)
                await self.increment_stat("total_discovered", inserted)
                await self.increment_stat("total_base_collected", inserted)

            logger.info(
                "soundcloud_genre_chart_done",
                genre=genre,
                kind=kind,
                chart_size=len(tracks),
                inserted=inserted,
                run=runs_completed + 1,
                next_run_in_hours=cycle_hours,
            )

        except Exception as exc:
            logger.error(
                "soundcloud_genre_chart_failed",
                genre=genre,
                kind=kind,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"locked_at": None, "locked_by": None}},
            )

    async def _process_search(self, item: Dict[str, Any]) -> None:
        assert self._client is not None
        col = self.db[SOUNDCLOUD_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        query: str = item["query"]
        runs_completed: int = item.get("runs_completed", 0)

        try:
            tracks = await self._client.search_tracks(query, limit=200)
            inserted = 0
            for t in tracks:
                if await self._upsert_track(t):
                    inserted += 1

            cycle_hours = self.settings.soundcloud_cycle_hours
            next_run = now + timedelta(hours=cycle_hours)

            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {
                    "locked_at": None,
                    "locked_by": None,
                    "last_run_at": now,
                    "next_run_at": next_run,
                    "tracks_last_run": inserted,
                    "runs_completed": runs_completed + 1,
                }},
            )

            if tracks:
                await self.increment_stat("soundcloud_tracks_inserted", inserted)
                await self.increment_stat("total_discovered", inserted)
                await self.increment_stat("total_base_collected", inserted)

            logger.info(
                "soundcloud_search_done",
                query=query,
                tracks_found=len(tracks),
                inserted=inserted,
                run=runs_completed + 1,
                next_run_in_hours=cycle_hours,
            )

        except Exception as exc:
            logger.error(
                "soundcloud_search_failed",
                query=query,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"locked_at": None, "locked_by": None}},
            )

    async def _release_item(self, item: Dict[str, Any]) -> None:
        """Release lock on an unknown item type without rescheduling."""
        col = self.db[SOUNDCLOUD_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        cycle_hours = self.settings.soundcloud_cycle_hours
        await col.update_one(
            {"_id": item["_id"]},
            {"$set": {
                "locked_at": None,
                "locked_by": None,
                "next_run_at": now + timedelta(hours=cycle_hours),
            }},
        )

    # ── Track upsert ──────────────────────────────────────────────────────────

    async def _upsert_track(self, track: Dict[str, Any]) -> bool:
        """
        Upsert a SoundCloud track into the main tracks collection.

        Deduplication: fingerprint only (no ISRC from SoundCloud).
        ``spotify_id`` is set to ``"soundcloud:{id}"`` as placeholder.
        Duration is already in milliseconds (SoundCloud API convention).
        Returns True if a new document was inserted.
        """
        track_id = track.get("id")
        title: str = track.get("title", "").strip()
        artist: str = track.get("artist", "").strip()
        duration_ms: int = track.get("duration_ms", 0)  # already ms

        if not track_id or not title or not artist:
            return False

        placeholder_id = f"soundcloud:{track_id}"
        fp = _soundcloud_fingerprint(title, artist, duration_ms)

        artist_ref = ArtistRef(
            spotify_id=f"soundcloud_artist:{track_id}",
            name=artist,
        )

        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        insert_doc: Dict[str, Any] = {
            "spotify_id": placeholder_id,
            "isrc": None,
            "fingerprint": fp,
            "name": title,
            "artists": [{"spotify_id": artist_ref.spotify_id, "name": artist_ref.name}],
            "album": None,
            "popularity": 0,
            "duration_ms": duration_ms,
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

        try:
            result = await col.update_one(
                {"fingerprint": fp},
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
                "soundcloud_track_upsert_error",
                track_id=track_id,
                title=title,
                error=str(exc),
            )
            return False
