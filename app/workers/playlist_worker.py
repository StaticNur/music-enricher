"""
Playlist Worker.

Polls ``playlist_queue`` for PENDING items, fetches all tracks from each
playlist, and inserts new tracks into the ``tracks`` collection with
status=base_collected.

For each track already in the DB, increments ``appearance_score`` to
reflect how many playlists feature it — a proxy for all-time popularity.

Deduplication uses ISRC first, then fingerprint fallback.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app.core.config import Settings
from app.db.collections import PLAYLIST_QUEUE_COL, TRACKS_COL, ARTIST_QUEUE_COL
from app.models.track import TrackDocument, TrackStatus, ArtistRef, AlbumRef
from app.models.queue import QueueStatus
from app.services.spotify import SpotifyClient
from app.utils.deduplication import compute_fingerprint, extract_isrc
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)


def _parse_track(raw: dict) -> Optional[TrackDocument]:
    """
    Convert a raw Spotify track dict into a ``TrackDocument``.

    Returns ``None`` if the track is missing required fields.
    """
    if not raw or not raw.get("id"):
        return None

    spotify_id: str = raw["id"]
    name: str = raw.get("name") or ""
    if not name:
        return None

    # Artists
    artists = [
        ArtistRef(spotify_id=a["id"], name=a.get("name") or "")
        for a in raw.get("artists", [])
        if a.get("id")
    ]
    if not artists:
        return None

    # Album
    album_raw = raw.get("album") or {}
    album: Optional[AlbumRef] = None
    if album_raw.get("id"):
        album = AlbumRef(
            spotify_id=album_raw["id"],
            name=album_raw.get("name") or "",
            release_date=album_raw.get("release_date"),
            album_type=album_raw.get("album_type"),
            total_tracks=album_raw.get("total_tracks"),
            images=album_raw.get("images", []),
        )

    isrc = extract_isrc(raw)
    markets_list = raw.get("available_markets") or []
    markets_count = len(markets_list)

    fp = compute_fingerprint(name, artists[0].spotify_id, raw.get("duration_ms", 0))

    return TrackDocument(
        spotify_id=spotify_id,
        isrc=isrc,
        fingerprint=fp,
        name=name,
        artists=artists,
        album=album,
        popularity=raw.get("popularity", 0),
        duration_ms=raw.get("duration_ms", 0),
        explicit=raw.get("explicit", False),
        markets_count=markets_count,
        markets=markets_list,       # v2: store full market list for regional scoring
        status=TrackStatus.BASE_COLLECTED,
        appearance_score=1,
    )


class PlaylistWorker(BaseWorker):
    """
    Fetches tracks from playlists in the playlist_queue.

    State transitions:
        playlist_queue: pending → processing → done | failed
        tracks:         (new) → base_collected
                        (existing) → appearance_score += 1
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._spotify: Optional[SpotifyClient] = None

    async def on_startup(self) -> None:
        self._spotify = SpotifyClient(self.settings)
        logger.info("playlist_worker_started")

    async def on_shutdown(self) -> None:
        if self._spotify:
            await self._spotify.aclose()

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Atomically claim up to ``batch_size`` pending playlist queue items.

        Also reclaims stale locks (from crashed workers).
        """
        if self._spotify and self._spotify.is_circuit_open:
            logger.warning("playlist_worker_idle_circuit_open", reason="Spotify circuit breaker is OPEN — skipping claim")
            return []
        col = self.db[PLAYLIST_QUEUE_COL]
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
                        "status": "processing",
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
        """Process a batch of playlist queue items with bounded concurrency."""
        semaphore = asyncio.Semaphore(3)

        async def _guarded(item: Dict[str, Any]) -> None:
            async with semaphore:
                await self._process_playlist(item)

        tasks = [_guarded(item) for item in batch]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_playlist(self, item: Dict[str, Any]) -> None:
        """
        Fetch all tracks from one playlist and upsert them into ``tracks``.

        On success: marks playlist as DONE.
        On failure: increments retry_count; marks FAILED after limit.
        """
        playlist_id: str = item["spotify_id"]
        col_pl = self.db[PLAYLIST_QUEUE_COL]
        now = datetime.now(timezone.utc)

        try:
            assert self._spotify is not None
            tracks_processed = 0
            new_tracks = 0

            async for raw_track in self._spotify.iter_playlist_tracks(playlist_id):
                track_doc = _parse_track(raw_track)
                if track_doc is None:
                    continue

                inserted = await self._upsert_track(track_doc)
                tracks_processed += 1
                if inserted:
                    new_tracks += 1

                # Enqueue artists for expansion
                for artist in track_doc.artists:
                    await self._enqueue_artist(artist.spotify_id, artist.name)

            logger.info(
                "playlist_processed",
                playlist_id=playlist_id,
                tracks_processed=tracks_processed,
                new_tracks=new_tracks,
            )

            # Mark playlist as done
            await col_pl.update_one(
                {"spotify_id": playlist_id},
                {
                    "$set": {
                        "status": QueueStatus.DONE.value,
                        "tracks_total": tracks_processed,
                        "updated_at": now,
                        "locked_at": None,
                        "locked_by": None,
                    }
                },
            )
            await self.increment_stat("total_discovered", new_tracks)
            await self.increment_stat("total_base_collected", new_tracks)

        except Exception as exc:
            logger.error(
                "playlist_processing_failed",
                playlist_id=playlist_id,
                error=str(exc),
                exc_info=True,
            )
            retry_count = item.get("retry_count", 0) + 1
            new_status = (
                QueueStatus.FAILED.value
                if retry_count >= self.settings.worker_retry_limit
                else QueueStatus.PENDING.value
            )
            await col_pl.update_one(
                {"spotify_id": playlist_id},
                {
                    "$set": {
                        "status": new_status,
                        "retry_count": retry_count,
                        "updated_at": now,
                        "locked_at": None,
                        "locked_by": None,
                    }
                },
            )

    async def _upsert_track(self, track: TrackDocument) -> bool:
        """
        Insert a track if it doesn't exist; increment appearance_score if it does.

        Returns ``True`` if the document was newly inserted.
        """
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        doc = track.to_mongo()

        # Build the duplicate-detection filter
        if track.isrc:
            filter_query = {"isrc": track.isrc}
        else:
            filter_query = {"fingerprint": track.fingerprint}

        insert_doc = {
            k: v for k, v in doc.items()
            if k not in ("created_at", "updated_at", "appearance_score")
        }
        try:
            result = await col.update_one(
                filter_query,
                {
                    # On insert: set all fields except managed ones
                    "$setOnInsert": {**insert_doc, "created_at": now},
                    # On every hit: increment appearance_score + update timestamp
                    "$inc": {"appearance_score": 1},
                    "$set": {"updated_at": now},
                },
                upsert=True,
            )
            # Backfill markets on existing docs that were stored with empty markets
            markets_list = track.markets
            if markets_list:
                await col.update_one(
                    {**filter_query, "markets": {"$size": 0}},
                    {"$set": {"markets": markets_list, "markets_count": len(markets_list), "updated_at": now}},
                )
            return result.upserted_id is not None
        except Exception as exc:
            # Duplicate key on spotify_id (race between two workers)
            if "duplicate key" in str(exc).lower():
                # Still increment appearance_score
                await col.update_one(
                    {"spotify_id": track.spotify_id},
                    {
                        "$inc": {"appearance_score": 1},
                        "$set": {"updated_at": now},
                    },
                )
                return False
            logger.error("track_upsert_error", spotify_id=track.spotify_id, error=str(exc))
            return False

    async def _enqueue_artist(self, artist_id: str, name: str) -> None:
        """Add an artist to the artist_queue if not already there."""
        col = self.db[ARTIST_QUEUE_COL]
        try:
            await col.update_one(
                {"spotify_id": artist_id},
                {
                    "$setOnInsert": {
                        "spotify_id": artist_id,
                        "name": name,
                        "status": QueueStatus.PENDING.value,
                        "retry_count": 0,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
                upsert=True,
            )
        except Exception:
            pass  # Ignore duplicate key errors silently
