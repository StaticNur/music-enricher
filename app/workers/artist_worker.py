"""
Artist Expansion Worker.

Polls ``artist_queue`` for PENDING artists, fetches all their albums and
tracks from Spotify, then inserts missing tracks into ``tracks`` with
status=base_collected.

Also stores full artist metadata (genres, followers, popularity) in the
``artists`` collection for later use by the quality_worker.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import Settings
from app.db.collections import (
    ARTIST_QUEUE_COL,
    ARTISTS_COL,
    TRACKS_COL,
)
from app.models.artist import ArtistDocument
from app.models.queue import QueueStatus
from app.models.track import TrackDocument, TrackStatus, ArtistRef, AlbumRef
from app.services.spotify import SpotifyClient, SpotifyError
from app.utils.deduplication import compute_fingerprint, extract_isrc
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)


class ArtistWorker(BaseWorker):
    """
    Expands the track catalog by harvesting all albums and singles
    from each artist discovered during playlist mining.

    State transitions:
        artist_queue: pending → processing → done | failed
        tracks:       (new) → base_collected
        artists:      upserted with full metadata
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._spotify: Optional[SpotifyClient] = None

    async def on_startup(self) -> None:
        self._spotify = SpotifyClient(self.settings)
        logger.info("artist_worker_started")

    async def on_shutdown(self) -> None:
        if self._spotify:
            await self._spotify.aclose()

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        col = self.db[ARTIST_QUEUE_COL]
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
        # Process artists one at a time to avoid spamming the API
        for item in batch:
            if not self._spotify:
                break
            await self._process_artist(item)

    async def _process_artist(self, item: Dict[str, Any]) -> None:
        artist_id: str = item["spotify_id"]
        now = datetime.now(timezone.utc)
        col_ar = self.db[ARTIST_QUEUE_COL]

        try:
            assert self._spotify is not None

            # Fetch and store full artist metadata
            await self._upsert_artist_metadata(artist_id)

            new_tracks = 0
            # Iterate all albums (albums + singles)
            async for album in self._spotify.iter_artist_albums(artist_id):
                album_id = album.get("id")
                if not album_id:
                    continue

                album_ref = AlbumRef(
                    spotify_id=album_id,
                    name=album.get("name", ""),
                    release_date=album.get("release_date"),
                    album_type=album.get("album_type"),
                    total_tracks=album.get("total_tracks"),
                    images=album.get("images", []),
                )

                async for raw_track in self._spotify.iter_album_tracks(album_id):
                    track_doc = self._parse_album_track(raw_track, album_ref, artist_id)
                    if track_doc is None:
                        continue

                    inserted = await self._upsert_track(track_doc)
                    if inserted:
                        new_tracks += 1

            logger.info(
                "artist_expanded",
                artist_id=artist_id,
                new_tracks=new_tracks,
            )

            await col_ar.update_one(
                {"spotify_id": artist_id},
                {
                    "$set": {
                        "status": QueueStatus.DONE.value,
                        "updated_at": now,
                        "locked_at": None,
                    }
                },
            )
            await self.increment_stat("total_discovered", new_tracks)
            await self.increment_stat("total_base_collected", new_tracks)

        except Exception as exc:
            logger.error(
                "artist_expansion_failed",
                artist_id=artist_id,
                error=str(exc),
                exc_info=True,
            )
            retry_count = item.get("retry_count", 0) + 1
            new_status = (
                QueueStatus.FAILED.value
                if retry_count >= self.settings.worker_retry_limit
                else QueueStatus.PENDING.value
            )
            await col_ar.update_one(
                {"spotify_id": artist_id},
                {
                    "$set": {
                        "status": new_status,
                        "retry_count": retry_count,
                        "updated_at": now,
                        "locked_at": None,
                    }
                },
            )

    async def _upsert_artist_metadata(self, artist_id: str) -> None:
        """Fetch full artist metadata from Spotify and upsert into ``artists``."""
        assert self._spotify is not None
        artists = await self._spotify.get_artists([artist_id])
        if not artists or not artists[0]:
            return

        raw = artists[0]
        images = raw.get("images", [])
        image_url = images[0].get("url") if images else None

        artist_doc = ArtistDocument(
            spotify_id=raw["id"],
            name=raw.get("name", ""),
            genres=raw.get("genres", []),
            followers=raw.get("followers", {}).get("total", 0),
            popularity=raw.get("popularity", 0),
            image_url=image_url,
        )

        now = datetime.now(timezone.utc)
        artist_data = {
            k: v for k, v in artist_doc.to_mongo().items()
            if k not in ("created_at", "updated_at")
        }
        await self.db[ARTISTS_COL].update_one(
            {"spotify_id": artist_id},
            {
                "$set": {**artist_data, "updated_at": now},
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    def _parse_album_track(
        self,
        raw: dict,
        album_ref: AlbumRef,
        artist_id: str,
    ) -> Optional[TrackDocument]:
        """
        Parse a simplified album track response into a ``TrackDocument``.

        Album track responses lack some fields (popularity, explicit may vary),
        so we default them and let the audio_features_worker enrich later.
        """
        if not raw or not raw.get("id"):
            return None
        name = raw.get("name") or ""
        if not name:
            return None

        artists = [
            ArtistRef(spotify_id=a["id"], name=a.get("name") or "")
            for a in raw.get("artists", [])
            if a.get("id")
        ]
        if not artists:
            artists = [ArtistRef(spotify_id=artist_id, name="")]

        isrc = extract_isrc(raw)
        duration_ms = raw.get("duration_ms", 0)
        fp = compute_fingerprint(name, artists[0].spotify_id, duration_ms)

        return TrackDocument(
            spotify_id=raw["id"],
            isrc=isrc,
            fingerprint=fp,
            name=name,
            artists=artists,
            album=album_ref,
            popularity=raw.get("popularity", 0),
            duration_ms=duration_ms,
            explicit=raw.get("explicit", False),
            markets_count=len(raw.get("available_markets") or []),
            status=TrackStatus.BASE_COLLECTED,
        )

    async def _upsert_track(self, track: TrackDocument) -> bool:
        """Insert track if new, otherwise skip. Returns True if inserted."""
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        doc = track.to_mongo()

        filter_query = (
            {"isrc": track.isrc} if track.isrc else {"fingerprint": track.fingerprint}
        )

        insert_doc = {k: v for k, v in doc.items() if k not in ("created_at", "updated_at")}
        try:
            result = await col.update_one(
                filter_query,
                {
                    "$setOnInsert": {**insert_doc, "created_at": now},
                    "$set": {"updated_at": now},
                },
                upsert=True,
            )
            return result.upserted_id is not None
        except Exception as exc:
            if "duplicate key" in str(exc).lower():
                return False
            logger.warning(
                "track_upsert_error", spotify_id=track.spotify_id, error=str(exc)
            )
            return False
