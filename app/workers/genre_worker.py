"""
Genre Expansion Worker.

Polls ``genre_queue`` for PENDING genres, searches Spotify for tracks
matching each genre, and inserts new tracks into ``tracks``.

Pagination is persisted in the queue document (``offset`` field), so
if a worker restarts mid-genre it continues from where it left off
rather than re-scanning from offset 0.

The genre search uses the Spotify search API with ``genre:`` operator
which may be partially deprecated. Fallback: plain keyword search.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import Settings
from app.db.collections import GENRE_QUEUE_COL, TRACKS_COL, ARTIST_QUEUE_COL
from app.models.queue import QueueStatus
from app.models.track import TrackDocument, TrackStatus, ArtistRef, AlbumRef
from app.services.spotify import SpotifyClient, SpotifyError
from app.utils.deduplication import compute_fingerprint, extract_isrc
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)


class GenreWorker(BaseWorker):
    """
    Searches Spotify by genre and inserts discovered tracks.

    Uses pagination persistence in the queue document so a crashed worker
    resumes exactly where it left off.

    State transitions:
        genre_queue: pending → processing → (offset += limit) → done | failed
        tracks:      (new) → base_collected
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._spotify: Optional[SpotifyClient] = None

    async def on_startup(self) -> None:
        self._spotify = SpotifyClient(self.settings)
        logger.info("genre_worker_started")

    async def on_shutdown(self) -> None:
        if self._spotify:
            await self._spotify.aclose()

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Claim one genre at a time (genre pagination is handled internally).

        We claim a single genre per iteration rather than a batch because
        each genre may require many API calls (pagination), and we want
        to release the lock and update the offset after each page.
        """
        col = self.db[GENRE_QUEUE_COL]
        now = datetime.now(timezone.utc)

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
        return [doc] if doc else []

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            await self._process_genre(item)

    async def _process_genre(self, item: Dict[str, Any]) -> None:
        genre: str = item["genre"]
        current_offset: int = item.get("offset", 0)
        max_offset = self.settings.genre_max_offset
        limit = self.settings.genre_batch_limit
        col = self.db[GENRE_QUEUE_COL]
        now = datetime.now(timezone.utc)

        try:
            assert self._spotify is not None
            new_tracks_total = 0
            offset = current_offset

            while offset <= max_offset:
                tracks_raw = await self._search_genre_tracks(genre, limit, offset)

                if not tracks_raw:
                    # End of results
                    break

                new_in_page = 0
                for raw_track in tracks_raw:
                    track_doc = self._parse_track(raw_track)
                    if track_doc is None:
                        continue
                    inserted = await self._upsert_track(track_doc)
                    if inserted:
                        new_in_page += 1
                        # Enqueue artists
                        for artist in track_doc.artists:
                            await self._enqueue_artist(artist.spotify_id, artist.name)

                new_tracks_total += new_in_page
                offset += limit

                # Persist offset progress after each page so we can resume
                await col.update_one(
                    {"genre": genre},
                    {
                        "$set": {
                            "offset": offset,
                            "updated_at": datetime.now(timezone.utc),
                        }
                    },
                )

                logger.debug(
                    "genre_page_processed",
                    genre=genre,
                    offset=offset,
                    new_tracks=new_in_page,
                )

                if len(tracks_raw) < limit:
                    # Fewer results than requested → last page
                    break

            logger.info(
                "genre_completed",
                genre=genre,
                new_tracks=new_tracks_total,
                final_offset=offset,
            )

            await col.update_one(
                {"genre": genre},
                {
                    "$set": {
                        "status": QueueStatus.DONE.value,
                        "updated_at": now,
                        "locked_at": None,
                    }
                },
            )
            await self.increment_stat("total_discovered", new_tracks_total)
            await self.increment_stat("total_base_collected", new_tracks_total)

        except Exception as exc:
            logger.error(
                "genre_processing_failed",
                genre=genre,
                error=str(exc),
                exc_info=True,
            )
            retry_count = item.get("retry_count", 0) + 1
            new_status = (
                QueueStatus.FAILED.value
                if retry_count >= self.settings.worker_retry_limit
                else QueueStatus.PENDING.value
            )
            await col.update_one(
                {"genre": genre},
                {
                    "$set": {
                        "status": new_status,
                        "retry_count": retry_count,
                        "updated_at": now,
                        "locked_at": None,
                    }
                },
            )

    async def _search_genre_tracks(
        self, genre: str, limit: int, offset: int
    ) -> List[dict]:
        """
        Search Spotify for tracks in a genre.

        Tries ``genre:`` operator first; falls back to plain keyword
        if the operator returns no results (deprecated behavior).
        """
        assert self._spotify is not None

        try:
            resp = await self._spotify.search_tracks(
                f"genre:{genre}", limit=limit, offset=offset
            )
            items = resp.get("tracks", {}).get("items", [])
            if items:
                return items
        except Exception:
            pass

        # Fallback: plain genre name as keyword
        try:
            resp = await self._spotify.search_tracks(
                genre, limit=limit, offset=offset
            )
            return resp.get("tracks", {}).get("items", [])
        except Exception as exc:
            logger.warning("genre_search_failed", genre=genre, error=str(exc))
            return []

    def _parse_track(self, raw: dict) -> Optional[TrackDocument]:
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
            return None

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
        duration_ms = raw.get("duration_ms", 0)
        fp = compute_fingerprint(name, artists[0].spotify_id, duration_ms)

        return TrackDocument(
            spotify_id=raw["id"],
            isrc=isrc,
            fingerprint=fp,
            name=name,
            artists=artists,
            album=album,
            popularity=raw.get("popularity", 0),
            duration_ms=duration_ms,
            explicit=raw.get("explicit", False),
            markets_count=len(raw.get("available_markets") or []),
            status=TrackStatus.BASE_COLLECTED,
        )

    async def _upsert_track(self, track: TrackDocument) -> bool:
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
            logger.warning("track_upsert_error", spotify_id=track.spotify_id, error=str(exc))
            return False

    async def _enqueue_artist(self, artist_id: str, name: str) -> None:
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
            pass
