"""
NetEase Cloud Music Discovery Worker (v9).

Discovers Chinese music from NetEase Cloud Music and inserts directly
into ``tracks`` collection as ``base_collected``.

NetEase has 800M+ songs — primarily Chinese-language content not
well covered by Deezer, iTunes, or Spotify.

Queue: netease_seed_queue
  Item types:
  - {"item_type": "playlist", "item_id": str(netease_id), "netease_id": int}
  - {"item_type": "search", "item_id": query, "query": str, "offset": int}
  - {"item_type": "artist", "item_id": str(netease_id), "netease_id": int}

Bootstrap: 13 chart playlists + search queries (idempotent).
Cycle: reset all processed=False when exhausted.
Artist BFS: when processing artist, enqueue related artists.

Scale: 1 replica only (pyncm uses global session state, conservative rate).
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
from app.db.collections import NETEASE_SEED_QUEUE_COL, TRACKS_COL
from app.models.track import ArtistRef, TrackStatus
from app.services.netease import NeteaseMusicClient
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID, LOCK_TIMEOUT_SECONDS

logger = structlog.get_logger(__name__)

# Chinese music genre/style search queries for NetEase discovery.
_CN_SEARCH_QUERIES: List[str] = [
    "华语流行", "国产说唱", "中国说唱", "古风音乐", "电音",
    "国风", "民谣", "摇滚", "粤语", "台湾流行", "东南亚华语",
    "抖音热歌", "网红歌曲", "ACG", "游戏音乐",
]


def _netease_fingerprint(title: str, artist: str, duration_ms: int) -> str:
    """
    Fingerprint for NetEase tracks (no ISRC available).

    Strips CJK-aware bracketed suffixes, lowercases, buckets duration to ±2s.
    """
    norm_title = re.sub(
        r"\s*[\(\[（【](.*?)[\)\]）】]", "", title, flags=re.IGNORECASE
    ).lower().strip()
    norm_artist = artist.lower().strip()
    bucket = round(duration_ms / 2000) * 2000
    raw = f"{norm_title}|{norm_artist}|{bucket}|netease"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class NeteaseWorker(BaseWorker):
    """
    Discovers tracks from NetEase Cloud Music and inserts them directly
    into the pipeline.

    Queue: ``netease_seed_queue`` (playlist, search, and artist items)
    Output: ``tracks`` (status=base_collected)
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._client: Optional[NeteaseMusicClient] = None

    async def on_startup(self) -> None:
        self._client = NeteaseMusicClient(self.settings)
        await self._bootstrap_queue()
        logger.info("netease_worker_started")

    async def on_shutdown(self) -> None:
        if self._client:
            await self._client.aclose()

    # ── Seeding ───────────────────────────────────────────────────────────────

    async def _bootstrap_queue(self) -> None:
        """
        Seed netease_seed_queue on first run (idempotent via $setOnInsert).

        On subsequent runs (all processed), resets items to processed=False.
        """
        col = self.db[NETEASE_SEED_QUEUE_COL]

        unprocessed = await col.count_documents({"processed": False})
        if unprocessed > 0:
            logger.info("netease_queue_has_pending", count=unprocessed)
            return

        total = await col.count_documents({})
        if total > 0:
            # All done — reset for next cycle
            await col.update_many({}, {"$set": {"processed": False, "locked_at": None}})
            reset_count = await col.count_documents({"processed": False})
            logger.info("netease_queue_reset_for_next_cycle", count=reset_count)
            return

        # First run: seed chart playlists + search queries
        await self._seed_playlists()
        await self._seed_search_queries()

    async def _seed_playlists(self) -> None:
        """Insert chart playlist items into the queue."""
        assert self._client is not None
        playlists = self._client.get_chart_playlists()

        col = self.db[NETEASE_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        ops: List[Any] = []

        for pl in playlists:
            netease_id = pl["id"]
            item_id = str(netease_id)
            ops.append(UpdateOne(
                {"item_type": "playlist", "item_id": item_id},
                {"$setOnInsert": {
                    "item_type": "playlist",
                    "item_id": item_id,
                    "netease_id": netease_id,
                    "name": pl["name"],
                    "processed": False,
                    "locked_at": None,
                    "locked_by": None,
                    "created_at": now,
                    "updated_at": now,
                }},
                upsert=True,
            ))

        if not ops:
            return

        try:
            result = await col.bulk_write(ops, ordered=False)
            logger.info("netease_playlists_seeded", seeded=result.upserted_count, total=len(ops))
        except BulkWriteError as bwe:
            logger.info("netease_playlists_seeded", seeded=bwe.details.get("nUpserted", 0))

    async def _seed_search_queries(self) -> None:
        """Insert search query items into the queue."""
        col = self.db[NETEASE_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        ops: List[Any] = []

        for query in _CN_SEARCH_QUERIES:
            ops.append(UpdateOne(
                {"item_type": "search", "item_id": query},
                {"$setOnInsert": {
                    "item_type": "search",
                    "item_id": query,
                    "query": query,
                    "offset": 0,
                    "processed": False,
                    "locked_at": None,
                    "locked_by": None,
                    "created_at": now,
                    "updated_at": now,
                }},
                upsert=True,
            ))

        if not ops:
            return

        try:
            result = await col.bulk_write(ops, ordered=False)
            logger.info("netease_queries_seeded", seeded=result.upserted_count, total=len(ops))
        except BulkWriteError as bwe:
            logger.info("netease_queries_seeded", seeded=bwe.details.get("nUpserted", 0))

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        col = self.db[NETEASE_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        batch: List[Dict[str, Any]] = []

        for _ in range(self.settings.netease_batch_size):
            doc = await col.find_one_and_update(
                {
                    "processed": False,
                    "$or": [
                        {"locked_at": None},
                        {"locked_at": {"$lt": cutoff}},
                    ],
                },
                {"$set": {
                    "locked_at": now,
                    "locked_by": WORKER_INSTANCE_ID,
                    "updated_at": now,
                }},
                return_document=True,
            )
            if doc is None:
                break
            batch.append(doc)

        return batch

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            item_type = item.get("item_type", "")
            if item_type == "playlist":
                await self._process_playlist(item)
            elif item_type == "search":
                await self._process_search(item)
            elif item_type == "artist":
                await self._process_artist(item)
            else:
                await self._mark_processed(item)

    async def _process_playlist(self, item: Dict[str, Any]) -> None:
        assert self._client is not None
        col = self.db[NETEASE_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        netease_id: int = item["netease_id"]
        name: str = item.get("name", str(netease_id))

        try:
            tracks = await self._client.get_playlist_tracks(netease_id)
            inserted = 0
            for t in tracks:
                if await self._upsert_track(t):
                    inserted += 1

            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

            await self.increment_stat("netease_tracks_inserted", inserted)
            await self.increment_stat("total_discovered", inserted)
            await self.increment_stat("total_base_collected", inserted)

            logger.info(
                "netease_playlist_done",
                netease_id=netease_id,
                name=name,
                tracks_found=len(tracks),
                inserted=inserted,
            )

        except Exception as exc:
            logger.error(
                "netease_playlist_failed",
                netease_id=netease_id,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

    async def _process_search(self, item: Dict[str, Any]) -> None:
        assert self._client is not None
        col = self.db[NETEASE_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        query: str = item["query"]
        offset: int = item.get("offset", 0)
        page_size = 100

        try:
            tracks = await self._client.search_songs(query, limit=page_size, offset=offset)
            inserted = 0
            for t in tracks:
                if await self._upsert_track(t):
                    inserted += 1

            # Pagination: if full page returned, advance offset
            if len(tracks) == page_size:
                await col.update_one(
                    {"_id": item["_id"]},
                    {"$set": {
                        "offset": offset + page_size,
                        "processed": False,
                        "locked_at": None,
                        "updated_at": now,
                    }},
                )
            else:
                await col.update_one(
                    {"_id": item["_id"]},
                    {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
                )

            await self.increment_stat("netease_tracks_inserted", inserted)
            await self.increment_stat("total_discovered", inserted)
            await self.increment_stat("total_base_collected", inserted)

            logger.info(
                "netease_search_done",
                query=query,
                offset=offset,
                tracks_found=len(tracks),
                inserted=inserted,
            )

        except Exception as exc:
            logger.error(
                "netease_search_failed",
                query=query,
                offset=offset,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

    async def _process_artist(self, item: Dict[str, Any]) -> None:
        assert self._client is not None
        col = self.db[NETEASE_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        netease_id: int = item["netease_id"]

        try:
            tracks = await self._client.get_artist_top_songs(netease_id)
            inserted = 0
            for t in tracks:
                if await self._upsert_track(t):
                    inserted += 1

            # BFS: enqueue related artists
            related_enqueued = await self._enqueue_related_artists(netease_id)

            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

            await self.increment_stat("netease_tracks_inserted", inserted)
            await self.increment_stat("total_discovered", inserted)
            await self.increment_stat("total_base_collected", inserted)

            logger.info(
                "netease_artist_done",
                netease_id=netease_id,
                tracks_inserted=inserted,
                related_enqueued=related_enqueued,
            )

        except Exception as exc:
            logger.error(
                "netease_artist_failed",
                netease_id=netease_id,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

    async def _enqueue_related_artists(self, artist_id: int) -> int:
        """Fetch related artists and insert new ones into the queue (BFS)."""
        assert self._client is not None
        related = await self._client.get_related_artists(artist_id)
        if not related:
            return 0

        col = self.db[NETEASE_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        enqueued = 0

        for rel in related:
            rel_id = rel.get("id")
            if not rel_id:
                continue
            item_id = str(rel_id)
            try:
                result = await col.update_one(
                    {"item_type": "artist", "item_id": item_id},
                    {"$setOnInsert": {
                        "item_type": "artist",
                        "item_id": item_id,
                        "netease_id": rel_id,
                        "name": rel.get("name", ""),
                        "processed": False,
                        "locked_at": None,
                        "locked_by": None,
                        "created_at": now,
                        "updated_at": now,
                    }},
                    upsert=True,
                )
                if result.upserted_id is not None:
                    enqueued += 1
            except Exception:
                pass

        return enqueued

    async def _mark_processed(self, item: Dict[str, Any]) -> None:
        """Mark an item as processed without doing any work."""
        col = self.db[NETEASE_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        await col.update_one(
            {"_id": item["_id"]},
            {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
        )

    # ── Track upsert ──────────────────────────────────────────────────────────

    async def _upsert_track(self, track: Dict[str, Any]) -> bool:
        """
        Upsert a NetEase track into the main tracks collection.

        Deduplication: fingerprint only (no ISRC from NetEase).
        ``spotify_id`` is set to ``"netease:{id}"`` as placeholder.
        Returns True if a new document was inserted.
        """
        song_id = track.get("id")
        name: str = track.get("name", "").strip()
        artist_name: str = track.get("artist_name", "").strip()
        duration_ms: int = track.get("duration_ms", 0)

        if not song_id or not name or not artist_name:
            return False

        placeholder_id = f"netease:{song_id}"
        fp = _netease_fingerprint(name, artist_name, duration_ms)

        artist_ref = ArtistRef(
            spotify_id=f"netease_artist:{song_id}",
            name=artist_name,
        )

        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        insert_doc: Dict[str, Any] = {
            "spotify_id": placeholder_id,
            "fingerprint": fp,
            "name": name,
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
            "mb_priority": 1,  # East Asia boost
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
            if result.upserted_id is not None:
                return True
            # fingerprint matched an existing doc — sample log for diagnosis
            existing = await col.find_one({"fingerprint": fp}, {"spotify_id": 1, "name": 1})
            logger.info(
                "netease_track_fp_collision",
                song_id=song_id,
                name=name,
                fingerprint=fp[:16],
                existing_spotify_id=existing.get("spotify_id") if existing else "NOT_FOUND",
            )
            return False

        except Exception as exc:
            logger.error(
                "netease_track_upsert_error",
                song_id=song_id,
                name=name,
                fingerprint=fp[:16],
                error=str(exc),
                exc_info=True,
            )
            return False
