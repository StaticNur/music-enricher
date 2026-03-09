"""
JioSaavn Discovery Worker (v9).

Discovers tracks from JioSaavn and inserts them directly into ``tracks``.
Covers Bollywood and 9 regional Indian languages — a major gap not
covered well by Deezer BFS or iTunes charts.

Queue: jiosaavn_seed_queue
  Two item types:
  - {"type": "chart", "item_id": chart_id, "chart_id": str}
  - {"type": "search", "item_id": query, "query": str, "page": int}

Bootstrap on startup: fetch JioSaavn charts → insert chart items.
Also seeds 30+ language-specific search queries.

Cycle: when all items processed, reset processed=False for next cycle.

Scale: 2 replicas safe (3 rps per instance, no strict IP limit).
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
from app.db.collections import JIOSAAVN_SEED_QUEUE_COL, TRACKS_COL
from app.models.track import ArtistRef, TrackStatus
from app.services.jiosaavn import JioSaavnClient
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID, LOCK_TIMEOUT_SECONDS

logger = structlog.get_logger(__name__)

# Language-specific and genre search queries for JioSaavn discovery.
_SEED_QUERIES: List[str] = [
    "bollywood hits", "new hindi songs", "punjabi hits", "top bollywood",
    "latest bollywood", "hindi romantic", "hindi rap", "desi hip hop",
    "punjabi rap", "punjabi pop", "latest punjabi", "bhangra hits",
    "tamil hits", "latest tamil songs", "kollywood", "tamil rap",
    "telugu hits", "tollywood", "latest telugu", "bengali songs",
    "rabindra sangeet", "bangla band", "kannada hits", "malayalam songs",
    "marathi songs", "gujarati garba", "odia songs",
    "indian pop", "indian indie", "sufi music", "ghazal",
    "devotional hindi", "bhajan", "qawwali",
]


def _jiosaavn_fingerprint(title: str, artist: str, duration_ms: int) -> str:
    """
    Fingerprint for JioSaavn tracks (no ISRC available).

    Strips bracketed suffixes, lowercases, and buckets duration to ±2s.
    """
    norm_title = re.sub(r"\s*[\(\[](.*?)[\)\]]", "", title, flags=re.IGNORECASE).lower().strip()
    norm_artist = artist.lower().strip()
    bucket = round(duration_ms / 2000) * 2000
    raw = f"{norm_title}|{norm_artist}|{bucket}|jiosaavn"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class JioSaavnWorker(BaseWorker):
    """
    Discovers tracks from JioSaavn and inserts them directly into the pipeline.

    Queue: ``jiosaavn_seed_queue`` (chart + search items)
    Output: ``tracks`` (status=base_collected)
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._client: Optional[JioSaavnClient] = None

    async def on_startup(self) -> None:
        self._client = JioSaavnClient(self.settings)
        await self._bootstrap_queue()
        logger.info("jiosaavn_worker_started")

    async def on_shutdown(self) -> None:
        if self._client:
            await self._client.aclose()

    # ── Seeding ───────────────────────────────────────────────────────────────

    async def _bootstrap_queue(self) -> None:
        """
        Seed jiosaavn_seed_queue on first run (idempotent via $setOnInsert).

        On subsequent runs (all processed), resets items to processed=False.
        """
        col = self.db[JIOSAAVN_SEED_QUEUE_COL]

        unprocessed = await col.count_documents({"processed": False})
        if unprocessed > 0:
            logger.info("jiosaavn_queue_has_pending", count=unprocessed)
            return

        total = await col.count_documents({})
        if total > 0:
            # All done — reset for next cycle
            await col.update_many({}, {"$set": {"processed": False, "locked_at": None}})
            reset_count = await col.count_documents({"processed": False})
            logger.info("jiosaavn_queue_reset_for_next_cycle", count=reset_count)
            return

        # First run: seed charts + search queries
        await self._seed_charts()
        await self._seed_search_queries()

    async def _seed_charts(self) -> None:
        """Fetch JioSaavn chart playlists and insert them as queue items."""
        assert self._client is not None
        charts = await self._client.get_charts()
        if not charts:
            logger.warning("jiosaavn_no_charts_returned")
            return

        col = self.db[JIOSAAVN_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        ops: List[Any] = []

        for chart in charts:
            chart_id = chart["id"]
            ops.append(UpdateOne(
                {"item_type": "chart", "item_id": chart_id},
                {"$setOnInsert": {
                    "item_type": "chart",
                    "item_id": chart_id,
                    "chart_id": chart_id,
                    "chart_name": chart["title"],
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
            logger.info("jiosaavn_charts_seeded", seeded=result.upserted_count, total=len(ops))
        except BulkWriteError as bwe:
            logger.info("jiosaavn_charts_seeded", seeded=bwe.details.get("nUpserted", 0))

    async def _seed_search_queries(self) -> None:
        """Insert language/genre search queries into the queue."""
        col = self.db[JIOSAAVN_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        ops: List[Any] = []

        for query in _SEED_QUERIES:
            ops.append(UpdateOne(
                {"item_type": "search", "item_id": query},
                {"$setOnInsert": {
                    "item_type": "search",
                    "item_id": query,
                    "query": query,
                    "page": 1,
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
            logger.info("jiosaavn_queries_seeded", seeded=result.upserted_count, total=len(ops))
        except BulkWriteError as bwe:
            logger.info("jiosaavn_queries_seeded", seeded=bwe.details.get("nUpserted", 0))

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        col = self.db[JIOSAAVN_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        batch: List[Dict[str, Any]] = []

        for _ in range(self.settings.jiosaavn_batch_size):
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
            if item_type == "chart":
                await self._process_chart(item)
            elif item_type == "search":
                await self._process_search(item)
            else:
                # Unknown type — mark processed to avoid blocking queue
                await self._mark_processed(item)

    async def _process_chart(self, item: Dict[str, Any]) -> None:
        assert self._client is not None
        col = self.db[JIOSAAVN_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        chart_id: str = item["chart_id"]
        chart_name: str = item.get("chart_name", chart_id)

        try:
            songs = await self._client.get_playlist_songs(chart_id, n=100)
            inserted = 0
            for song in songs:
                if await self._upsert_track(song):
                    inserted += 1

            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

            await self.increment_stat("jiosaavn_tracks_inserted", inserted)
            await self.increment_stat("total_discovered", inserted)
            await self.increment_stat("total_base_collected", inserted)

            logger.info(
                "jiosaavn_chart_done",
                chart_id=chart_id,
                chart_name=chart_name,
                songs_found=len(songs),
                inserted=inserted,
            )

        except Exception as exc:
            logger.error(
                "jiosaavn_chart_failed",
                chart_id=chart_id,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

    async def _process_search(self, item: Dict[str, Any]) -> None:
        assert self._client is not None
        col = self.db[JIOSAAVN_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        query: str = item["query"]
        page: int = item.get("page", 1)
        max_pages: int = self.settings.jiosaavn_max_search_pages

        try:
            songs = await self._client.search_songs(query, page=page, n=50)
            inserted = 0
            for song in songs:
                if await self._upsert_track(song):
                    inserted += 1

            # Pagination: if full page returned and haven't exceeded max, advance page
            if len(songs) == 50 and page < max_pages:
                await col.update_one(
                    {"_id": item["_id"]},
                    {"$set": {
                        "page": page + 1,
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

            await self.increment_stat("jiosaavn_tracks_inserted", inserted)
            await self.increment_stat("total_discovered", inserted)
            await self.increment_stat("total_base_collected", inserted)

            logger.info(
                "jiosaavn_search_done",
                query=query,
                page=page,
                songs_found=len(songs),
                inserted=inserted,
            )

        except Exception as exc:
            logger.error(
                "jiosaavn_search_failed",
                query=query,
                page=page,
                error=str(exc),
                exc_info=True,
            )
            await col.update_one(
                {"_id": item["_id"]},
                {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
            )

    async def _mark_processed(self, item: Dict[str, Any]) -> None:
        """Mark an item as processed without doing any work (unknown type)."""
        col = self.db[JIOSAAVN_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)
        await col.update_one(
            {"_id": item["_id"]},
            {"$set": {"processed": True, "locked_at": None, "updated_at": now}},
        )

    # ── Track upsert ──────────────────────────────────────────────────────────

    async def _upsert_track(self, song: Dict[str, Any]) -> bool:
        """
        Upsert a JioSaavn track into the main tracks collection.

        Deduplication: fingerprint only (no ISRC from JioSaavn).
        ``spotify_id`` is set to ``"jiosaavn:{id}"`` as placeholder.
        Returns True if a new document was inserted.
        """
        song_id: str = song.get("id", "").strip()
        title: str = song.get("title", "").strip()
        artist: str = song.get("artist", "").strip()
        duration_ms: int = song.get("duration_ms", 0)
        language: str = song.get("language", "")

        if not song_id or not title or not artist:
            return False

        placeholder_id = f"jiosaavn:{song_id}"
        fp = _jiosaavn_fingerprint(title, artist, duration_ms)

        artist_ref = ArtistRef(
            spotify_id=f"jiosaavn_artist:{song_id}",
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
            "mb_priority": 1,  # South Asia boost
            "quality_score": 0.0,
            "regional_score": 0.0,
            "artist_followers": 0,
            "retry_count": 0,
            "error_log": [],
            "locked_at": None,
            "locked_by": None,
        }

        # Optionally store the language from JioSaavn if available
        if language:
            insert_doc["language"] = language

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
                "jiosaavn_track_upsert_error",
                song_id=song_id,
                title=title,
                error=str(exc),
            )
            return False
