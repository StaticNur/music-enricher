"""
Artist Graph Expansion Worker.

Implements a breadth-first artist graph expansion:

    seed artists (from existing tracks)
        → full discography (album + single + compilation + appears_on)
            → tracks → upsert into main pipeline
            → featured artists → enqueue (depth + 1)
        → related artists → enqueue (depth + 1, lower priority)

Each artist is processed exactly once (checked against ``artist_processed_cache``
before any API call).  Each album is processed exactly once (checked against
``album_processed_cache``).

Expected scale at depth=5:
    artists:   50k–100k
    albums:    500k–1M
    tracks:    10M–30M

Horizontally scalable: multiple replicas compete via ``locked_at``
optimistic locking on ``artist_graph_queue``.

Priority ordering: highest ``priority`` score is claimed first, so popular
artists (high Spotify popularity + followers) are processed before obscure ones.
"""
from __future__ import annotations

import asyncio
import math
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import DESCENDING, UpdateOne
from pymongo.errors import BulkWriteError
from tenacity import RetryError

from app.core.config import Settings
from app.db.collections import (
    TRACKS_COL,
    ARTISTS_COL,
    ARTIST_QUEUE_COL,
    ARTIST_GRAPH_QUEUE_COL,
    ARTIST_PROCESSED_CACHE_COL,
    ALBUM_PROCESSED_CACHE_COL,
)
from app.models.artist_graph import ArtistGraphItem, GraphSource, compute_artist_priority
from app.models.track import (
    TrackDocument,
    TrackStatus,
    ArtistRef,
    AlbumRef,
)
from app.services.spotify import SpotifyClient
from app.services.deezer import DeezerClient
from app.utils.circuit_breaker import CircuitBreakerOpen
from app.utils.deduplication import compute_fingerprint, extract_isrc
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID, LOCK_TIMEOUT_SECONDS
from app.workers.deezer_direct_worker import _deezer_fingerprint

try:
    from rapidfuzz import fuzz as _fuzz
except ImportError:
    _fuzz = None  # type: ignore[assignment]

logger = structlog.get_logger(__name__)


class ArtistGraphWorker(BaseWorker):
    """
    Expands the music catalog by traversing the artist collaboration graph.

    Queue: ``artist_graph_queue``
    Caches: ``artist_processed_cache``, ``album_processed_cache``
    Output: ``tracks`` (status=base_collected)
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._spotify: Optional[SpotifyClient] = None
        self._deezer: Optional[DeezerClient] = None

    async def on_startup(self) -> None:
        if not self.settings.spotify_enabled:
            self._deezer = DeezerClient(self.settings)
            await self._bootstrap_queue()
            await self._seed_regional_artists()
            logger.info("artist_graph_worker_started", mode="deezer")
            return
        self._spotify = SpotifyClient(self.settings)
        await self._bootstrap_queue()
        await self._seed_regional_artists()
        logger.info("artist_graph_worker_started", mode="spotify")

    async def on_shutdown(self) -> None:
        if self._spotify:
            await self._spotify.aclose()
        if self._deezer:
            await self._deezer.aclose()

    # ── Seeding ───────────────────────────────────────────────────────────────

    async def _seed_regional_artists(self) -> None:
        """
        Inject artists from CIS/Central Asia/MENA tracks into the graph queue.

        Runs on EVERY startup — not gated on queue emptiness — so regional
        artists discovered by regional_seed_worker, deezer_direct_worker, or
        itunes_worker always enter the BFS even after the initial Western-biased
        bootstrap from top-500 artists.

        Only real Spotify IDs are enqueued (deezer:/itunes: placeholders skipped).
        Already-processed artists are skipped via artist_processed_cache check
        inside _maybe_enqueue_artist, so this method is fully idempotent.
        """
        col = self.db[TRACKS_COL]

        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"regions.cis": True},
                        {"regions.central_asia": True},
                        {"regions.mena": True},
                    ]
                }
            },
            {"$unwind": "$artists"},
            {
                "$group": {
                    "_id": "$artists.spotify_id",
                    "name": {"$first": "$artists.name"},
                    "track_count": {"$sum": 1},
                    "popularity": {"$max": "$popularity"},
                }
            },
            {"$match": {"_id": {"$ne": None}}},
            {"$sort": {"track_count": -1}},
            {"$limit": 5000},
        ]

        enqueued = 0
        async for doc in col.aggregate(pipeline):
            artist_id: str = doc["_id"] or ""
            # Skip placeholder IDs from Deezer/iTunes workers
            if not artist_id or ":" in artist_id:
                continue
            priority = compute_artist_priority(doc.get("popularity") or 0, 0)
            was_new = await self._maybe_enqueue_artist(
                artist_id,
                doc.get("name") or "",
                depth=0,
                priority=max(priority, 0.3),  # floor so regional artists aren't buried
            )
            if was_new:
                enqueued += 1

        if enqueued > 0:
            logger.info("artist_graph_regional_artists_seeded", count=enqueued)

    async def _bootstrap_queue(self) -> None:
        """
        Seed ``artist_graph_queue`` on first run (idempotent via ``$setOnInsert``).

        Strategy (in order):
        1. Aggregate top-N artists from ``tracks`` by track-count.
        2. If that returns nothing (pipeline just started, tracks collection
           empty), fall back to ``artist_queue`` — the v1 pipeline populates
           it with artists discovered through Spotify playlists and already
           has thousands of entries within minutes of first run.

        Only runs when ``artist_graph_queue`` is empty.
        """
        col = self.db[ARTIST_GRAPH_QUEUE_COL]
        now = datetime.now(timezone.utc)

        unprocessed = await col.count_documents({"processed": False})
        if unprocessed > 0:
            logger.info("artist_graph_queue_has_unprocessed", count=unprocessed)
            return

        total = await col.count_documents({})
        if total > 0:
            # BFS is complete — all discovered artists have been processed.
            # No automatic re-seed: the graph is fully traversed up to
            # ARTIST_GRAPH_MAX_DEPTH. To restart, clear artist_graph_queue
            # and artist_processed_cache manually.
            logger.info("artist_graph_bfs_complete", total_artists_processed=total)
            return
        ops: List[Any] = []

        # ── Strategy 1: aggregate from tracks ────────────────────────────────
        pipeline = [
            {"$unwind": "$artists"},
            {
                "$group": {
                    "_id": "$artists.spotify_id",
                    "name": {"$first": "$artists.name"},
                    "track_count": {"$sum": 1},
                    "popularity": {"$max": "$popularity"},
                    "followers": {"$max": "$artist_followers"},
                }
            },
            {"$match": {"_id": {"$ne": None}}},
            {"$sort": {"track_count": -1}},
            {"$limit": self.settings.artist_graph_seed_limit},
        ]

        async for doc in self.db[TRACKS_COL].aggregate(pipeline):
            artist_id = doc["_id"]
            if not artist_id:
                continue
            popularity = doc.get("popularity") or 0
            followers = doc.get("followers") or 0
            ops.append(self._build_seed_op(
                artist_id, doc.get("name") or "",
                popularity, followers, now,
            ))

        # ── Strategy 2: fall back to artist_queue (v1 pipeline) ──────────────
        if not ops:
            logger.info(
                "artist_graph_seeding_from_artist_queue",
                reason="tracks aggregation returned no results",
            )
            cursor = self.db[ARTIST_QUEUE_COL].find(
                {},
                {"spotify_id": 1, "name": 1},
                limit=self.settings.artist_graph_seed_limit,
            )
            async for doc in cursor:
                artist_id = doc.get("spotify_id")
                if not artist_id:
                    continue
                ops.append(self._build_seed_op(
                    artist_id, doc.get("name") or "",
                    0, 0, now,
                ))

        if not ops:
            logger.warning(
                "artist_graph_no_seed_artists_found",
                message="Neither tracks nor artist_queue contain artists yet. "
                        "The worker will retry seeding on the next restart once "
                        "other workers have populated data.",
            )
            return

        try:
            result = await self.db[ARTIST_GRAPH_QUEUE_COL].bulk_write(
                ops, ordered=False
            )
            logger.info(
                "artist_graph_queue_bootstrapped",
                seeded=result.upserted_count,
                total_candidates=len(ops),
            )
        except BulkWriteError as bwe:
            logger.info(
                "artist_graph_queue_bootstrapped",
                seeded=bwe.details.get("nUpserted", 0),
            )

    @staticmethod
    def _build_seed_op(
        artist_id: str,
        name: str,
        popularity: int,
        followers: int,
        now: datetime,
    ) -> UpdateOne:
        """Build a bulk-write op for seeding one artist into the graph queue."""
        priority = compute_artist_priority(popularity, followers)
        item = ArtistGraphItem(
            artist_id=artist_id,
            name=name,
            source=GraphSource.SEED,
            depth=0,
            priority=priority,
            followers=followers or None,
            popularity=popularity or None,
        )
        mongo_doc = item.to_mongo()
        mongo_doc["created_at"] = now
        mongo_doc["updated_at"] = now
        return UpdateOne(
            {"artist_id": artist_id},
            {"$setOnInsert": mongo_doc},
            upsert=True,
        )

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Claim up to ``artist_graph_batch_size`` artists, highest priority first.

        Uses ``locked_at`` for stale-lock recovery (same pattern as
        ``candidate_match_worker``, without a ``status`` enum).
        """
        if self._spotify and self._spotify.is_circuit_open:
            logger.warning("artist_graph_worker_idle_circuit_open", reason="Spotify circuit breaker is OPEN — skipping claim")
            return []
        col = self.db[ARTIST_GRAPH_QUEUE_COL]
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        batch = []

        batch_size = max(1, self.settings.artist_graph_batch_size)

        for _ in range(batch_size):
            doc = await col.find_one_and_update(
                {
                    "processed": False,
                    "$or": [
                        {"locked_at": None},
                        {"locked_at": {"$lt": cutoff}},
                    ],
                },
                {
                    "$set": {
                        "locked_at": now,
                        "locked_by": WORKER_INSTANCE_ID,
                        "updated_at": now,
                    }
                },
                sort=[("priority", DESCENDING)],
                return_document=True,
            )
            if doc is None:
                break
            batch.append(doc)

        return batch

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            await self._process_artist(item)

    async def _process_artist(self, item: Dict[str, Any]) -> None:
        if not self.settings.spotify_enabled:
            await self._process_artist_deezer(item)
            return

        artist_id: str = item["artist_id"]
        depth: int = item.get("depth", 0)
        col = self.db[ARTIST_GRAPH_QUEUE_COL]
        cache_col = self.db[ARTIST_PROCESSED_CACHE_COL]
        now = datetime.now(timezone.utc)

        try:
            assert self._spotify is not None

            # Skip if already processed by another replica
            if await cache_col.find_one({"artist_id": artist_id}, {"_id": 1}):
                await self._mark_processed(col, artist_id, now)
                return

            # Fetch artist metadata (followers, popularity, name)
            artist_data = await self._spotify.get_artist(artist_id)
            if artist_data is None:
                # Artist not found or API error — mark done so we don't retry forever
                await self._mark_processed(col, artist_id, now)
                logger.debug("artist_graph_artist_not_found", artist_id=artist_id)
                return

            followers = (artist_data.get("followers") or {}).get("total") or 0
            popularity = artist_data.get("popularity") or 0
            artist_name = artist_data.get("name") or ""

            # Cache artist metadata so quality_worker can look up followers
            await self.db[ARTISTS_COL].update_one(
                {"spotify_id": artist_id},
                {
                    "$set": {
                        "name": artist_name,
                        "followers": followers,
                        "popularity": popularity,
                        "updated_at": now,
                    },
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )

            # Harvest full discography and insert tracks
            tracks_discovered, albums_processed = await self._harvest_discography(
                artist_id, artist_data, depth
            )

            # Enqueue related artists at next depth level
            new_artists_enqueued = 0
            if depth < self.settings.artist_graph_max_depth:
                new_artists_enqueued = await self._enqueue_related_artists(
                    artist_id, depth, followers, popularity
                )

            # Mark artist as done in the processed cache
            await cache_col.update_one(
                {"artist_id": artist_id},
                {
                    "$setOnInsert": {
                        "artist_id": artist_id,
                        "processed_at": now,
                    }
                },
                upsert=True,
            )
            await self._mark_processed(col, artist_id, now)

            await self.increment_stat("artists_processed", 1)
            await self.increment_stat("tracks_discovered", tracks_discovered)
            await self.increment_stat("albums_processed", albums_processed)
            await self.increment_stat("new_artists_enqueued", new_artists_enqueued)

            logger.info(
                "artist_graph_artist_done",
                artist_id=artist_id,
                artist_name=artist_name,
                depth=depth,
                tracks_discovered=tracks_discovered,
                albums_processed=albums_processed,
                new_artists_enqueued=new_artists_enqueued,
            )

        except (CircuitBreakerOpen, RetryError) as exc:
            # Transient Spotify API unavailability (429 / circuit open).
            # Release the lock so the item can be retried later — no retry_count
            # penalty, because the artist itself is not the problem.
            logger.warning(
                "artist_graph_api_unavailable",
                artist_id=artist_id,
                depth=depth,
                error=str(exc),
            )
            await col.update_one(
                {"artist_id": artist_id},
                {"$set": {"locked_at": None, "locked_by": None, "updated_at": now}},
            )
            await asyncio.sleep(30)

        except Exception as exc:
            logger.error(
                "artist_graph_artist_failed",
                artist_id=artist_id,
                depth=depth,
                error=str(exc),
                exc_info=True,
            )
            retry_count = item.get("retry_count", 0) + 1
            processed = retry_count >= self.settings.worker_retry_limit
            await col.update_one(
                {"artist_id": artist_id},
                {
                    "$set": {
                        "processed": processed,
                        "retry_count": retry_count,
                        "locked_at": None,
                        "locked_by": None,
                        "updated_at": now,
                    }
                },
            )

    # ── Discography harvesting ────────────────────────────────────────────────

    async def _harvest_discography(
        self,
        artist_id: str,
        artist_data: Dict[str, Any],
        depth: int,
    ) -> tuple[int, int]:
        """
        Iterate all releases for ``artist_id`` and upsert tracks.

        Own catalog (album + single + compilation) and ``appears_on`` are
        fetched via separate API calls so each can be capped independently.
        ``ARTIST_GRAPH_MAX_APPEARS_ON`` (default 50) prevents stalling on
        popular artists that have 500–2000 compilation appearances.

        Returns ``(tracks_inserted, albums_processed)``.
        """
        assert self._spotify is not None
        album_cache_col = self.db[ALBUM_PROCESSED_CACHE_COL]
        now = datetime.now(timezone.utc)

        total_tracks = 0
        total_albums = 0

        max_own = self.settings.artist_graph_max_own_albums        # 0 = unlimited
        max_appears_on = self.settings.artist_graph_max_appears_on

        # ── Own catalog ───────────────────────────────────────────────────────
        own_count = 0
        async for album in self._spotify.iter_artist_albums(
            artist_id, include_groups="album,single,compilation"
        ):
            if max_own > 0 and own_count >= max_own:
                break
            album_id = album.get("id")
            if not album_id:
                continue
            own_count += 1
            t, a = await self._process_album(album, artist_id, depth, album_cache_col, now)
            total_tracks += t
            total_albums += a

        # ── Compilation appearances — capped to avoid stalling ────────────────
        appears_on_count = 0
        async for album in self._spotify.iter_artist_albums(
            artist_id, include_groups="appears_on"
        ):
            if appears_on_count >= max_appears_on:
                break
            album_id = album.get("id")
            if not album_id:
                continue
            appears_on_count += 1
            t, a = await self._process_album(album, artist_id, depth, album_cache_col, now)
            total_tracks += t
            total_albums += a

        if appears_on_count >= max_appears_on:
            logger.debug(
                "artist_graph_appears_on_capped",
                artist_id=artist_id,
                limit=max_appears_on,
            )

        return total_tracks, total_albums

    async def _process_album(
        self,
        album: Dict[str, Any],
        artist_id: str,
        depth: int,
        album_cache_col: Any,
        now: datetime,
    ) -> tuple[int, int]:
        """
        Process a single album: fetch tracks, upsert, enqueue featured artists.

        Returns ``(tracks_inserted, albums_processed)`` where
        ``albums_processed`` is 1 on success or 0 if skipped/empty.
        """
        assert self._spotify is not None
        album_id: str = album.get("id") or ""

        # Skip albums already processed by any worker instance
        if await album_cache_col.find_one({"album_id": album_id}, {"_id": 1}):
            return 0, 0

        # Collect simplified track IDs from this album
        track_ids: List[str] = []
        async for simplified in self._spotify.iter_album_tracks(album_id):
            tid = simplified.get("id")
            if tid:
                track_ids.append(tid)

        if not track_ids:
            await self._cache_album(album_id, now)
            return 0, 1

        # Batch-fetch full track objects to get ISRC from external_ids
        full_tracks = await self._spotify.get_tracks(track_ids)

        inserted_this_album = 0
        for track in full_tracks:
            if not track or not track.get("id"):
                continue

            inserted = await self._upsert_track(track, album)
            if inserted:
                inserted_this_album += 1

            # Enqueue all non-primary artists as graph candidates
            if depth < self.settings.artist_graph_max_depth:
                for art in track.get("artists") or []:
                    art_id = art.get("id")
                    if art_id and art_id != artist_id:
                        await self._maybe_enqueue_artist(
                            art_id,
                            art.get("name") or "",
                            depth=depth + 1,
                            priority=0.1,  # no follower data at this point
                        )

        await self._cache_album(album_id, now)

        logger.debug(
            "artist_graph_album_done",
            album_id=album_id,
            album_name=album.get("name"),
            tracks_inserted=inserted_this_album,
        )

        return inserted_this_album, 1

    # ── Track upsert (mirrors candidate_match_worker pattern) ────────────────

    async def _upsert_track(
        self,
        raw: Dict[str, Any],
        album: Dict[str, Any],
    ) -> bool:
        """
        Insert track if new; increment appearance_score if duplicate.

        ``version_album_ids`` is updated via ``$addToSet`` so each unique
        album ID is recorded even for pre-existing tracks.

        Returns ``True`` if the document was newly inserted.
        """
        spotify_id: str = raw.get("id") or ""
        name: str = (raw.get("name") or "").strip()
        if not spotify_id or not name:
            return False

        artists = [
            ArtistRef(spotify_id=a["id"], name=a.get("name") or "")
            for a in (raw.get("artists") or [])
            if a.get("id")
        ]
        if not artists:
            return False

        album_id: Optional[str] = album.get("id")
        album_ref: Optional[AlbumRef] = None
        if album_id:
            album_ref = AlbumRef(
                spotify_id=album_id,
                name=album.get("name") or "",
                release_date=album.get("release_date"),
                album_type=album.get("album_type"),
                total_tracks=album.get("total_tracks"),
                images=album.get("images") or [],
            )

        isrc = extract_isrc(raw)
        duration_ms: int = raw.get("duration_ms") or 0
        fp = compute_fingerprint(name, artists[0].spotify_id, duration_ms)
        markets_list = raw.get("available_markets") or []

        track_doc = TrackDocument(
            spotify_id=spotify_id,
            isrc=isrc,
            fingerprint=fp,
            name=name,
            artists=artists,
            album=album_ref,
            popularity=raw.get("popularity") or 0,
            duration_ms=duration_ms,
            explicit=raw.get("explicit") or False,
            markets_count=len(markets_list),
            markets=markets_list,
            status=TrackStatus.BASE_COLLECTED,
            appearance_score=1,
            version_album_ids=[album_id] if album_id else [],
        )

        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        doc = track_doc.to_mongo()
        insert_doc = {
            k: v
            for k, v in doc.items()
            if k not in ("created_at", "updated_at", "appearance_score", "version_album_ids")
        }

        filter_q = {"isrc": isrc} if isrc else {"fingerprint": fp}

        update_ops: Dict[str, Any] = {
            "$setOnInsert": {**insert_doc, "created_at": now},
            "$inc": {"appearance_score": 1},
            "$set": {"updated_at": now},
        }
        if album_id:
            update_ops["$addToSet"] = {"version_album_ids": album_id}

        try:
            result = await col.update_one(filter_q, update_ops, upsert=True)
            # Backfill markets on existing docs that were stored with empty markets
            if markets_list:
                await col.update_one(
                    {**filter_q, "markets": {"$size": 0}},
                    {"$set": {"markets": markets_list, "markets_count": len(markets_list), "updated_at": now}},
                )
            # Backfill real spotify_id if existing doc has a deezer: placeholder
            if result.upserted_id is None:
                await col.update_one(
                    {**filter_q, "spotify_id": {"$regex": "^deezer:"}},
                    {"$set": {"spotify_id": spotify_id, "updated_at": now}},
                )
            return result.upserted_id is not None
        except Exception as exc:
            if "duplicate key" in str(exc).lower():
                # Race between replicas — still bump counters
                race_ops: Dict[str, Any] = {
                    "$inc": {"appearance_score": 1},
                    "$set": {"updated_at": now},
                }
                if album_id:
                    race_ops["$addToSet"] = {"version_album_ids": album_id}
                await col.update_one({"spotify_id": spotify_id}, race_ops)
                return False
            logger.error(
                "artist_graph_track_upsert_error",
                spotify_id=spotify_id,
                error=str(exc),
            )
            return False

    # ── Artist enqueueing ─────────────────────────────────────────────────────

    async def _enqueue_related_artists(
        self,
        artist_id: str,
        current_depth: int,
        followers: int,
        popularity: int,
    ) -> int:
        """
        Fetch Spotify related artists and add them to the graph queue.

        Related artists receive 60 % of the priority a direct-featured
        artist would receive (they are second-degree connections).
        """
        assert self._spotify is not None
        related = await self._spotify.get_related_artists(artist_id)
        enqueued = 0

        for rel in related:
            rel_id = rel.get("id")
            if not rel_id:
                continue
            rel_followers = (rel.get("followers") or {}).get("total") or 0
            rel_pop = rel.get("popularity") or 0
            priority = compute_artist_priority(rel_pop, rel_followers) * 0.6

            was_new = await self._maybe_enqueue_artist(
                rel_id,
                rel.get("name") or "",
                depth=current_depth + 1,
                priority=priority,
                followers=rel_followers,
                popularity=rel_pop,
            )
            if was_new:
                enqueued += 1

        return enqueued

    async def _maybe_enqueue_artist(
        self,
        artist_id: str,
        name: str,
        depth: int,
        priority: float = 0.1,
        followers: Optional[int] = None,
        popularity: Optional[int] = None,
    ) -> bool:
        """
        Insert artist into the graph queue if not already present.

        Checks both the processed cache and the queue (via ``$setOnInsert``).
        Returns ``True`` if the artist was newly added to the queue.
        """
        # Fast-path: already processed
        cache_col = self.db[ARTIST_PROCESSED_CACHE_COL]
        if await cache_col.find_one({"artist_id": artist_id}, {"_id": 1}):
            return False

        col = self.db[ARTIST_GRAPH_QUEUE_COL]
        now = datetime.now(timezone.utc)

        item = ArtistGraphItem(
            artist_id=artist_id,
            name=name,
            source=GraphSource.SPOTIFY,
            depth=depth,
            priority=priority,
            followers=followers,
            popularity=popularity,
        )
        doc = item.to_mongo()
        doc["created_at"] = now
        doc["updated_at"] = now

        try:
            result = await col.update_one(
                {"artist_id": artist_id},
                {"$setOnInsert": doc},
                upsert=True,
            )
            return result.upserted_id is not None
        except Exception:
            return False

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _mark_processed(
        self, col: Any, artist_id: str, now: datetime
    ) -> None:
        await col.update_one(
            {"artist_id": artist_id},
            {"$set": {"processed": True, "locked_at": None, "locked_by": None, "updated_at": now}},
        )

    # ── Deezer fallback path (SPOTIFY_ENABLED=false) ─────────────────────────

    async def _process_artist_deezer(self, item: Dict[str, Any]) -> None:
        """
        Process one artist_graph_queue item via Deezer API.

        Used when SPOTIFY_ENABLED=false. Resolves the Spotify artist name to a
        Deezer artist (fuzzy name match), harvests top tracks + albums, upserts
        them using the same ISRC/fingerprint dedup as deezer_direct_worker, and
        enqueues Deezer related artists back into artist_graph_queue.
        """
        assert self._deezer is not None
        artist_id: str = item["artist_id"]
        artist_name: str = item.get("name") or ""
        depth: int = item.get("depth", 0)
        col = self.db[ARTIST_GRAPH_QUEUE_COL]
        cache_col = self.db[ARTIST_PROCESSED_CACHE_COL]
        now = datetime.now(timezone.utc)

        try:
            # Skip if already processed by another replica
            if await cache_col.find_one({"artist_id": artist_id}, {"_id": 1}):
                await self._mark_processed(col, artist_id, now)
                return

            # Resolve Deezer artist ID
            if artist_id.startswith("deezer:"):
                deezer_id = int(artist_id.split(":", 1)[1])
            else:
                if not artist_name:
                    await self._mark_processed(col, artist_id, now)
                    return
                deezer_artist = await self._find_deezer_artist(artist_name)
                if not deezer_artist:
                    logger.debug(
                        "artist_graph_deezer_not_found",
                        artist_id=artist_id,
                        name=artist_name,
                    )
                    await self._mark_processed(col, artist_id, now)
                    return
                deezer_id = deezer_artist["id"]

            inserted = 0

            # Top tracks (1 API call)
            top_tracks = await self._deezer.get_artist_top_tracks(deezer_id, limit=100)
            for t in top_tracks:
                if await self._upsert_deezer_track(t, album_data=t.get("album")):
                    inserted += 1

            # Albums (optional, same flag as deezer_direct_worker)
            if self.settings.deezer_crawl_albums:
                inserted += await self._crawl_deezer_albums(deezer_id)

            # BFS: related artists → artist_graph_queue
            new_enqueued = 0
            if depth < self.settings.artist_graph_max_depth:
                new_enqueued = await self._enqueue_related_deezer(deezer_id, depth)

            # Mark done in processed cache
            await cache_col.update_one(
                {"artist_id": artist_id},
                {"$setOnInsert": {"artist_id": artist_id, "processed_at": now}},
                upsert=True,
            )
            await self._mark_processed(col, artist_id, now)

            await self.increment_stat("artists_processed", 1)
            await self.increment_stat("tracks_discovered", inserted)

            logger.info(
                "artist_graph_deezer_artist_done",
                artist_id=artist_id,
                deezer_id=deezer_id,
                artist_name=artist_name,
                depth=depth,
                tracks_inserted=inserted,
                new_artists_enqueued=new_enqueued,
            )

        except Exception as exc:
            logger.error(
                "artist_graph_deezer_artist_failed",
                artist_id=artist_id,
                artist_name=artist_name,
                error=str(exc),
                exc_info=True,
            )
            retry_count = item.get("retry_count", 0) + 1
            processed = retry_count >= self.settings.worker_retry_limit
            await col.update_one(
                {"artist_id": artist_id},
                {
                    "$set": {
                        "processed": processed,
                        "retry_count": retry_count,
                        "locked_at": None,
                        "locked_by": None,
                        "updated_at": now,
                    }
                },
            )

    async def _find_deezer_artist(self, name: str) -> Optional[Dict[str, Any]]:
        """Search Deezer by artist name, return best fuzzy match or None."""
        results = await self._deezer.search_artists(name, limit=5)
        if not results:
            return None
        norm = name.lower().strip()
        best: Optional[Dict[str, Any]] = None
        best_score = 0.0
        for r in results:
            r_name = (r.get("name") or "").lower().strip()
            if _fuzz is not None:
                score = float(_fuzz.ratio(norm, r_name))
            else:
                score = 100.0 if norm == r_name else 0.0
            if score > best_score:
                best_score = score
                best = r
        return best if best_score >= 60.0 else None

    async def _crawl_deezer_albums(self, deezer_id: int) -> int:
        """Fetch artist albums via Deezer and upsert their tracks. Returns inserted count."""
        assert self._deezer is not None
        inserted = 0
        index = 0
        limit = 25
        max_albums = self.settings.deezer_max_albums_per_artist

        while True:
            page = await self._deezer.get_artist_albums(deezer_id, limit=limit, index=index)
            albums = page.get("data", [])
            if not albums:
                break
            for album in albums:
                album_id = album.get("id")
                if not album_id:
                    continue
                tracks = await self._deezer.get_album_tracks(album_id)
                for t in tracks:
                    if await self._upsert_deezer_track(t, album_data=album):
                        inserted += 1

            total = page.get("total", 0)
            index += limit
            if index >= total or index >= max_albums * limit:
                break

        return inserted

    async def _enqueue_related_deezer(self, deezer_id: int, current_depth: int) -> int:
        """
        Fetch Deezer related artists and enqueue new ones into artist_graph_queue.

        Uses artist_id = "deezer:{id}" so the next processing cycle resolves them
        via the Deezer path (no Spotify lookup needed).
        """
        assert self._deezer is not None
        related = await self._deezer.get_artist_related(deezer_id)
        if not related:
            return 0

        col = self.db[ARTIST_GRAPH_QUEUE_COL]
        cache_col = self.db[ARTIST_PROCESSED_CACHE_COL]
        now = datetime.now(timezone.utc)
        enqueued = 0

        for rel in related:
            rel_deezer_id = rel.get("id")
            rel_name = rel.get("name") or ""
            if not rel_deezer_id:
                continue

            graph_id = f"deezer:{rel_deezer_id}"

            # Skip already-processed artists
            if await cache_col.find_one({"artist_id": graph_id}, {"_id": 1}):
                continue

            item = ArtistGraphItem(
                artist_id=graph_id,
                name=rel_name,
                source=GraphSource.SPOTIFY,  # reused enum value; source label only
                depth=current_depth + 1,
                priority=0.1,
            )
            doc = item.to_mongo()
            doc["created_at"] = now
            doc["updated_at"] = now

            try:
                result = await col.update_one(
                    {"artist_id": graph_id},
                    {"$setOnInsert": doc},
                    upsert=True,
                )
                if result.upserted_id is not None:
                    enqueued += 1
            except Exception:
                pass

        return enqueued

    async def _upsert_deezer_track(
        self,
        raw: Dict[str, Any],
        album_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Upsert a Deezer track into the main tracks collection.

        Identical dedup logic to deezer_direct_worker:
          1. ISRC match → existing doc, bump appearance_score only.
          2. Fingerprint match (title + artist_name hash) → same.
          3. Neither → new insert with spotify_id = "deezer:{track_id}".
        """
        from app.models.track import AlbumRef  # already imported at top level, but explicit here

        track_id = raw.get("id")
        title = (raw.get("title") or raw.get("title_short") or "").strip()
        if not track_id or not title:
            return False

        artist_raw = raw.get("artist") or {}
        artist_name = (artist_raw.get("name") or "").strip()
        if not artist_name:
            return False

        duration_s: int = raw.get("duration") or 0
        duration_ms: int = duration_s * 1000
        isrc: Optional[str] = raw.get("isrc") or None
        explicit: bool = raw.get("explicit_lyrics") or False
        deezer_artist_id = artist_raw.get("id", 0)

        album_ref: Optional[AlbumRef] = None
        if album_data and album_data.get("id"):
            album_ref = AlbumRef(
                spotify_id=f"deezer_album:{album_data['id']}",
                name=album_data.get("title") or album_data.get("name") or "",
                release_date=album_data.get("release_date"),
                images=[{"url": album_data["cover"]}] if album_data.get("cover") else [],
            )

        fp = _deezer_fingerprint(title, artist_name, duration_s)
        placeholder_spotify_id = f"deezer:{track_id}"

        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)

        insert_doc: Dict[str, Any] = {
            "spotify_id": placeholder_spotify_id,
            "isrc": isrc,
            "fingerprint": fp,
            "name": title,
            "artists": [{"spotify_id": f"deezer:{deezer_artist_id}", "name": artist_name}],
            "album": album_ref.model_dump() if album_ref else None,
            "popularity": raw.get("rank", 0) // 10000,
            "duration_ms": duration_ms,
            "explicit": explicit,
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

        filter_q: Dict[str, Any] = {"isrc": isrc} if isrc else {"fingerprint": fp}

        update_ops: Dict[str, Any] = {
            "$setOnInsert": {**insert_doc, "created_at": now},
            "$inc": {"appearance_score": 1},
            "$set": {"updated_at": now},
        }

        try:
            result = await col.update_one(filter_q, update_ops, upsert=True)
            return result.upserted_id is not None
        except Exception as exc:
            if "duplicate key" in str(exc).lower():
                return False
            logger.error(
                "artist_graph_deezer_track_upsert_error",
                track_id=track_id,
                title=title,
                error=str(exc),
            )
            return False

    async def _cache_album(self, album_id: str, now: datetime) -> None:
        album_cache_col = self.db[ALBUM_PROCESSED_CACHE_COL]
        try:
            await album_cache_col.update_one(
                {"album_id": album_id},
                {"$setOnInsert": {"album_id": album_id, "processed_at": now}},
                upsert=True,
            )
        except Exception:
            pass
