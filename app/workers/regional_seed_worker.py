"""
Regional Seed Worker.

Discovers tracks from CIS, Central Asia, and MENA regions through targeted
Spotify searches. Self-seeding: on first run it populates the
``regional_seed_queue`` collection, then processes items from it.

Search strategy per region:
1. Genre keyword queries (e.g. "uzbek pop", "arabic hip hop")
2. Market-scoped searches (Spotify search with market=UZ, market=AE, etc.)
3. Artist expansion from discovered regional tracks
   (this happens naturally via artist_worker after tracks are inserted)

Discovered tracks are inserted as ``base_collected`` with:
- ``mb_priority`` pre-set based on region
- ``regions`` pre-classified based on the seeding query
- ``markets`` populated

All inserts use the same ISRC/fingerprint deduplication as the main workers.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne

from app.core.config import Settings
from app.db.collections import (
    REGIONAL_SEED_QUEUE_COL,
    TRACKS_COL,
    ARTIST_QUEUE_COL,
)
from app.models.queue import QueueStatus
from app.models.track import TrackDocument, TrackStatus, ArtistRef, AlbumRef
from app.services.spotify import SpotifyClient, SpotifyError
from app.utils.deduplication import compute_fingerprint, extract_isrc
from app.utils.regional import compute_mb_priority, classify_regions
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID

logger = structlog.get_logger(__name__)


# ── Regional query definitions ────────────────────────────────────────────────
# (query_string, region_label, spotify_market_code_or_None)

RegionalQuery = Tuple[str, str, Optional[str]]

REGIONAL_QUERIES: List[RegionalQuery] = [
    # ── Central Asia ──────────────────────────────────────────────────────────
    ("uzbek pop",           "central_asia", "UZ"),
    ("o'zbek pop",          "central_asia", "UZ"),
    ("uzbek rap",           "central_asia", "UZ"),
    ("o'zbek rap",          "central_asia", "UZ"),
    ("uzbek folk",          "central_asia", "UZ"),
    ("kazakh pop",          "central_asia", "KZ"),
    ("qazaq pop",           "central_asia", "KZ"),
    ("kazakh hip hop",      "central_asia", "KZ"),
    ("kyrgyz music",        "central_asia", "KG"),
    ("tajik pop",           "central_asia", "TJ"),
    ("tajik music",         "central_asia", "TJ"),
    ("turkmen music",       "central_asia", "TM"),
    ("central asian folk",  "central_asia", None),
    ("central asian pop",   "central_asia", None),
    # ── CIS ──────────────────────────────────────────────────────────────────
    ("russian pop",         "cis", "RU"),
    ("russian rap",         "cis", "RU"),
    ("russian hip hop",     "cis", "RU"),
    ("russian rock",        "cis", "RU"),
    ("shanson",             "cis", "RU"),
    ("russian estrada",     "cis", "RU"),
    ("russian indie",       "cis", "RU"),
    ("ukrainian pop",       "cis", "UA"),
    ("ukrainian hip hop",   "cis", "UA"),
    ("ukrainian rock",      "cis", "UA"),
    ("belarusian music",    "cis", "BY"),
    ("georgian pop",        "cis", "GE"),
    ("georgian music",      "cis", "GE"),
    ("armenian pop",        "cis", "AM"),
    ("azerbaijani pop",     "cis", "AZ"),
    ("azerbaijani music",   "cis", "AZ"),
    ("moldovan pop",        "cis", "MD"),
    ("baltics pop",         "cis", "LV"),
    # ── MENA ─────────────────────────────────────────────────────────────────
    ("arabic pop",          "mena", "AE"),
    ("arab pop",            "mena", "AE"),
    ("arabic hip hop",      "mena", "SA"),
    ("khaleeji",            "mena", "AE"),
    ("gulf music",          "mena", "SA"),
    ("egyptian pop",        "mena", "EG"),
    ("levant pop",          "mena", "LB"),
    ("moroccan music",      "mena", "MA"),
    ("rai",                 "mena", "DZ"),
    ("arabic rnb",          "mena", "AE"),
    ("arabic trap",         "mena", "AE"),
    ("arabic indie",        "mena", "LB"),
    ("mahraganat",          "mena", "EG"),
    ("turkish pop",         "mena", "TR"),
    ("turkish hip hop",     "mena", "TR"),
    ("persian pop",         "mena", "IR"),
    # ── Eastern Europe ───────────────────────────────────────────────────────
    ("polish pop",          "eastern_europe", "PL"),
    ("polish hip hop",      "eastern_europe", "PL"),
    ("polish rap",          "eastern_europe", "PL"),
    ("czech pop",           "eastern_europe", "CZ"),
    ("slovak pop",          "eastern_europe", "SK"),
    ("hungarian pop",       "eastern_europe", "HU"),
    ("romanian pop",        "eastern_europe", "RO"),
    ("romanian hip hop",    "eastern_europe", "RO"),
    ("bulgarian pop",       "eastern_europe", "BG"),
    ("serbian pop",         "eastern_europe", "RS"),
    ("serbian rap",         "eastern_europe", "RS"),
    ("croatian pop",        "eastern_europe", "HR"),
    ("greek pop",           "eastern_europe", "GR"),
    ("greek rap",           "eastern_europe", "GR"),
    ("balkan music",        "eastern_europe", None),
    ("balkan pop",          "eastern_europe", None),
    ("turbo folk",          "eastern_europe", "RS"),
    # ── South Asia ───────────────────────────────────────────────────────────
    ("bollywood",           "south_asia", "IN"),
    ("hindi pop",           "south_asia", "IN"),
    ("hindi rap",           "south_asia", "IN"),
    ("punjabi pop",         "south_asia", "IN"),
    ("punjabi hip hop",     "south_asia", "IN"),
    ("tamil pop",           "south_asia", "IN"),
    ("telugu pop",          "south_asia", "IN"),
    ("bangla music",        "south_asia", "BD"),
    ("bangladeshi pop",     "south_asia", "BD"),
    ("sinhala music",       "south_asia", "LK"),
    ("nepali pop",          "south_asia", "NP"),
    ("indian indie",        "south_asia", "IN"),
    # ── East Asia ────────────────────────────────────────────────────────────
    ("mandopop",            "east_asia", "TW"),
    ("cantopop",            "east_asia", "HK"),
    ("chinese pop",         "east_asia", "CN"),
    ("chinese hip hop",     "east_asia", "CN"),
    ("j-pop",               "east_asia", "JP"),
    ("j-rock",              "east_asia", "JP"),
    ("j-rap",               "east_asia", "JP"),
    ("city pop",            "east_asia", "JP"),
    ("k-pop",               "east_asia", "KR"),
    ("k-rap",               "east_asia", "KR"),
    ("k-indie",             "east_asia", "KR"),
    ("mongolian music",     "east_asia", "MN"),
    ("mongolian pop",       "east_asia", "MN"),
    # ── Southeast Asia ───────────────────────────────────────────────────────
    ("thai pop",            "southeast_asia", "TH"),
    ("thai hip hop",        "southeast_asia", "TH"),
    ("thai rap",            "southeast_asia", "TH"),
    ("vietnamese pop",      "southeast_asia", "VN"),
    ("vpop",                "southeast_asia", "VN"),
    ("v-pop",               "southeast_asia", "VN"),
    ("opm",                 "southeast_asia", "PH"),
    ("p-pop",               "southeast_asia", "PH"),
    ("philippine pop",      "southeast_asia", "PH"),
    ("indonesian pop",      "southeast_asia", "ID"),
    ("dangdut",             "southeast_asia", "ID"),
    ("malay pop",           "southeast_asia", "MY"),
    ("singapore pop",       "southeast_asia", "SG"),
    ("myanmar music",       "southeast_asia", "MM"),
]

# Market-only searches (no genre keyword — just popular tracks per market)
MARKET_SEARCHES: List[Tuple[str, str]] = [
    # Central Asia
    ("UZ", "central_asia"),
    ("KZ", "central_asia"),
    ("KG", "central_asia"),
    ("TJ", "central_asia"),
    # CIS
    ("AZ", "cis"),
    ("GE", "cis"),
    ("AM", "cis"),
    # MENA
    ("AE", "mena"),
    ("SA", "mena"),
    ("EG", "mena"),
    ("QA", "mena"),
    ("KW", "mena"),
    # Eastern Europe
    ("PL", "eastern_europe"),
    ("RO", "eastern_europe"),
    ("HU", "eastern_europe"),
    ("RS", "eastern_europe"),
    ("BG", "eastern_europe"),
    ("GR", "eastern_europe"),
    # South Asia
    ("IN", "south_asia"),
    ("BD", "south_asia"),
    # East Asia
    ("JP", "east_asia"),
    ("KR", "east_asia"),
    ("TW", "east_asia"),
    ("HK", "east_asia"),
    # Southeast Asia
    ("ID", "southeast_asia"),
    ("TH", "southeast_asia"),
    ("PH", "southeast_asia"),
    ("VN", "southeast_asia"),
    ("MY", "southeast_asia"),
]

MARKET_KEYWORDS = [
    "hits", "top", "best", "popular",
    "rap", "pop", "folk", "music",
]


class RegionalSeedWorker(BaseWorker):
    """
    Seeds the pipeline with regionally targeted tracks.

    Self-initializes the ``regional_seed_queue`` on first run.
    Then processes queue items as genre/market searches.
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._spotify: Optional[SpotifyClient] = None
        self._seeded: bool = False

    async def on_startup(self) -> None:
        self._spotify = SpotifyClient(self.settings)
        await self._bootstrap_queue()
        logger.info("regional_seed_worker_started")

    async def on_shutdown(self) -> None:
        if self._spotify:
            await self._spotify.aclose()

    async def _bootstrap_queue(self) -> None:
        """Populate regional_seed_queue if no pending items remain.

        On first run: inserts all regional query items.
        On subsequent runs (all queries done): resets them back to pending
        so regional searches are refreshed in the next cycle.
        """
        col = self.db[REGIONAL_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)

        pending = await col.count_documents({"status": QueueStatus.PENDING.value})
        if pending > 0:
            logger.info("regional_queue_has_pending", count=pending)
            return

        # Reset completed queries for another discovery cycle
        reset = await col.update_many(
            {"status": QueueStatus.DONE.value},
            {"$set": {"status": QueueStatus.PENDING.value, "offset": 0, "updated_at": now}},
        )
        if reset.modified_count > 0:
            logger.info("regional_queue_reset_for_next_cycle", count=reset.modified_count)
            return

        ops = []

        # Genre + market queries
        for (query, region, market) in REGIONAL_QUERIES:
            m = market or "ALL"
            ops.append(UpdateOne(
                {"query": query, "market": m},
                {"$setOnInsert": {
                    "query": query,
                    "market": m,
                    "region": region,
                    "offset": 0,
                    "status": QueueStatus.PENDING.value,
                    "retry_count": 0,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            ))

        # Market + keyword combos
        for market, region in MARKET_SEARCHES:
            for kw in MARKET_KEYWORDS:
                ops.append(UpdateOne(
                    {"query": kw, "market": market},
                    {"$setOnInsert": {
                        "query": kw,
                        "market": market,
                        "region": region,
                        "offset": 0,
                        "status": QueueStatus.PENDING.value,
                        "retry_count": 0,
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    }},
                    upsert=True,
                ))

        if ops:
            result = await col.bulk_write(ops, ordered=False)
            logger.info("regional_queue_bootstrapped", inserted=result.upserted_count)

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """Claim one regional query at a time (handles pagination internally)."""
        if self._spotify and self._spotify.is_circuit_open:
            logger.warning("regional_seed_worker_idle_circuit_open", reason="Spotify circuit breaker is OPEN — skipping claim")
            return []
        col = self.db[REGIONAL_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)

        # Priority order: niche/underrepresented first
        for region in [
            "central_asia", "cis", "mena",
            "east_asia", "south_asia", "southeast_asia", "eastern_europe",
            None,
        ]:
            filter_q: Dict[str, Any] = {
                "$or": [
                    {"status": QueueStatus.PENDING.value},
                    self.stale_lock_query(),
                ]
            }
            if region is not None:
                filter_q["region"] = region

            doc = await col.find_one_and_update(
                filter_q,
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
            if doc:
                return [doc]

        return []

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for item in batch:
            await self._process_query(item)

    async def _process_query(self, item: Dict[str, Any]) -> None:
        query: str = item["query"]
        market: str = item.get("market", "ALL")
        region: str = item.get("region", "unknown")
        offset: int = item.get("offset", 0)
        max_offset = self.settings.regional_genre_max_offset
        limit = self.settings.regional_genre_batch_limit
        col = self.db[REGIONAL_SEED_QUEUE_COL]
        now = datetime.now(timezone.utc)

        try:
            assert self._spotify is not None
            new_tracks_total = 0

            while offset <= max_offset:
                tracks_raw = await self._search(query, market, limit, offset)
                if not tracks_raw:
                    break

                new_in_page = 0
                for raw in tracks_raw:
                    track_doc = self._parse_track(raw, region, market)
                    if track_doc is None:
                        continue
                    inserted = await self._upsert_track(track_doc)
                    if inserted:
                        new_in_page += 1
                        for artist in track_doc.artists:
                            await self._enqueue_artist(artist.spotify_id, artist.name)

                new_tracks_total += new_in_page
                offset += limit

                # Persist offset so restarts resume here
                await col.update_one(
                    {"query": query, "market": market},
                    {"$set": {"offset": offset, "updated_at": datetime.now(timezone.utc)}},
                )

                logger.debug(
                    "regional_page_done",
                    query=query,
                    market=market,
                    offset=offset,
                    new=new_in_page,
                )

                if len(tracks_raw) < limit:
                    break

            logger.info(
                "regional_query_done",
                query=query,
                market=market,
                region=region,
                new_tracks=new_tracks_total,
            )

            await col.update_one(
                {"query": query, "market": market},
                {"$set": {
                    "status": QueueStatus.DONE.value,
                    "updated_at": now,
                    "locked_at": None,
                    "locked_by": None,
                }},
            )
            await self.increment_stat("total_discovered", new_tracks_total)
            await self.increment_stat("total_base_collected", new_tracks_total)

        except Exception as exc:
            logger.error(
                "regional_query_failed",
                query=query,
                market=market,
                error=str(exc),
                exc_info=True,
            )
            retry = item.get("retry_count", 0) + 1
            new_status = (
                QueueStatus.FAILED.value
                if retry >= self.settings.worker_retry_limit
                else QueueStatus.PENDING.value
            )
            await col.update_one(
                {"query": query, "market": market},
                {"$set": {
                    "status": new_status,
                    "retry_count": retry,
                    "updated_at": now,
                    "locked_at": None,
                    "locked_by": None,
                }},
            )

    async def _search(
        self, query: str, market: str, limit: int, offset: int
    ) -> List[dict]:
        """Search Spotify with optional market scoping."""
        assert self._spotify is not None
        params: Dict[str, Any] = {
            "q": query, "type": "track",
            "limit": min(limit, 50), "offset": offset,
        }
        if market and market != "ALL":
            params["market"] = market

        try:
            resp = await self._spotify.search_tracks(query, limit=limit, offset=offset)
            items = resp.get("tracks", {}).get("items", [])
            # Filter by market if specified (Spotify may not honor market param in search)
            if market and market != "ALL":
                items = [
                    t for t in items
                    if market in (t.get("available_markets") or [])
                ]
            return items
        except SpotifyError as exc:
            logger.warning("regional_search_failed", query=query, market=market, error=str(exc))
            return []

    def _parse_track(
        self, raw: dict, region: str, market: str
    ) -> Optional[TrackDocument]:
        if not raw or not raw.get("id"):
            return None
        name = raw.get("name") or ""
        if not name:
            return None

        artists = [
            ArtistRef(spotify_id=a["id"], name=a.get("name") or "")
            for a in raw.get("artists", []) if a.get("id")
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

        markets_list = raw.get("available_markets") or []
        if market and market != "ALL" and market not in markets_list:
            markets_list.append(market)

        isrc = extract_isrc(raw)
        fp = compute_fingerprint(name, artists[0].spotify_id, raw.get("duration_ms", 0))

        # Pre-classify region from the seeding query
        regions = classify_regions(
            markets=markets_list,
            language=None,
            artist_country=None,
        )

        mb_priority = compute_mb_priority(
            regions={
                "central_asia":   region == "central_asia",
                "cis":            region == "cis",
                "mena":           region == "mena",
                "eastern_europe": region == "eastern_europe",
                "south_asia":     region == "south_asia",
                "east_asia":      region == "east_asia",
                "southeast_asia": region == "southeast_asia",
            },
            language=None,
            markets=markets_list,
        )

        return TrackDocument(
            spotify_id=raw["id"],
            isrc=isrc,
            fingerprint=fp,
            name=name,
            artists=artists,
            album=album,
            popularity=raw.get("popularity", 0),
            duration_ms=raw.get("duration_ms", 0),
            explicit=raw.get("explicit", False),
            markets_count=len(markets_list),
            markets=markets_list,
            status=TrackStatus.BASE_COLLECTED,
            mb_priority=mb_priority,
            regions=regions,  # type: ignore[arg-type]
        )

    async def _upsert_track(self, track: TrackDocument) -> bool:
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        doc = track.to_mongo()
        filter_q = (
            {"isrc": track.isrc} if track.isrc else {"fingerprint": track.fingerprint}
        )
        # Exclude timestamp fields managed separately to avoid MongoDB
        # "Updating the path would create a conflict" error when the same
        # field appears in both $setOnInsert and $set/$max operators.
        insert_doc = {
            k: v for k, v in doc.items()
            if k not in ("created_at", "updated_at")
        }
        try:
            result = await col.update_one(
                filter_q,
                {
                    "$setOnInsert": {**insert_doc, "created_at": now},
                    "$set": {"updated_at": now},
                    # Boost mb_priority if the new value is higher
                    "$max": {"mb_priority": track.mb_priority},
                },
                upsert=True,
            )
            return result.upserted_id is not None
        except Exception as exc:
            if "duplicate key" in str(exc).lower():
                return False
            logger.warning("regional_upsert_error", spotify_id=track.spotify_id, error=str(exc))
            return False

    async def _enqueue_artist(self, artist_id: str, name: str) -> None:
        col = self.db[ARTIST_QUEUE_COL]
        try:
            await col.update_one(
                {"spotify_id": artist_id},
                {"$setOnInsert": {
                    "spotify_id": artist_id, "name": name,
                    "status": QueueStatus.PENDING.value, "retry_count": 0,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            )
        except Exception:
            pass
