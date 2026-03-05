"""
Seeder worker.

Runs once at startup to populate:
1. ``playlist_queue`` — featured playlists + category playlists + keyword searches
2. ``genre_queue``    — all Spotify genre seeds (hardcoded + dynamic)

After seeding, this worker exits (it does not loop forever).
It is idempotent: re-running it skips playlists/genres already in the queue.

``docker-compose`` uses ``restart: on-failure`` for this container so it
only re-runs if it crashes.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import List

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne

from app.core.config import Settings
from app.db.collections import PLAYLIST_QUEUE_COL, GENRE_QUEUE_COL, STATS_COL
from app.models.queue import PlaylistQueueItem, GenreQueueItem, QueueStatus
from app.models.stats import STATS_DOC_ID
from app.services.spotify import SpotifyClient
from app.utils.signals import shutdown_event

logger = structlog.get_logger(__name__)

# ── Comprehensive genre list ───────────────────────────────────────────────────
# Covers Spotify's available genre seeds plus extended taxonomy.
GENRES: List[str] = [
    "acoustic", "afrobeat", "alt-rock", "alternative", "ambient", "anime",
    "black-metal", "bluegrass", "blues", "bossanova", "brazil", "breakbeat",
    "british", "cantopop", "chicago-blues", "children", "chill", "classical",
    "club", "comedy", "country", "dance", "dancehall", "death-metal",
    "deep-house", "detroit-techno", "disco", "disney", "drum-and-bass", "dub",
    "dubstep", "edm", "electro", "electronic", "emo", "folk", "forro",
    "french", "funk", "garage", "german", "gospel", "goth", "grindcore",
    "groove", "grunge", "guitar", "happy", "hard-rock", "hardcore", "hardstyle",
    "heavy-metal", "hip-hop", "holidays", "honky-tonk", "house", "idm",
    "indian", "indie", "indie-pop", "industrial", "iranian", "j-dance",
    "j-idol", "j-pop", "j-rock", "jazz", "k-pop", "kids", "latin",
    "latino", "malay", "mandopop", "metal", "metal-misc", "metalcore",
    "minimal-techno", "movies", "mpb", "new-age", "new-release", "opera",
    "pagode", "party", "philippines-opm", "piano", "pop", "pop-film",
    "post-dubstep", "power-pop", "progressive-house", "psych-rock", "punk",
    "punk-rock", "r-n-b", "rainy-day", "reggae", "reggaeton", "road-trip",
    "rock", "rock-n-roll", "rockabilly", "romance", "sad", "salsa", "samba",
    "sertanejo", "show-tunes", "singer-songwriter", "ska", "sleep", "songwriter",
    "soul", "soundtracks", "spanish", "study", "summer", "swedish", "synth-pop",
    "tango", "techno", "trance", "trip-hop", "turkish", "work-out", "world-music",
    # Extended genres
    "afro-house", "afro-pop", "amapiano", "bachata", "baile-funk", "baladas",
    "banda", "brega", "c-pop", "cafe-au-lait", "caribbean", "celtic",
    "cha-cha", "chillwave", "christian", "cumbia", "deep-tech-house",
    "dembow", "disco-house", "doo-wop", "downtempo", "drone", "drum-bass",
    "easy-listening", "experimental", "flamenco", "future-bass", "future-house",
    "g-funk", "gangsta-rap", "glitch-hop", "gospel-blues", "gothic-metal",
    "hip-hop-old-school", "indie-folk", "indie-r-n-b", "indie-rock",
    "j-folk", "j-fusion", "jazz-funk", "jpop", "jungle", "lo-fi",
    "lo-fi-beats", "lounge", "macarena", "melodic-death-metal", "merengue",
    "merseybeat", "motown", "neo-soul", "noise", "nu-jazz", "nu-metal",
    "nujazz", "nuyorican", "outlaw-country", "overtone", "phonk",
    "post-rock", "progressive-metal", "progressive-rock", "psychedelic",
    "rnb", "roots-reggae", "shoegaze", "slow-core", "smooth-jazz",
    "speed-metal", "surf-rock", "swing", "symphonic-metal", "synthwave",
    "trap", "trap-latino", "tropicalia", "uk-garage", "vaporwave",
    "vocal-jazz", "west-coast-rap", "zouk",
]


# Well-known Spotify editorial playlist IDs to seed from
SPOTIFY_EDITORIAL_PLAYLISTS: List[str] = [
    "37i9dQZF1DXcBWIGoYBM5M",  # Today's Top Hits
    "37i9dQZF1DX0XUsuxWHRQd",  # RapCaviar
    "37i9dQZF1DX4JAvHpjipBk",  # New Music Friday
    "37i9dQZEVXbMDoHDwVN2tF",  # Global Top 50
    "37i9dQZEVXbNG2KDcFcKOF",  # Top Songs - Global
    "37i9dQZF1DXbYM3nMM0oPk",  # mint
    "37i9dQZF1DX10zKzsJ2jva",  # Viva Latino
    "37i9dQZF1DX4dyzvuaRJ0n",  # misery business
    "37i9dQZF1DXdPec7aLTmlC",  # Hot Country
    "37i9dQZF1DWXRqgorJj26U",  # Rock Classics
    "37i9dQZF1DX4o1uurG5MnT",  # Jazz Classics
    "37i9dQZF1DXcZDD7cfEKhW",  # Piano in the Background
    "37i9dQZF1DX4sWSpwq3LiO",  # Peaceful Piano
    "37i9dQZF1DWZd79rJ6a7lp",  # Jazz Vibes
    "37i9dQZF1DX0SM0LYsmbMT",  # Hip-Hop Controller
    "37i9dQZF1DWY4xHQp97fN6",  # Get Turnt
    "37i9dQZF1DXbTxeAdrVG2l",  # Beast Mode
    "37i9dQZF1DX4UtSsGT1Sbe",  # All Out 80s
    "37i9dQZF1DXarRysLJmuju",  # All Out 90s
    "37i9dQZF1DX4o1uurG5MnT",  # All Out 00s
    "37i9dQZF1DX5Ejj0EkURtP",  # All Out 2010s
    "37i9dQZF1DWWEJlAGA9gs0",  # Classical Essentials
    "37i9dQZF1DWXLeA8Omikj7",  # Pop Rising
    "37i9dQZF1DX2sUQwD7tbmL",  # Electronic Rising
    "37i9dQZF1DX0r3x8OtiwEM",  # R&B Rising
    "37i9dQZF1DX4JAvHpjipBk",  # Fresh Finds
]

# Playlist search keywords to discover more playlists
PLAYLIST_SEARCH_KEYWORDS: List[str] = [
    "best hits", "top songs", "greatest hits", "playlist essentials",
    "most popular", "viral hits", "hot songs", "trending",
    "classics", "throwback", "old school", "throwback hits",
    "summer hits", "chill vibes", "party playlist", "workout music",
    "sleep music", "focus music", "road trip", "coffee shop",
    "indie essentials", "pop essentials", "hip hop essentials",
    "jazz essentials", "rock essentials", "r&b essentials",
    "alternative essentials", "electronic essentials",
]


class SeederWorker:
    """
    One-shot seeder that initializes the work queues.

    Not a ``BaseWorker`` subclass because it runs once and exits,
    rather than polling indefinitely.
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        self.db = db
        self.settings = settings

    async def run(self) -> None:
        """Execute seeding and exit."""
        logger.info("seeder_started")

        spotify = SpotifyClient(self.settings)
        try:
            await asyncio.gather(
                self._seed_genres(),
                self._seed_playlists(spotify),
            )
        finally:
            await spotify.aclose()

        # Record seeding completion in stats
        now = datetime.now(timezone.utc)
        await self.db[STATS_COL].update_one(
            {"doc_id": STATS_DOC_ID},
            {
                "$set": {
                    "last_stats_at": now,
                    "started_at": now,
                },
                "$setOnInsert": {"doc_id": STATS_DOC_ID},
            },
            upsert=True,
        )

        logger.info("seeder_finished")

    async def _seed_genres(self) -> None:
        """Insert all genres into genre_queue (idempotent)."""
        col = self.db[GENRE_QUEUE_COL]
        ops = []
        for genre in GENRES:
            item = GenreQueueItem(genre=genre)
            ops.append(
                UpdateOne(
                    {"genre": genre},
                    {
                        "$setOnInsert": item.to_mongo(),
                    },
                    upsert=True,
                )
            )

        if ops:
            result = await col.bulk_write(ops, ordered=False)
            inserted = result.upserted_count
            logger.info("genres_seeded", total=len(GENRES), new=inserted)
            await self._update_stat("genres_seeded", inserted)

    async def _seed_playlists(self, spotify: SpotifyClient) -> None:
        """
        Seed playlist queue from multiple sources:
        1. Hardcoded editorial playlists
        2. Spotify featured playlists
        3. Category playlists
        4. Keyword searches
        """
        playlist_ids: dict[str, dict] = {}

        # 1. Hardcoded editorials
        for pid in SPOTIFY_EDITORIAL_PLAYLISTS:
            playlist_ids[pid] = {"name": "editorial", "followers": 0}

        # 2. Featured playlists
        try:
            resp = await spotify.get_featured_playlists(
                limit=self.settings.featured_playlists_limit
            )
            for pl in resp.get("playlists", {}).get("items", []):
                if pl and pl.get("id"):
                    followers = pl.get("followers", {}).get("total", 0)
                    playlist_ids[pl["id"]] = {
                        "name": pl.get("name", ""),
                        "followers": followers,
                    }
            logger.info(
                "featured_playlists_fetched",
                count=len(resp.get("playlists", {}).get("items", [])),
            )
        except Exception as exc:
            logger.warning("featured_playlists_failed", error=str(exc))

        # 3. Browse categories → category playlists
        try:
            cat_resp = await spotify.get_categories(
                limit=self.settings.categories_limit
            )
            categories = cat_resp.get("categories", {}).get("items", [])
            consecutive_errors = 0
            for cat in categories:
                if shutdown_event.is_set():
                    break
                cat_id = cat.get("id")
                if not cat_id:
                    continue
                try:
                    pl_resp = await spotify.get_category_playlists(
                        cat_id, limit=self.settings.category_playlists_limit
                    )
                    consecutive_errors = 0
                    for pl in pl_resp.get("playlists", {}).get("items", []):
                        if pl and pl.get("id"):
                            playlist_ids[pl["id"]] = {
                                "name": pl.get("name", ""),
                                "followers": 0,
                            }
                except Exception as exc:
                    consecutive_errors += 1
                    if consecutive_errors >= 3:
                        logger.warning(
                            "category_playlists_endpoint_unavailable",
                            skipped=len(categories),
                        )
                        break
                    logger.debug(
                        "category_playlists_skip",
                        category=cat_id,
                        error=str(exc),
                    )
            logger.info(
                "category_playlists_fetched",
                categories=len(categories),
                total_playlists=len(playlist_ids),
            )
        except Exception as exc:
            logger.warning("categories_failed", error=str(exc))

        # 4. Keyword searches
        for keyword in PLAYLIST_SEARCH_KEYWORDS:
            if shutdown_event.is_set():
                break
            try:
                resp = await spotify.search_playlists(keyword, limit=50)
                for pl in resp.get("playlists", {}).get("items", []):
                    if pl and pl.get("id"):
                        playlist_ids[pl["id"]] = {
                            "name": pl.get("name", ""),
                            "followers": 0,
                        }
            except Exception as exc:
                logger.debug("playlist_search_skip", keyword=keyword, error=str(exc))

        logger.info("playlists_discovered", total=len(playlist_ids))

        # Filter by minimum followers where we have that data
        filtered = {
            pid: meta
            for pid, meta in playlist_ids.items()
            if meta["followers"] == 0
            or meta["followers"] >= self.settings.min_playlist_followers
        }
        logger.info("playlists_after_filter", total=len(filtered))

        # Bulk upsert into playlist_queue
        col = self.db[PLAYLIST_QUEUE_COL]
        ops = []
        for pid, meta in filtered.items():
            item = PlaylistQueueItem(
                spotify_id=pid,
                name=meta.get("name", ""),
                followers=meta.get("followers", 0),
            )
            ops.append(
                UpdateOne(
                    {"spotify_id": pid},
                    {"$setOnInsert": item.to_mongo()},
                    upsert=True,
                )
            )

        if ops:
            result = await col.bulk_write(ops, ordered=False)
            inserted = result.upserted_count
            logger.info("playlists_seeded", total=len(ops), new=inserted)
            await self._update_stat("playlists_seeded", inserted)

    async def _update_stat(self, field: str, amount: int) -> None:
        await self.db[STATS_COL].update_one(
            {"doc_id": STATS_DOC_ID},
            {
                "$inc": {field: amount},
                "$setOnInsert": {
                    "doc_id": STATS_DOC_ID,
                    "started_at": datetime.now(timezone.utc),
                },
                "$set": {"last_stats_at": datetime.now(timezone.utc)},
            },
            upsert=True,
        )
