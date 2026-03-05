"""
MongoDB collection accessors and index bootstrapping.

Call ``ensure_indexes(db)`` once at startup from every worker so that
indexes are created idempotently (MongoDB is a no-op if they exist).

v2 additions:
- REGIONAL_SEED_QUEUE_COL  — regional search queue for regional_seed_worker
- New track indexes: language, script, regions.*, musicbrainz_*, mb_priority
"""
from __future__ import annotations

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, IndexModel

logger = structlog.get_logger(__name__)

# ── Collection name constants ────────────────────────────────────────────────
TRACKS_COL = "tracks"
ARTISTS_COL = "artists"
STATS_COL = "system_stats"
PLAYLIST_QUEUE_COL = "playlist_queue"
ARTIST_QUEUE_COL = "artist_queue"
GENRE_QUEUE_COL = "genre_queue"
REGIONAL_SEED_QUEUE_COL = "regional_seed_queue"   # v2


async def ensure_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    """
    Create all required indexes idempotently.

    Safe to call on every worker startup — MongoDB will not recreate
    existing indexes and the operation is fast when nothing changes.
    """
    await _ensure_track_indexes(db)
    await _ensure_artist_indexes(db)
    await _ensure_queue_indexes(db)
    logger.info("indexes_ensured")


async def _ensure_track_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    col = db[TRACKS_COL]
    indexes = [
        # ── Deduplication (v1) ────────────────────────────────────────────
        IndexModel([("spotify_id", ASCENDING)], unique=True, name="spotify_id_unique"),
        IndexModel(
            [("isrc", ASCENDING)],
            unique=True, sparse=True, name="isrc_unique_sparse",
        ),
        IndexModel(
            [("fingerprint", ASCENDING)],
            unique=True, sparse=True, name="fingerprint_unique_sparse",
        ),

        # ── Worker polling (v1) ───────────────────────────────────────────
        IndexModel([("status", ASCENDING)], name="status_idx"),
        IndexModel(
            [("status", ASCENDING), ("locked_at", ASCENDING)],
            name="status_locked_at_idx",
        ),

        # ── Analytics / export (v1) ───────────────────────────────────────
        IndexModel([("quality_score", DESCENDING)], name="quality_score_idx"),
        IndexModel([("popularity", DESCENDING)], name="popularity_idx"),
        IndexModel([("appearance_score", DESCENDING)], name="appearance_score_idx"),
        IndexModel([("artists.spotify_id", ASCENDING)], name="artist_spotify_id_idx"),

        # ── Language & script (v2) ────────────────────────────────────────
        # language_worker polls: language IS NULL, status IN [...]
        IndexModel([("language", ASCENDING)], sparse=True, name="language_idx"),
        IndexModel([("script", ASCENDING)], sparse=True, name="script_idx"),
        IndexModel([("language_detected", ASCENDING)], name="language_detected_idx"),

        # ── Transliteration (v2) ──────────────────────────────────────────
        IndexModel(
            [("transliteration_done", ASCENDING), ("script", ASCENDING)],
            name="transliteration_done_script_idx",
        ),

        # ── MusicBrainz (v2) ──────────────────────────────────────────────
        # musicbrainz_worker polls: mb_enriched=false, sorted by mb_priority desc
        IndexModel([("musicbrainz_enriched", ASCENDING)], name="mb_enriched_idx"),
        IndexModel(
            [("musicbrainz_enriched", ASCENDING), ("mb_priority", DESCENDING)],
            name="mb_enriched_priority_idx",
        ),
        IndexModel(
            [("musicbrainz_confidence_score", DESCENDING)],
            sparse=True, name="mb_confidence_idx",
        ),

        # ── Regional (v2) ─────────────────────────────────────────────────
        IndexModel([("regions.cis", ASCENDING)], sparse=True, name="regions_cis_idx"),
        IndexModel(
            [("regions.central_asia", ASCENDING)],
            sparse=True, name="regions_central_asia_idx",
        ),
        IndexModel(
            [("regions.mena", ASCENDING)],
            sparse=True, name="regions_mena_idx",
        ),
        IndexModel([("mb_priority", DESCENDING)], name="mb_priority_idx"),

        # ── Compound: regional quality ranking ────────────────────────────
        IndexModel(
            [("regions.cis", ASCENDING), ("quality_score", DESCENDING)],
            sparse=True, name="cis_quality_idx",
        ),
        IndexModel(
            [("regions.central_asia", ASCENDING), ("quality_score", DESCENDING)],
            sparse=True, name="central_asia_quality_idx",
        ),
    ]
    await col.create_indexes(indexes)


async def _ensure_artist_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    col = db[ARTISTS_COL]
    indexes = [
        IndexModel([("spotify_id", ASCENDING)], unique=True, name="spotify_id_unique"),
        IndexModel([("popularity", DESCENDING)], name="popularity_idx"),
        IndexModel([("followers", DESCENDING)], name="followers_idx"),
        # v2: country for regional classification
        IndexModel([("country", ASCENDING)], sparse=True, name="country_idx"),
    ]
    await col.create_indexes(indexes)


async def _ensure_queue_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    # Playlist queue
    pl_col = db[PLAYLIST_QUEUE_COL]
    await pl_col.create_indexes([
        IndexModel([("spotify_id", ASCENDING)], unique=True, name="spotify_id_unique"),
        IndexModel([("status", ASCENDING)], name="status_idx"),
    ])

    # Artist queue
    ar_col = db[ARTIST_QUEUE_COL]
    await ar_col.create_indexes([
        IndexModel([("spotify_id", ASCENDING)], unique=True, name="spotify_id_unique"),
        IndexModel([("status", ASCENDING)], name="status_idx"),
    ])

    # Genre queue
    ge_col = db[GENRE_QUEUE_COL]
    await ge_col.create_indexes([
        IndexModel([("genre", ASCENDING)], unique=True, name="genre_unique"),
        IndexModel([("status", ASCENDING)], name="status_idx"),
    ])

    # Regional seed queue (v2)
    rs_col = db[REGIONAL_SEED_QUEUE_COL]
    await rs_col.create_indexes([
        # query + market together is the unique key
        IndexModel(
            [("query", ASCENDING), ("market", ASCENDING)],
            unique=True, name="query_market_unique",
        ),
        IndexModel([("status", ASCENDING)], name="status_idx"),
        IndexModel([("region", ASCENDING), ("status", ASCENDING)], name="region_status_idx"),
    ])
