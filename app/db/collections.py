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

# v3: external-source discovery
TRACK_CANDIDATES_COL = "track_candidates"
LASTFM_SEED_QUEUE_COL = "lastfm_seed_queue"
YTMUSIC_SEED_QUEUE_COL = "ytmusic_seed_queue"
DISCOGS_SEED_QUEUE_COL = "discogs_seed_queue"

# v4: artist graph expansion
ARTIST_GRAPH_QUEUE_COL   = "artist_graph_queue"
ARTIST_PROCESSED_CACHE_COL = "artist_processed_cache"
ALBUM_PROCESSED_CACHE_COL  = "album_processed_cache"

# v5: Deezer direct discovery (no Spotify required)
DEEZER_SEED_QUEUE_COL = "deezer_seed_queue"

# v6: iTunes / Apple Music discovery (no Spotify required)
ITUNES_SEED_QUEUE_COL = "itunes_seed_queue"

# v7: Yandex Music CIS discovery (no Spotify required; token needed)
YANDEX_SEED_QUEUE_COL = "yandex_seed_queue"


async def ensure_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    """
    Create all required indexes idempotently.

    Safe to call on every worker startup — MongoDB will not recreate
    existing indexes and the operation is fast when nothing changes.
    """
    await _ensure_track_indexes(db)
    await _ensure_artist_indexes(db)
    await _ensure_queue_indexes(db)
    await _ensure_v3_indexes(db)
    await _ensure_graph_indexes(db)
    await _ensure_deezer_indexes(db)
    await _ensure_itunes_indexes(db)
    await _ensure_yandex_indexes(db)
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
        # Pre-check before iTunes/Deezer API calls — count tracks by artist name
        IndexModel([("artists.name", ASCENDING)], name="artist_name_idx"),

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
        # ── Regional (v8: Eurasia expansion) ──────────────────────────────
        IndexModel(
            [("regions.eastern_europe", ASCENDING)],
            sparse=True, name="regions_eastern_europe_idx",
        ),
        IndexModel(
            [("regions.south_asia", ASCENDING)],
            sparse=True, name="regions_south_asia_idx",
        ),
        IndexModel(
            [("regions.east_asia", ASCENDING)],
            sparse=True, name="regions_east_asia_idx",
        ),
        IndexModel(
            [("regions.southeast_asia", ASCENDING)],
            sparse=True, name="regions_southeast_asia_idx",
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
        IndexModel(
            [("regions.east_asia", ASCENDING), ("quality_score", DESCENDING)],
            sparse=True, name="east_asia_quality_idx",
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


async def _ensure_v3_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    """Create indexes for v3 external-source discovery collections."""

    # track_candidates — the staging area before Spotify matching
    cand_col = db[TRACK_CANDIDATES_COL]
    await cand_col.create_indexes([
        # Primary deduplication key for candidates
        IndexModel(
            [("candidate_fingerprint", ASCENDING)],
            unique=True, name="candidate_fingerprint_unique",
        ),
        # candidate_match_worker polling: unprocessed, not locked
        IndexModel(
            [("processed", ASCENDING), ("locked_at", ASCENDING)],
            name="processed_locked_idx",
        ),
        IndexModel([("source", ASCENDING)], name="source_idx"),
        IndexModel([("discovered_at", DESCENDING)], name="discovered_at_idx"),
    ])

    # lastfm_seed_queue — (tag, method) is the unique key
    lf_col = db[LASTFM_SEED_QUEUE_COL]
    await lf_col.create_indexes([
        IndexModel(
            [("tag", ASCENDING), ("method", ASCENDING)],
            unique=True, name="tag_method_unique",
        ),
        IndexModel([("status", ASCENDING)], name="status_idx"),
    ])

    # ytmusic_seed_queue — (query_type, query) is the unique key
    yt_col = db[YTMUSIC_SEED_QUEUE_COL]
    await yt_col.create_indexes([
        IndexModel(
            [("query_type", ASCENDING), ("query", ASCENDING)],
            unique=True, name="query_type_query_unique",
        ),
        IndexModel([("status", ASCENDING)], name="status_idx"),
    ])

    # discogs_seed_queue — style is the unique key (page tracked in document)
    dc_col = db[DISCOGS_SEED_QUEUE_COL]
    await dc_col.create_indexes([
        IndexModel([("style", ASCENDING)], unique=True, name="style_unique"),
        IndexModel([("status", ASCENDING)], name="status_idx"),
    ])


async def _ensure_graph_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    """Create indexes for v4 artist graph expansion collections."""

    # artist_graph_queue — artist_id is unique; sort by priority DESC
    ag_col = db[ARTIST_GRAPH_QUEUE_COL]
    await ag_col.create_indexes([
        IndexModel([("artist_id", ASCENDING)], unique=True, name="artist_id_unique"),
        # Polling: unprocessed, not locked, highest priority first
        IndexModel(
            [("processed", ASCENDING), ("locked_at", ASCENDING), ("priority", DESCENDING)],
            name="processed_locked_priority_idx",
        ),
        IndexModel([("depth", ASCENDING)], name="depth_idx"),
        IndexModel([("source", ASCENDING)], name="source_idx"),
    ])

    # artist_processed_cache — simple lookup by artist_id
    apc_col = db[ARTIST_PROCESSED_CACHE_COL]
    await apc_col.create_indexes([
        IndexModel([("artist_id", ASCENDING)], unique=True, name="artist_id_unique"),
        IndexModel([("processed_at", DESCENDING)], name="processed_at_idx"),
    ])

    # album_processed_cache — simple lookup by album_id
    alc_col = db[ALBUM_PROCESSED_CACHE_COL]
    await alc_col.create_indexes([
        IndexModel([("album_id", ASCENDING)], unique=True, name="album_id_unique"),
        IndexModel([("processed_at", DESCENDING)], name="processed_at_idx"),
    ])

    # Compound index on tracks for youtube_enrichment_worker polling
    tr_col = db[TRACKS_COL]
    try:
        await tr_col.create_indexes([
            IndexModel(
                [("youtube_searched", ASCENDING), ("status", ASCENDING)],
                name="youtube_searched_status_idx",
            ),
        ])
    except Exception:
        pass  # Index may already exist from track indexes above


async def _ensure_deezer_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    """Create indexes for v5 Deezer direct discovery queue."""
    col = db[DEEZER_SEED_QUEUE_COL]
    await col.create_indexes([
        IndexModel([("artist_id", ASCENDING)], unique=True, name="artist_id_unique"),
        IndexModel(
            [("processed", ASCENDING), ("locked_at", ASCENDING)],
            name="processed_locked_idx",
        ),
        IndexModel([("genre_id", ASCENDING)], name="genre_id_idx"),
    ])


async def _ensure_itunes_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    """Create indexes for v6 iTunes / Apple Music discovery queue."""
    col = db[ITUNES_SEED_QUEUE_COL]
    await col.create_indexes([
        IndexModel([("artist_name", ASCENDING)], unique=True, name="artist_name_unique"),
        IndexModel(
            [("processed", ASCENDING), ("locked_at", ASCENDING)],
            name="processed_locked_idx",
        ),
    ])


async def _ensure_yandex_indexes(db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
    """Create indexes for v7 Yandex Music CIS discovery queue."""
    col = db[YANDEX_SEED_QUEUE_COL]
    await col.create_indexes([
        IndexModel([("artist_id", ASCENDING)], unique=True, name="artist_id_unique"),
        IndexModel(
            [("processed", ASCENDING), ("locked_at", ASCENDING)],
            name="processed_locked_idx",
        ),
    ])
