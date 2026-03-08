"""
Central configuration module.

All settings are loaded from environment variables (or .env file).
No global mutable state — settings are read once at startup.
"""
from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application-wide configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── MongoDB ──────────────────────────────────────────────────────────────
    mongodb_uri: str = Field(default="mongodb://mongo:27017")
    mongodb_db: str = Field(default="music_enricher")
    mongodb_max_pool_size: int = Field(default=20)

    # ── Spotify Web API ──────────────────────────────────────────────────────
    spotify_client_id: str = Field(default="")
    spotify_client_secret: str = Field(default="")
    spotify_rate_limit_rps: float = Field(default=3.0)
    spotify_max_retries: int = Field(default=5)
    # Maximum seconds to honor Spotify Retry-After. Spotify issues bans of
    # 20,000+ seconds when overloaded — we cap the sleep so workers don't
    # stall for hours. Tenacity's backoff handles the remaining wait.
    spotify_max_rate_limit_sleep: int = Field(default=60)
    # Circuit breaker: open after this many consecutive 429/5xx/network errors
    spotify_circuit_breaker_threshold: int = Field(default=5)
    # Circuit breaker: default seconds to wait in OPEN state before probing again
    spotify_circuit_breaker_timeout: float = Field(default=120.0)
    # Circuit breaker: max seconds to stay OPEN when Retry-After hint is provided
    spotify_circuit_breaker_max_timeout: float = Field(default=3600.0)

    # ── Genius API ───────────────────────────────────────────────────────────
    genius_access_token: str = Field(default="")
    genius_rate_limit_rps: float = Field(default=3.0)
    genius_max_retries: int = Field(default=3)
    genius_min_confidence: float = Field(default=0.6)

    # ── Worker settings ──────────────────────────────────────────────────────
    batch_size: int = Field(default=50)
    worker_sleep_sec: int = Field(default=5)
    worker_retry_limit: int = Field(default=3)

    # ── Quality filtering ────────────────────────────────────────────────────
    quality_threshold: float = Field(default=0.1)
    min_playlist_followers: int = Field(default=10_000)

    # ── Genre expansion ──────────────────────────────────────────────────────
    genre_max_offset: int = Field(default=950)  # Spotify cap: offset + limit <= 1000
    genre_batch_limit: int = Field(default=50)

    # ── Observability ────────────────────────────────────────────────────────
    stats_log_interval_min: int = Field(default=10)

    # ── Export ───────────────────────────────────────────────────────────────
    export_dir: str = Field(default="/data/exports")
    export_batch_size: int = Field(default=1000)

    # ── Seeder ───────────────────────────────────────────────────────────────
    featured_playlists_limit: int = Field(default=50)
    category_playlists_limit: int = Field(default=20)
    categories_limit: int = Field(default=50)

    # ── MusicBrainz ──────────────────────────────────────────────────────────
    musicbrainz_enabled: bool = Field(default=True)
    # Strict 1 req/s per MusicBrainz API policy
    musicbrainz_rate_limit_rps: float = Field(default=1.0)
    musicbrainz_max_retries: int = Field(default=3)
    musicbrainz_user_agent: str = Field(
        default="MusicEnricher/1.0 (https://github.com/music-enricher)"
    )
    # Only enrich tracks with quality_score > this OR mb_priority > 0
    musicbrainz_priority_threshold: float = Field(default=0.3)
    # Minimum combined confidence to store MB data
    musicbrainz_min_confidence: float = Field(default=0.75)
    # ±3 seconds duration tolerance for MB matching
    musicbrainz_duration_tolerance_ms: int = Field(default=3000)

    # ── Regional targeting ────────────────────────────────────────────────────
    # Comma-separated: cis,central_asia,mena
    target_regions: str = Field(default="cis,central_asia,mena")
    regional_boost_enabled: bool = Field(default=True)
    # Weight of regional_score in the final quality formula (0–1)
    regional_boost_weight: float = Field(default=0.15)

    # ── Regional seeding ─────────────────────────────────────────────────────
    regional_genre_max_offset: int = Field(default=500)
    regional_genre_batch_limit: int = Field(default=50)

    # ── Language detection ────────────────────────────────────────────────────
    language_detection_enabled: bool = Field(default=True)
    # Minimum text length (chars) for reliable langdetect
    language_min_text_len: int = Field(default=20)

    # ── Transliteration ───────────────────────────────────────────────────────
    transliteration_enabled: bool = Field(default=True)

    # ── Last.fm API (v3) ──────────────────────────────────────────────────────
    lastfm_api_key: str = Field(default="")
    lastfm_rate_limit_rps: float = Field(default=4.0)
    lastfm_max_retries: int = Field(default=3)
    lastfm_max_pages: int = Field(default=200)   # max pages per tag (50 tracks/page)

    # ── YouTube Music (v3) ────────────────────────────────────────────────────
    ytmusic_rate_limit_rps: float = Field(default=10.0)
    ytmusic_max_retries: int = Field(default=3)

    # ── Discogs API (v3) ──────────────────────────────────────────────────────
    discogs_token: str = Field(default="")
    # Discogs: 60 req/min authenticated = 1 req/s
    discogs_rate_limit_rps: float = Field(default=1.0)
    discogs_max_retries: int = Field(default=3)
    discogs_max_pages: int = Field(default=100)  # max pages per style

    # ── Candidate matching (v3) ───────────────────────────────────────────────
    candidate_match_confidence: float = Field(default=0.8)  # min Spotify match score

    # ── Artist graph expansion (v4) ───────────────────────────────────────────
    # Maximum BFS depth from seed artists (0 = seed only)
    artist_graph_max_depth: int = Field(default=5)
    # Artists processed per worker iteration (small: each artist = many API calls)
    artist_graph_batch_size: int = Field(default=3)
    # How many artists to seed from existing tracks on first run
    artist_graph_seed_limit: int = Field(default=500)
    # Whether artist_graph_worker also searches YTMusic for video IDs
    artist_graph_ytmusic_enabled: bool = Field(default=False)
    # Max albums from own catalog (album+single+compilation) per artist.
    # Set 0 to disable limit.
    artist_graph_max_own_albums: int = Field(default=0)
    # Max appears_on albums per artist. Popular artists can have 500–2000
    # compilation albums — without this limit the worker stalls on artist #1.
    artist_graph_max_appears_on: int = Field(default=50)

    # ── YouTube enrichment (v4) ───────────────────────────────────────────────
    # Minimum rapidfuzz confidence to accept a YTMusic search result
    ytmusic_video_match_confidence: float = Field(default=0.65)

    # ── iTunes / Apple Music (v6) ─────────────────────────────────────────────
    # Undocumented iTunes rate limit; 4 rps is safe in practice.
    itunes_rate_limit_rps: float = Field(default=4.0)
    # Artist names claimed per worker iteration (each = 1 iTunes search call)
    itunes_batch_size: int = Field(default=10)
    # Max artist names to pull from tracks on each queue seed cycle
    itunes_seed_artist_limit: int = Field(default=200_000)
    # Skip iTunes search for an artist if we already have this many tracks for them.
    # Saves API quota when the artist is already well-covered in the DB.
    itunes_skip_if_tracks_gte: int = Field(default=50)
    # After this many consecutive 403 responses, sleep before continuing.
    itunes_403_backoff_after: int = Field(default=5)
    # How long to sleep (seconds) when consecutive 403 threshold is reached.
    itunes_403_backoff_seconds: int = Field(default=300)

    # ── Feature flags ────────────────────────────────────────────────────────
    # Set to False when Spotify revokes/restricts API access.
    # candidate_match_worker will use Deezer search exclusively.
    # artist_graph_worker will become a no-op (no Spotify → no albums/tracks).
    spotify_enabled: bool = Field(default=True)

    # ── Deezer direct discovery (v5) ─────────────────────────────────────────
    # Per-instance rate limit. Deezer allows 50 req/5s = 10 rps per IP.
    # With N replicas all on same host, set this to floor(10 / N).
    # Default 3.0 → safe for up to 3 replicas (3×3=9 rps < 10 rps IP limit).
    deezer_rate_limit_rps: float = Field(default=3.0)
    # Artists processed per worker iteration
    deezer_batch_size: int = Field(default=5)
    # Top tracks fetched per artist (Deezer max = 100)
    deezer_top_tracks_limit: int = Field(default=50)
    # Whether to also crawl artist albums (more tracks but more API calls)
    deezer_crawl_albums: bool = Field(default=True)
    # Max albums crawled per artist when deezer_crawl_albums=True
    deezer_max_albums_per_artist: int = Field(default=20)

    # ── Yandex Music discovery (v7) ───────────────────────────────────────────
    # OAuth token from a free Yandex account. Worker is idle if not set.
    yandex_music_token: str = Field(default="")
    # Self-imposed rate limit — unofficial API, keep conservative.
    yandex_music_rate_limit_rps: float = Field(default=2.0)
    # Comma-separated CIS country codes to fetch charts from.
    yandex_music_chart_countries: str = Field(default="ru,kz,by,uz,am,az,ge,ua,md")

    @property
    def yandex_music_chart_countries_list(self) -> list[str]:
        return [c.strip() for c in self.yandex_music_chart_countries.split(",") if c.strip()]

    @field_validator(
        "quality_threshold",
        "genius_min_confidence",
        "candidate_match_confidence",
        "musicbrainz_min_confidence",
        "ytmusic_video_match_confidence",
        mode="before",
    )
    @classmethod
    def _validate_confidence(cls, v: float) -> float:
        v = float(v)
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"Confidence/threshold must be in [0, 1], got {v}")
        return v

    @property
    def target_regions_list(self) -> list[str]:
        """Parse comma-separated target_regions into a list."""
        return [r.strip() for r in self.target_regions.split(",") if r.strip()]


def get_settings() -> Settings:
    """Return a fresh Settings instance (reads env vars each call)."""
    return Settings()
