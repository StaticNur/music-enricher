"""
Central configuration module.

All settings are loaded from environment variables (or .env file).
No global mutable state — settings are read once at startup.
"""
from __future__ import annotations

from pydantic import Field
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
    spotify_rate_limit_rps: float = Field(default=10.0)
    spotify_max_retries: int = Field(default=5)

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

    @property
    def target_regions_list(self) -> list[str]:
        """Parse comma-separated target_regions into a list."""
        return [r.strip() for r in self.target_regions.split(",") if r.strip()]


def get_settings() -> Settings:
    """Return a fresh Settings instance (reads env vars each call)."""
    return Settings()
