"""
Track candidate models.

``CandidateDocument`` is the staging area for tracks discovered from external
sources (Last.fm, YouTube Music, Discogs) before they are matched to Spotify
tracks and inserted into the main ``tracks`` collection.

Deduplication within the candidates collection is performed via
``candidate_fingerprint`` (a unique index). Duplicates against already-known
Spotify tracks are resolved by ``candidate_match_worker`` during matching.

Seed queue models for the three new discovery sources are also defined here.
Each seed queue stores pagination state so workers resume safely after crashes.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class CandidateSource(str, Enum):
    LASTFM = "lastfm"
    YTMUSIC = "ytmusic"
    DISCOGS = "discogs"


class CandidateDocument(BaseModel):
    """
    A track candidate discovered from an external source.

    Lifecycle:
        1. Discovery worker inserts with ``processed=False``.
        2. ``candidate_match_worker`` claims it (sets ``locked_at``).
        3. Worker searches Spotify, sets ``matched_spotify_id`` if found.
        4. Worker sets ``processed=True`` and clears ``locked_at``.

    ``candidate_fingerprint`` is a SHA-256 of (normalized_title | normalized_artist
    [| duration_bucket]) — enforced as a unique index to prevent duplicate candidates
    from the same or different sources.
    """

    source: CandidateSource
    title: str
    artist: str
    duration_ms: Optional[int] = None
    youtube_video_id: Optional[str] = None   # set by ytmusic_worker
    discogs_release_id: Optional[int] = None  # set by discogs_worker

    # Source-side confidence (always 1.0 for direct API results)
    confidence: float = 1.0

    # Deduplication key — unique index in MongoDB
    candidate_fingerprint: str

    processed: bool = False
    retry_count: int = 0

    # Set after successful Spotify match
    matched_spotify_id: Optional[str] = None

    # Optimistic locking
    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    discovered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        data["source"] = self.source.value
        return data


# ── Seed queue models ─────────────────────────────────────────────────────────
# Each model tracks pagination state so workers can resume after a crash.


class QueueStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


class LastFmSeedItem(BaseModel):
    """
    One (tag, method) pair in the Last.fm seed queue.

    ``page`` tracks how far we've paginated. After each successful page fetch
    the worker increments ``page`` and saves it back. When ``page`` exceeds
    ``max_pages`` or the API returns an empty result, the item is marked DONE.
    """

    tag: str            # e.g. "pop", "" for global chart
    method: str         # "tag.getTopTracks" | "chart.getTopTracks"
    page: int = 1
    max_pages: int = 200
    status: QueueStatus = QueueStatus.PENDING
    retry_count: int = 0

    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        data["status"] = self.status.value
        return data


class YtMusicSeedItem(BaseModel):
    """
    One (query_type, query) pair in the YouTube Music seed queue.

    Each item is processed exactly once — YTMusic returns a fixed result set
    per query so there is no pagination state to persist.
    """

    query_type: str    # "search" | "chart"
    query: str         # search query string or ISO-3166 country code for charts
    status: QueueStatus = QueueStatus.PENDING
    retry_count: int = 0

    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        data["status"] = self.status.value
        return data


class DiscogsSeedItem(BaseModel):
    """
    One style in the Discogs seed queue.

    ``page`` persists pagination state. The worker fetches one page per
    iteration, increments ``page``, and marks DONE when exhausted.
    """

    style: str          # e.g. "hip hop", "rock"
    page: int = 1
    per_page: int = 50
    max_pages: int = 100
    status: QueueStatus = QueueStatus.PENDING
    retry_count: int = 0

    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        data["status"] = self.status.value
        return data
