"""
Artist graph discovery models.

Used by ``artist_graph_worker`` to track the expansion queue and
per-artist / per-album processing state.

Priority scoring formula (weights from spec):
    0.4 × normalized_popularity  (Spotify 0–100 → 0–1)
    0.3 × normalized_log_followers  (log(followers) / 14 → 0–1)

Higher-priority artists are processed first, ensuring the most popular
and well-connected nodes in the graph are expanded before obscure ones.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class GraphSource(str, Enum):
    SPOTIFY = "spotify"   # discovered via Spotify related-artists or featured
    LASTFM  = "lastfm"    # discovered via Last.fm artist data
    DISCOGS = "discogs"   # discovered via Discogs
    YTMUSIC = "ytmusic"   # discovered via YouTube Music
    SEED    = "seed"      # seeded from existing tracks in DB


def compute_artist_priority(popularity: int, followers: int) -> float:
    """
    Compute a priority score in [0.0, 1.0] for queue ordering.

    log(1_000_000) ≈ 13.8 → normalise by 14 to stay within 0–1.
    Result is capped at 0.7 so there is always room for the
    0.3 * log_followers component even at max popularity.
    """
    norm_pop = min(max(popularity, 0), 100) / 100.0
    norm_fol = min(math.log1p(max(followers, 0)) / 14.0, 1.0)
    return round(0.4 * norm_pop + 0.3 * norm_fol, 4)


class ArtistGraphItem(BaseModel):
    """Queue document for ``artist_graph_queue``."""

    artist_id: str
    name: str = ""
    source: GraphSource = GraphSource.SPOTIFY

    # Graph traversal depth (0 = seed, max = artist_graph_max_depth)
    depth: int = 0

    # Higher priority → processed first
    priority: float = 0.0

    # Spotify metadata stored at enqueue time (if available)
    followers: Optional[int] = None
    popularity: Optional[int] = None

    # Worker locking — same pattern as other queues
    processed: bool = False
    retry_count: int = 0
    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        data["source"] = self.source.value
        return data
