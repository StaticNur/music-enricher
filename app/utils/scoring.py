"""
Quality scoring for tracks.

Formula (weighted sum of normalized sub-scores):

    quality_score =
        0.40 * norm(popularity,       0, 100)
        0.30 * norm(artist_followers, 0, MAX_FOLLOWERS)
        0.20 * norm(appearance_score, 0, MAX_APPEARANCES)
        0.10 * norm(markets_count,    0, 185)

All components are clamped to [0, 1] before weighting.
"""
from __future__ import annotations

from typing import Optional

# ── Normalization ceilings ────────────────────────────────────────────────────
# These are soft caps — anything above is treated as 1.0, not an error.
MAX_ARTIST_FOLLOWERS = 50_000_000   # ~Ed Sheeran / Drake level
MAX_APPEARANCES = 200               # 200 high-follower playlists is exceptional
MAX_MARKETS = 185                   # Spotify's maximum market count


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    """Clamp value to [lo, hi]."""
    return max(lo, min(hi, value))


def normalize(value: float, max_value: float) -> float:
    """Linearly normalize ``value`` to [0, 1] using ``max_value`` as ceiling."""
    if max_value <= 0:
        return 0.0
    return _clamp(value / max_value)


def compute_quality_score(
    popularity: int,
    artist_followers: int,
    appearance_score: int,
    markets_count: int,
    *,
    max_followers: int = MAX_ARTIST_FOLLOWERS,
    max_appearances: int = MAX_APPEARANCES,
    max_markets: int = MAX_MARKETS,
) -> float:
    """
    Compute a composite quality score for a track.

    Args:
        popularity: Spotify track popularity (0–100).
        artist_followers: Follower count of the primary artist.
        appearance_score: Number of distinct playlists containing the track.
        markets_count: Number of markets where the track is available.
        max_followers: Normalization ceiling for artist followers.
        max_appearances: Normalization ceiling for playlist appearances.
        max_markets: Normalization ceiling for market count.

    Returns:
        Float in [0.0, 1.0].
    """
    norm_popularity = normalize(popularity, 100)
    norm_followers = normalize(artist_followers, max_followers)
    norm_appearances = normalize(appearance_score, max_appearances)
    norm_markets = normalize(markets_count, max_markets)

    score = (
        0.40 * norm_popularity
        + 0.30 * norm_followers
        + 0.20 * norm_appearances
        + 0.10 * norm_markets
    )

    return round(_clamp(score), 6)


def compute_quality_score_with_regional(
    popularity: int,
    artist_followers: int,
    appearance_score: int,
    markets_count: int,
    regional_score: float,
    regional_boost_weight: float = 0.15,
    *,
    max_followers: int = MAX_ARTIST_FOLLOWERS,
    max_appearances: int = MAX_APPEARANCES,
    max_markets: int = MAX_MARKETS,
) -> float:
    """
    Compute quality score with regional presence boost.

    When regional_boost_enabled=True, the quality_worker calls this variant
    instead of the base ``compute_quality_score``.  The base signal weights
    are scaled down proportionally to make room for the regional component:

        quality_score =
            (1 - regional_boost_weight) * base_score
            + regional_boost_weight * regional_score

    This ensures:
    - A track with regional_score=0 gets (1 - weight) of its base score
      → slight penalty for being completely outside target regions
    - A track with regional_score=1 gets a strong boost
    - The original formula is preserved when regional_boost_weight=0

    Args:
        popularity: Spotify track popularity (0–100).
        artist_followers: Follower count of the primary artist.
        appearance_score: Number of distinct playlists containing the track.
        markets_count: Number of markets where the track is available.
        regional_score: Regional presence score from ``compute_regional_score`` [0,1].
        regional_boost_weight: Weight given to regional signal (default 0.15).
        max_followers: Normalization ceiling for artist followers.
        max_appearances: Normalization ceiling for playlist appearances.
        max_markets: Normalization ceiling for market count.

    Returns:
        Float in [0.0, 1.0].
    """
    base = compute_quality_score(
        popularity, artist_followers, appearance_score, markets_count,
        max_followers=max_followers,
        max_appearances=max_appearances,
        max_markets=max_markets,
    )
    w = _clamp(regional_boost_weight, 0.0, 1.0)
    combined = (1.0 - w) * base + w * _clamp(regional_score)
    return round(_clamp(combined), 6)
