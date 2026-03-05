"""
Track deduplication utilities.

Deduplication uses two strategies in priority order:
1. **ISRC** — the international standard recording code, globally unique.
2. **Fingerprint** — SHA-256 hash of (normalized_name + first_artist_id + duration_bucket).
   Used when ISRC is absent (live recordings, local files, some older tracks).

The duration is bucketed to ±2 seconds so minor metadata discrepancies
(different rip lengths) do not create false duplicates.
"""
from __future__ import annotations

import hashlib
import re
from typing import Optional


_WHITESPACE_RE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    """
    Lowercase, strip, and collapse whitespace.

    Removes common parenthetical suffixes like "(Remastered)", "(Live)", etc.
    to improve matching across editions of the same recording.
    """
    # Strip remaster/live/radio edit suffixes
    text = re.sub(
        r"\s*[\(\[](remaster(ed)?|live|radio edit|explicit|clean|version|edit|mix|remix).*?[\)\]]",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = text.lower().strip()
    text = _WHITESPACE_RE.sub(" ", text)
    return text


def duration_bucket(duration_ms: int, bucket_ms: int = 2_000) -> int:
    """
    Round duration to the nearest bucket to absorb minor length differences.

    Args:
        duration_ms: Track length in milliseconds.
        bucket_ms: Bucket size in milliseconds (default 2 s).

    Returns:
        Rounded millisecond value.
    """
    return round(duration_ms / bucket_ms) * bucket_ms


def compute_fingerprint(
    name: str,
    first_artist_id: str,
    duration_ms: int,
) -> str:
    """
    Compute a deterministic fingerprint for a track.

    Args:
        name: Track name (will be normalized).
        first_artist_id: Spotify ID of the primary artist.
        duration_ms: Track duration in milliseconds.

    Returns:
        Hex-encoded SHA-256 digest (64 characters).
    """
    normalized = normalize_text(name)
    bucketed = duration_bucket(duration_ms)
    raw = f"{normalized}|{first_artist_id}|{bucketed}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def extract_isrc(track_data: dict) -> Optional[str]:
    """
    Extract ISRC from a raw Spotify track response dict.

    Args:
        track_data: Raw dict from Spotify ``/tracks/{id}`` endpoint.

    Returns:
        ISRC string or ``None`` if not present.
    """
    return track_data.get("external_ids", {}).get("isrc") or None
