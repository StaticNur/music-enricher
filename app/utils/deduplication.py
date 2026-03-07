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

# Parenthetical/bracketed edition tags to strip from titles
_EDITION_RE = re.compile(
    r"\s*[\(\[](remaster(ed)?(\s+\d{4})?|live(\s+version)?|radio\s+edit|explicit|"
    r"clean(\s+version)?|version|edit|mix|remix|original(\s+mix)?|mono|stereo|"
    r"single(\s+version)?|album(\s+version)?|extended(\s+mix)?|acoustic|"
    r"official\s+(video|audio|lyrics?|music\s+video)|lyrics?|"
    r"visuali[sz]er|hq|hd|4k|full\s+version|bonus\s+track|\d{4}\s+remaster|\d{4}).*?[\)\]]",
    flags=re.IGNORECASE,
)

# feat./ft./featuring/with anywhere in the string (including without parens at end of line)
_FEAT_RE = re.compile(
    r"\s*[\(\[]?(feat\.?|ft\.?|featuring|prod\.?\s+by)\s+[^\)\]]+[\)\]]?",
    flags=re.IGNORECASE,
)

# Leading track-number prefix: "01. ", "1 - ", "02 " etc.
_TRACK_NUM_RE = re.compile(r"^\d{1,3}[\.\-\s]\s*")

# Collapse non-alphanumeric characters to a space (keeps spaces/letters/digits)
_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)


def normalize_text(text: str) -> str:
    """
    Canonical normalization for music track/artist names.

    Steps applied in order:
    1. Strip edition tags: (Remastered), [Live], (Official Video), etc.
    2. Strip feat./ft./featuring and everything after.
    3. Strip leading track-number prefixes: "01. ", "1 - ".
    4. Lowercase.
    5. Replace punctuation with spaces.
    6. Collapse whitespace.

    Used for both fingerprint computation and fuzzy-match scoring, so it must
    produce a stable, idempotent canonical form.
    """
    text = _EDITION_RE.sub("", text)
    text = _FEAT_RE.sub("", text)
    text = _TRACK_NUM_RE.sub("", text)
    text = text.lower()
    text = _PUNCT_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
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


def compute_candidate_fingerprint(
    title: str,
    artist: str,
    duration_ms: Optional[int] = None,
) -> str:
    """
    Compute a deduplication fingerprint for an external candidate track.

    Used when we don't yet have a Spotify artist ID, so we normalize both
    title and artist name instead.

    Primary (with duration):
        sha256(normalized_title | normalized_artist | duration_bucket)
    Fallback (no duration):
        sha256(normalized_title | normalized_artist)

    Args:
        title: Track title (will be normalized).
        artist: Primary artist name (will be normalized).
        duration_ms: Duration in milliseconds, or ``None`` if unavailable.

    Returns:
        Hex-encoded SHA-256 digest (64 characters).
    """
    normalized_title = normalize_text(title)
    normalized_artist = normalize_text(artist)
    if duration_ms:
        bucketed = duration_bucket(duration_ms)
        raw = f"{normalized_title}|{normalized_artist}|{bucketed}"
    else:
        raw = f"{normalized_title}|{normalized_artist}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
