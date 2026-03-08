"""
Track data models.

``TrackStatus`` defines the state-machine transitions (unchanged from v1).
New supplementary sub-documents added without breaking existing fields:
- ``MusicBrainzData``  — MB enrichment results (musicbrainz_worker)
- ``RegionData``       — regional classification (language_worker + MB)
New fields on ``TrackDocument`` are all optional with safe defaults,
so existing documents in MongoDB remain fully readable.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TrackStatus(str, Enum):
    """
    State machine for the enrichment pipeline.

    Transitions:
        discovered → base_collected
        base_collected → audio_features_added
        audio_features_added → lyrics_added
        lyrics_added → enriched | filtered_out
        * → failed  (after exceeding retry_count)

    Supplementary workers (language, transliteration, musicbrainz) run
    orthogonally — they do NOT change this status field. They add data
    and set their own boolean flags (language_detected, mb_enriched, etc.).
    """
    DISCOVERED = "discovered"
    BASE_COLLECTED = "base_collected"
    AUDIO_FEATURES_ADDED = "audio_features_added"
    LYRICS_ADDED = "lyrics_added"
    ENRICHED = "enriched"
    FILTERED_OUT = "filtered_out"
    FAILED = "failed"


# ── Script constants ──────────────────────────────────────────────────────────
class Script(str, Enum):
    LATIN = "latin"
    CYRILLIC = "cyrillic"
    ARABIC = "arabic"
    GEORGIAN = "georgian"
    ARMENIAN = "armenian"
    HEBREW = "hebrew"
    DEVANAGARI = "devanagari"
    CJK = "cjk"
    OTHER = "other"


# ── Embedded sub-documents ────────────────────────────────────────────────────

class ArtistRef(BaseModel):
    """Minimal artist reference embedded inside a track document."""
    spotify_id: str
    name: str


class AlbumRef(BaseModel):
    """Album metadata embedded inside a track document."""
    spotify_id: str
    name: str
    release_date: Optional[str] = None
    album_type: Optional[str] = None  # album | single | compilation
    total_tracks: Optional[int] = None
    images: List[Dict[str, Any]] = Field(default_factory=list)


class AudioFeatures(BaseModel):
    """Spotify audio analysis features for a track."""
    danceability: Optional[float] = None
    energy: Optional[float] = None
    key: Optional[int] = None
    loudness: Optional[float] = None
    mode: Optional[int] = None
    speechiness: Optional[float] = None
    acousticness: Optional[float] = None
    instrumentalness: Optional[float] = None
    liveness: Optional[float] = None
    valence: Optional[float] = None
    tempo: Optional[float] = None
    time_signature: Optional[int] = None
    duration_ms: Optional[int] = None


class LyricsData(BaseModel):
    """Lyrics data sourced from Genius."""
    text: Optional[str] = None
    language: Optional[str] = None
    genius_url: Optional[str] = None
    genius_song_id: Optional[int] = None
    confidence_score: float = 0.0
    fetched_at: Optional[datetime] = None


class YoutubeData(BaseModel):
    """YouTube video linked to this track (from ytmusic_worker or candidate_match_worker)."""
    video_id: str
    confidence: float = 1.0
    source: str = "ytmusic"  # "ytmusic" | "search" | "ytmusic_search"
    # v4: extended by youtube_enrichment_worker
    duration_ms: Optional[int] = None
    channel: Optional[str] = None


class TrackVersion(BaseModel):
    """Records each album/release on which this track appears."""
    album_id: str
    release_date: Optional[str] = None
    version_type: str = "album"  # album | single | compilation | appears_on


class MusicBrainzData(BaseModel):
    """
    MusicBrainz enrichment data.

    Populated by ``musicbrainz_worker`` using either ISRC lookup
    (high precision) or fuzzy text search (lower precision).

    ``language`` and ``script`` here come from the MB text-representation
    of the first release, which is often more accurate than langdetect
    (especially for regional/minority languages).
    """
    mbid: Optional[str] = None                   # MusicBrainz Recording ID
    canonical_title: Optional[str] = None        # MB official title
    aliases: List[str] = Field(default_factory=list)
    composers: List[str] = Field(default_factory=list)
    lyricists: List[str] = Field(default_factory=list)
    first_release_date: Optional[str] = None
    release_countries: List[str] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)
    language: Optional[str] = None               # ISO 639-1 (mapped from MB 639-3)
    script: Optional[str] = None                 # Latn | Cyrl | Arab | etc.
    tags: List[str] = Field(default_factory=list)


class RegionData(BaseModel):
    """
    Regional classification of a track.

    Populated by ``language_worker`` (from markets + language) and
    enriched by ``musicbrainz_worker`` (release_countries).

    Flags are OR-aggregated: a track is ``cis=True`` if it appears in
    CIS markets OR its language is a CIS language OR the primary artist
    is from a CIS country.

    All fields default to False / None so existing MongoDB documents
    remain fully readable without migration.
    """
    # Post-Soviet / CIS
    cis: bool = False
    central_asia: bool = False
    # Middle East & North Africa
    mena: bool = False
    # Eurasian expansion (v8)
    eastern_europe: bool = False   # PL, CZ, HU, RO, BG, RS, HR, GR, ...
    south_asia: bool = False       # IN, BD, LK, NP, PK, ...
    east_asia: bool = False        # CN, JP, KR, TW, MN, ...
    southeast_asia: bool = False   # TH, VN, ID, MY, PH, SG, ...

    countries: List[str] = Field(default_factory=list)  # ISO-3166-1 alpha-2

    # From MusicBrainz artist relations
    artist_country: Optional[str] = None
    artist_begin_area: Optional[str] = None
    artist_origin_region: Optional[str] = None  # region name string


# ── Main document ─────────────────────────────────────────────────────────────

class TrackDocument(BaseModel):
    """
    Full MongoDB document for a music track.

    ``appearance_score`` counts how many distinct playlists contain this track
    and is used as a proxy for all-time popularity.

    All fields added in v2 are Optional with safe defaults so existing
    MongoDB documents validate without migration.
    """

    # ── Identity ─────────────────────────────────────────────────────────────
    spotify_id: str
    isrc: Optional[str] = None
    fingerprint: Optional[str] = None  # hash fallback for deduplication

    # ── Core metadata ─────────────────────────────────────────────────────────
    name: str = ""
    artists: List[ArtistRef] = Field(default_factory=list)
    album: Optional[AlbumRef] = None
    popularity: int = 0
    duration_ms: int = 0
    explicit: bool = False
    markets_count: int = 0
    markets: List[str] = Field(default_factory=list)  # v2: full market code list

    # ── Enrichment data ───────────────────────────────────────────────────────
    audio_features: Optional[AudioFeatures] = None
    lyrics: Optional[LyricsData] = None
    youtube: Optional[YoutubeData] = None  # v3: linked YouTube video

    # ── MusicBrainz enrichment (v2) ───────────────────────────────────────────
    musicbrainz: Optional[MusicBrainzData] = None
    musicbrainz_confidence_score: float = 0.0
    musicbrainz_enriched: bool = False

    # ── Language & script (v2) ────────────────────────────────────────────────
    # Resolved from: lyrics → MB → langdetect on title (in that priority order)
    language: Optional[str] = None               # ISO 639-1 code
    script: Optional[str] = None                 # Script enum value
    language_source: Optional[str] = None        # "lyrics" | "musicbrainz" | "detection"
    language_detected: bool = False              # True when language_worker has run

    # ── Transliteration (v2) ─────────────────────────────────────────────────
    normalized_name: Optional[str] = None        # Stripped, lowercased
    transliterated_name: Optional[str] = None    # Cyrillic/Arabic → Latin
    normalized_artist_name: Optional[str] = None
    transliteration_done: bool = False

    # ── Regional classification (v2) ─────────────────────────────────────────
    regions: Optional[RegionData] = None

    # ── Priority for MusicBrainz queue (v2) ─────────────────────────────────
    # 3=central_asia, 2=cis, 1=mena, 0=other
    mb_priority: int = 0

    # ── Scoring ───────────────────────────────────────────────────────────────
    appearance_score: int = 0       # incremented on each playlist occurrence
    quality_score: float = 0.0
    regional_score: float = 0.0     # v2: regional presence bonus component

    # ── Artist cache for quality scoring ──────────────────────────────────────
    artist_followers: int = 0       # followers of primary artist

    # ── v4: artist graph discovery ────────────────────────────────────────────
    # All album IDs this track has been discovered on (across releases).
    # Used to track versions without creating duplicate track documents.
    version_album_ids: List[str] = Field(default_factory=list)

    # ── v4: YouTube enrichment flag ───────────────────────────────────────────
    # Set True by youtube_enrichment_worker after searching (found or not).
    youtube_searched: bool = False

    # ── State machine ─────────────────────────────────────────────────────────
    status: TrackStatus = TrackStatus.DISCOVERED
    retry_count: int = 0
    error_log: List[str] = Field(default_factory=list)

    # ── Worker locking (optimistic concurrency) ───────────────────────────────
    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None  # worker container hostname

    # ── Timestamps ────────────────────────────────────────────────────────────
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        """Serialize to a MongoDB-compatible dict."""
        data = self.model_dump(mode="python")
        data["status"] = self.status.value
        return data

    @classmethod
    def from_mongo(cls, doc: dict) -> "TrackDocument":
        """Deserialize from a raw MongoDB document dict."""
        if "_id" in doc:
            doc = dict(doc)
            doc.pop("_id", None)
        return cls.model_validate(doc)
