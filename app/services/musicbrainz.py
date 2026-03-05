"""
MusicBrainz Web Service client.

API constraints (strictly enforced by MusicBrainz):
- Rate limit: 1 request / second
- User-Agent header is REQUIRED (requests without it are rejected)
- No API key needed for read-only access

Lookup strategy:
1. If ISRC is available → ``GET /ws/2/isrc/{ISRC}`` (precise)
2. If no ISRC → ``GET /ws/2/recording?query=...`` (fuzzy text search)
3. For matched recording → ``GET /ws/2/recording/{MBID}`` with inc= flags
   to retrieve artist-rels (composers/lyricists), releases, labels

Matching validation (applied after API response):
- Title similarity (rapidfuzz token_sort_ratio) ≥ weight 0.7
- Duration within ±DURATION_TOLERANCE_MS
- Combined confidence must exceed settings.musicbrainz_min_confidence
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx
import structlog
from rapidfuzz import fuzz
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
    before_sleep_log,
)

from app.core.config import Settings
from app.utils.rate_limiter import RateLimiter
from app.utils.transliteration import normalize_text

logger = structlog.get_logger(__name__)

MB_BASE = "https://musicbrainz.org/ws/2"

# ISO 639-3 → ISO 639-1 mapping for the languages we care about
_LANG_3_TO_1: Dict[str, str] = {
    "eng": "en", "rus": "ru", "ukr": "uk", "bel": "be",
    "uzb": "uz", "kaz": "kk", "kir": "ky", "tgk": "tg", "tuk": "tk",
    "aze": "az", "arm": "hy", "geo": "ka",
    "ara": "ar", "pes": "fa", "heb": "he", "tur": "tr",
    "fra": "fr", "deu": "de", "spa": "es", "ita": "it",
    "por": "pt", "pol": "pl", "ron": "ro", "nld": "nl",
    "swe": "sv", "nor": "no", "dan": "da", "fin": "fi",
    "zho": "zh", "jpn": "ja", "kor": "ko", "hin": "hi",
    "ben": "bn", "vie": "vi", "tha": "th", "ind": "id",
    "[mul]": None,   # multiple languages
    "[zxx]": None,   # no linguistic content
}

# MusicBrainz script codes → our script names
_MB_SCRIPT_MAP: Dict[str, str] = {
    "Latn": "latin",
    "Cyrl": "cyrillic",
    "Arab": "arabic",
    "Geor": "georgian",
    "Armn": "armenian",
    "Hebr": "hebrew",
    "Deva": "devanagari",
    "Hani": "cjk",
    "Hans": "cjk",
    "Hant": "cjk",
    "Hang": "cjk",
    "Jpan": "cjk",
}

# Artist relation types that indicate composition credit
_COMPOSER_ROLES = {"composer", "co-composer", "music", "written-by"}
_LYRICIST_ROLES = {"lyricist", "co-lyricist", "lyrics", "librettist"}


class MusicBrainzError(Exception):
    def __init__(self, status: int, msg: str) -> None:
        super().__init__(f"MusicBrainz {status}: {msg}")
        self.status = status


class MusicBrainzRateLimit(Exception):
    """Raised on 503 or 429 for retry handling."""


@dataclass
class MBRecordingMatch:
    """Result of a successful MusicBrainz recording lookup."""
    mbid: str
    canonical_title: str
    aliases: List[str] = field(default_factory=list)
    composers: List[str] = field(default_factory=list)
    lyricists: List[str] = field(default_factory=list)
    first_release_date: Optional[str] = None
    release_countries: List[str] = field(default_factory=list)
    labels: List[str] = field(default_factory=list)
    language: Optional[str] = None        # ISO 639-1
    script: Optional[str] = None          # our script name
    tags: List[str] = field(default_factory=list)
    confidence: float = 0.0
    artist_country: Optional[str] = None


class MusicBrainzClient:
    """
    Async read-only MusicBrainz Web Service client.

    Rate-limited to 1 req/s. Uses tenacity for transient error retry.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._limiter = RateLimiter(
            rate=settings.musicbrainz_rate_limit_rps,
            capacity=settings.musicbrainz_rate_limit_rps,  # no burst
        )
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers={
                "User-Agent": settings.musicbrainz_user_agent,
                "Accept": "application/json",
            },
            limits=httpx.Limits(max_keepalive_connections=2, max_connections=5),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "MusicBrainzClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    # ── Low-level GET ─────────────────────────────────────────────────────────

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Rate-limited GET. Returns parsed JSON or raises."""
        await self._limiter.acquire()
        url = f"{MB_BASE}{path}"
        resp = await self._http.get(url, params={**(params or {}), "fmt": "json"})

        if resp.status_code in (429, 503):
            wait = int(resp.headers.get("Retry-After", "2"))
            logger.warning("mb_rate_limited", retry_after=wait)
            await asyncio.sleep(wait)
            raise MusicBrainzRateLimit()

        if resp.status_code == 404:
            return None

        if resp.status_code >= 400:
            raise MusicBrainzError(resp.status_code, resp.text[:200])

        return resp.json()

    def _retry(self, func):  # type: ignore[no-untyped-def]
        return retry(
            retry=retry_if_exception_type(
                (MusicBrainzRateLimit, httpx.TransportError, httpx.TimeoutException)
            ),
            stop=stop_after_attempt(self._settings.musicbrainz_max_retries),
            wait=wait_exponential_jitter(initial=2, max=60, jitter=3),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    # ── ISRC lookup ───────────────────────────────────────────────────────────

    async def lookup_by_isrc(self, isrc: str) -> Optional[List[Dict]]:
        """
        Fetch recordings associated with an ISRC.

        Returns list of minimal recording dicts (id, title, length) or None.
        """
        async def _do() -> Optional[Dict]:
            return await self._get(f"/isrc/{isrc}")
        try:
            data = await self._retry(_do)()
            if not data:
                return None
            return data.get("recordings", [])
        except (MusicBrainzError, Exception) as exc:
            logger.debug("mb_isrc_lookup_failed", isrc=isrc, error=str(exc))
            return None

    # ── Recording search ──────────────────────────────────────────────────────

    async def search_recording(
        self, track_name: str, artist_name: str, limit: int = 5
    ) -> List[Dict]:
        """
        Search MusicBrainz recordings by title + artist.

        Uses Lucene query syntax. Also attempts transliterated name if the
        original is non-Latin (to improve recall for Cyrillic/Arabic tracks).
        """
        queries = [
            f'recording:"{_lucene_escape(track_name)}" AND artist:"{_lucene_escape(artist_name)}"',
        ]

        # Normalised query as fallback
        norm_name = normalize_text(track_name)
        if norm_name != track_name.lower():
            queries.append(
                f'recording:"{_lucene_escape(norm_name)}" AND artist:"{_lucene_escape(artist_name)}"'
            )

        results: List[Dict] = []
        for q in queries:
            async def _do(query: str = q) -> Dict:
                return await self._get(
                    "/recording",
                    params={"query": query, "limit": limit},
                )
            try:
                data = await self._retry(_do)()
                if data:
                    hits = data.get("recordings", [])
                    results.extend(hits)
                    if results:
                        break   # first successful query is enough
            except Exception as exc:
                logger.debug("mb_search_failed", query=q[:80], error=str(exc))

        # Deduplicate by MBID
        seen: set[str] = set()
        unique = []
        for r in results:
            if r.get("id") and r["id"] not in seen:
                seen.add(r["id"])
                unique.append(r)
        return unique

    # ── Recording detail ──────────────────────────────────────────────────────

    async def get_recording_detail(self, mbid: str) -> Optional[Dict]:
        """
        Fetch full recording detail with relations and releases.

        inc flags:
        - artist-rels  → composers, lyricists
        - releases     → release countries, language, script
        - tags         → genre tags
        """
        async def _do() -> Optional[Dict]:
            return await self._get(
                f"/recording/{mbid}",
                params={"inc": "artist-rels+releases+tags"},
            )
        try:
            return await self._retry(_do)()
        except Exception as exc:
            logger.debug("mb_detail_failed", mbid=mbid, error=str(exc))
            return None

    # ── Artist detail ─────────────────────────────────────────────────────────

    async def get_artist_country(self, artist_mbid: str) -> Optional[str]:
        """
        Fetch artist begin-area country code.

        Returns ISO 3166-1 alpha-2 code (e.g. "UZ") or None.
        """
        async def _do() -> Optional[Dict]:
            return await self._get(
                f"/artist/{artist_mbid}",
                params={"inc": "area-rels"},
            )
        try:
            data = await self._retry(_do)()
            if not data:
                return None
            area = data.get("begin-area") or data.get("area") or {}
            iso = area.get("iso-3166-1-codes", [])
            return iso[0] if iso else None
        except Exception:
            return None

    # ── High-level: find best match ───────────────────────────────────────────

    async def find_best_match(
        self,
        track_name: str,
        artist_name: str,
        duration_ms: int,
        isrc: Optional[str] = None,
    ) -> Optional[MBRecordingMatch]:
        """
        Find the best MusicBrainz recording match for a Spotify track.

        Strategy:
        1. ISRC lookup (if available) → usually exact
        2. Text search fallback
        3. Fuzzy validation (title similarity + duration check)
        4. Detail fetch for composer/lyricist/language data
        """
        candidates: List[Dict] = []

        if isrc:
            recs = await self.lookup_by_isrc(isrc)
            if recs:
                candidates.extend(recs)

        if not candidates:
            candidates = await self.search_recording(track_name, artist_name)

        if not candidates:
            return None

        # Score each candidate
        best_candidate = None
        best_confidence = 0.0
        tol = self._settings.musicbrainz_duration_tolerance_ms

        for rec in candidates[:10]:
            conf = self._score_candidate(
                rec, track_name, artist_name, duration_ms, tol
            )
            if conf > best_confidence:
                best_confidence = conf
                best_candidate = rec

        if best_candidate is None or best_confidence < self._settings.musicbrainz_min_confidence:
            logger.debug(
                "mb_no_confident_match",
                track=track_name,
                artist=artist_name,
                best_conf=best_confidence,
            )
            return None

        # Fetch full detail
        mbid = best_candidate["id"]
        detail = await self.get_recording_detail(mbid)
        if not detail:
            return None

        match = self._parse_detail(detail, best_confidence)
        return match

    def _score_candidate(
        self,
        rec: Dict,
        track_name: str,
        artist_name: str,
        duration_ms: int,
        tol_ms: int,
    ) -> float:
        """
        Compute match confidence for a MusicBrainz recording candidate.

        Components:
        - Title similarity: 0.65 weight (token_sort_ratio handles word-order variance)
        - Artist similarity: 0.20 weight
        - Duration proximity: 0.15 weight (binary: within tolerance or not)

        MusicBrainz API score (0–100) is used as a tiebreaker modifier (±5%).
        """
        rec_title = rec.get("title", "")
        rec_artists = " ".join(
            ac.get("name", "") or ac.get("artist", {}).get("name", "")
            for ac in rec.get("artist-credit", [])
            if isinstance(ac, dict)
        )
        rec_length = rec.get("length") or 0  # MB length is in ms

        title_sim = fuzz.token_sort_ratio(
            normalize_text(track_name), normalize_text(rec_title)
        ) / 100.0

        artist_sim = fuzz.token_sort_ratio(
            normalize_text(artist_name), normalize_text(rec_artists)
        ) / 100.0

        # Duration: 1.0 if within tolerance, decays to 0.0 at 2× tolerance
        if duration_ms and rec_length:
            dur_diff = abs(duration_ms - rec_length)
            if dur_diff <= tol_ms:
                dur_score = 1.0
            elif dur_diff <= tol_ms * 2:
                dur_score = 0.5
            else:
                dur_score = 0.0
        else:
            dur_score = 0.5  # no duration info — neutral

        # MB's own relevance score as small modifier
        mb_score = (rec.get("score") or 0) / 100.0
        mb_modifier = mb_score * 0.05

        confidence = (
            0.65 * title_sim
            + 0.20 * artist_sim
            + 0.15 * dur_score
            + mb_modifier
        )
        return round(min(confidence, 1.0), 4)

    def _parse_detail(self, detail: Dict, confidence: float) -> MBRecordingMatch:
        """
        Parse a full MusicBrainz recording detail response into ``MBRecordingMatch``.
        """
        mbid = detail.get("id", "")
        canonical_title = detail.get("title", "")

        # Aliases
        aliases = [a.get("name", "") for a in detail.get("aliases", []) if a.get("name")]

        # Composers and lyricists from artist relations
        composers: List[str] = []
        lyricists: List[str] = []
        relations = detail.get("relations", [])
        for rel in relations:
            rel_type = (rel.get("type") or "").lower()
            artist = rel.get("artist") or {}
            name = artist.get("name", "")
            if not name:
                continue
            if rel_type in _COMPOSER_ROLES:
                composers.append(name)
            elif rel_type in _LYRICIST_ROLES:
                lyricists.append(name)

        # Releases: collect countries, labels, first release date, language, script
        releases = detail.get("releases", [])
        release_countries: List[str] = []
        labels: List[str] = []
        first_date: Optional[str] = None
        language: Optional[str] = None
        script: Optional[str] = None

        dates = []
        for rel in releases:
            # Countries from release-event
            for ev in rel.get("release-events", []):
                area = ev.get("area") or {}
                iso = area.get("iso-3166-1-codes", [])
                release_countries.extend(iso)
            # Direct release country
            rc = rel.get("country")
            if rc:
                release_countries.append(rc)

            # Release date
            date = rel.get("date") or rel.get("release-events", [{}])[0].get("date", "") if rel.get("release-events") else ""
            if date:
                dates.append(date)

            # Labels
            for li in rel.get("label-info", []):
                lbl = li.get("label") or {}
                lname = lbl.get("name", "")
                if lname:
                    labels.append(lname)

            # Language and script from text-representation
            tr = rel.get("text-representation", {})
            if tr.get("language") and not language:
                lang3 = tr["language"]
                language = _LANG_3_TO_1.get(lang3, lang3[:2] if lang3 else None)
            if tr.get("script") and not script:
                script = _MB_SCRIPT_MAP.get(tr["script"], tr["script"].lower())

        # Earliest release date
        if dates:
            first_date = sorted(dates)[0]

        # Tags
        tags = [t.get("name", "") for t in detail.get("tags", []) if t.get("name")]

        # Deduplicate
        release_countries = sorted(set(filter(None, release_countries)))
        labels = sorted(set(filter(None, labels)))

        return MBRecordingMatch(
            mbid=mbid,
            canonical_title=canonical_title,
            aliases=aliases[:10],
            composers=list(dict.fromkeys(composers))[:10],
            lyricists=list(dict.fromkeys(lyricists))[:10],
            first_release_date=first_date,
            release_countries=release_countries,
            labels=labels[:10],
            language=language,
            script=script,
            tags=tags[:20],
            confidence=confidence,
        )


# ── Utility ───────────────────────────────────────────────────────────────────

_LUCENE_SPECIAL = re.compile(r'([+\-&|!(){}\[\]^"~*?:\\])')


def _lucene_escape(text: str) -> str:
    """Escape special Lucene query characters."""
    return _LUCENE_SPECIAL.sub(r"\\\1", text)
