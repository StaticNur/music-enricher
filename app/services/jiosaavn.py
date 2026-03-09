"""
JioSaavn API client (v9).

Uses JioSaavn's public API — no authentication required.
Covers Bollywood + 9 Indian regional languages (Hindi, Punjabi, Tamil,
Telugu, Bengali, Kannada, Malayalam, Marathi, Gujarati, Odia).

Endpoints:
  Charts: GET https://www.jiosaavn.com/api.php?__call=content.getCharts&api_version=4&_format=json&_marker=0&ctx=wap6dot0
  Playlist: GET https://www.jiosaavn.com/api.php?__call=playlist.getDetails&_format=json&_marker=0&api_version=4&ctx=wap6dot0&listid={list_id}
  Search: GET https://www.jiosaavn.com/api.php?__call=search.getResults&q={q}&p={page}&n=50&_format=json&_marker=0&api_version=4&ctx=wap6dot0

Rate limit: self-imposed 3 rps (no documented limit).

Track data:
  - id: JioSaavn song ID → placeholder "jiosaavn:{id}"
  - title: song name
  - more_info.primary_artists: comma-separated artist names (use first)
  - more_info.duration: seconds as string → duration_ms
  - more_info.language: "hindi", "punjabi", "tamil", etc.
  - No ISRC available → fingerprint dedup only
"""
from __future__ import annotations

import asyncio
import html
import logging
from typing import Any, Dict, List, Optional

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
    before_sleep_log,
)

from app.core.config import Settings
from app.utils.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)

_BASE_URL = "https://www.jiosaavn.com/api.php"

_CHART_PARAMS: Dict[str, str] = {
    "__call": "content.getCharts",
    "api_version": "4",
    "_format": "json",
    "_marker": "0",
    "ctx": "wap6dot0",
}


class JioSaavnError(Exception):
    """Raised for non-retryable JioSaavn API errors."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"JioSaavn {status}: {message}")
        self.status = status


class JioSaavnClient:
    """
    Async JioSaavn API client with rate limiting and retry.

    Manages its own httpx.AsyncClient — call ``aclose()`` when done.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        rate = settings.jiosaavn_rate_limit_rps
        self._limiter = RateLimiter(rate=rate, capacity=rate)
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.jiosaavn.com/",
            },
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    @staticmethod
    def _unescape(text: str) -> str:
        """Unescape HTML entities that JioSaavn embeds in text fields."""
        return html.unescape(text or "").strip()

    async def _get(self, params: Dict[str, Any]) -> Any:
        """Internal GET with rate limiting and retries."""
        await self._limiter.acquire()

        @retry(
            retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
            stop=stop_after_attempt(3),
            wait=wait_exponential_jitter(initial=2, max=30, jitter=2),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        async def _fetch() -> Any:
            resp = await self._http.get(_BASE_URL, params=params)

            if resp.status_code == 403:
                logger.warning("jiosaavn_rate_limited", sleeping_for=60)
                await asyncio.sleep(60)
                raise httpx.TransportError("Rate limited (403)")

            if resp.status_code >= 500:
                raise httpx.TransportError(f"JioSaavn server error {resp.status_code}")

            if not resp.is_success:
                raise JioSaavnError(resp.status_code, resp.text[:100])

            try:
                data = resp.json()
            except Exception:
                raise JioSaavnError(0, "Invalid JSON response")

            return data

        return await _fetch()

    # ── Charts ────────────────────────────────────────────────────────────────

    async def get_charts(self) -> List[Dict[str, Any]]:
        """
        Return a list of JioSaavn chart playlists.

        Each item is normalized to ``{id, title}``.
        """
        try:
            data = await self._get(_CHART_PARAMS)
        except Exception as exc:
            logger.error("jiosaavn_get_charts_failed", error=str(exc))
            return []

        if not isinstance(data, list):
            logger.warning("jiosaavn_unexpected_charts_response", type=type(data).__name__)
            return []

        charts: List[Dict[str, Any]] = []
        for item in data:
            list_id = item.get("listid") or item.get("id") or ""
            title = self._unescape(item.get("title") or item.get("listname") or "")
            if list_id and title:
                charts.append({"id": str(list_id), "title": title})

        return charts

    # ── Playlist songs ────────────────────────────────────────────────────────

    async def get_playlist_songs(self, list_id: str, n: int = 100) -> List[Dict[str, Any]]:
        """
        Return songs from a JioSaavn chart playlist.

        Each song is normalized to ``{id, title, artist, duration_ms, language}``.
        """
        params = {
            "__call": "playlist.getDetails",
            "_format": "json",
            "_marker": "0",
            "api_version": "4",
            "ctx": "wap6dot0",
            "listid": list_id,
            "n": min(n, 500),
        }
        try:
            data = await self._get(params)
        except Exception as exc:
            logger.error("jiosaavn_get_playlist_failed", list_id=list_id, error=str(exc))
            return []

        inner = data.get("data") or {}
        if isinstance(inner, str):
            import json as _json
            try:
                inner = _json.loads(inner)
            except Exception:
                inner = {}

        songs = (
            data.get("songs")
            or data.get("list")
            or inner.get("songs")
            or inner.get("list")
            or inner.get("tracks")
            or []
        )
        # Newer JioSaavn API may return list as a JSON-encoded string
        if isinstance(songs, str):
            import json as _json
            try:
                songs = _json.loads(songs)
            except Exception:
                songs = []
        if not isinstance(songs, list):
            songs = []

        if not songs:
            logger.warning(
                "jiosaavn_playlist_parse_failed",
                list_id=list_id,
                top_keys=list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                inner_keys=list(inner.keys()) if isinstance(inner, dict) else None,
                sample=str(data)[:300],
            )

        return [t for t in (self._parse_song(s) for s in songs) if t]

    # ── Search ────────────────────────────────────────────────────────────────

    async def search_songs(
        self, query: str, page: int = 1, n: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search for songs on JioSaavn.

        Returns up to ``n`` results for ``query`` at page ``page``.
        Each song is normalized to ``{id, title, artist, duration_ms, language}``.
        """
        params = {
            "__call": "search.getResults",
            "q": query,
            "p": page,
            "n": min(n, 50),
            "_format": "json",
            "_marker": "0",
            "api_version": "4",
            "ctx": "wap6dot0",
        }
        try:
            data = await self._get(params)
        except Exception as exc:
            logger.error("jiosaavn_search_failed", query=query, page=page, error=str(exc))
            return []

        # Search response: {"results": [...], "total": N}
        # Newer API may wrap in {"data": {"results": [...]}} or use "songs" key
        inner = data.get("data") or {}
        if isinstance(inner, str):
            import json as _json
            try:
                inner = _json.loads(inner)
            except Exception:
                inner = {}

        results = (
            data.get("results")
            or data.get("songs")
            or inner.get("results")
            or inner.get("songs")
            or inner.get("tracks")
            or []
        )
        if isinstance(results, str):
            import json as _json
            try:
                results = _json.loads(results)
            except Exception:
                results = []
        if not isinstance(results, list):
            results = []

        if not results:
            logger.warning(
                "jiosaavn_search_parse_failed",
                query=query,
                page=page,
                top_keys=list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                inner_keys=list(inner.keys()) if isinstance(inner, dict) else None,
                sample=str(data)[:300],
            )

        return [t for t in (self._parse_song(s) for s in results) if t]

    # ── Parsing ───────────────────────────────────────────────────────────────

    def _parse_song(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a raw JioSaavn song object into a normalized dict.

        Returns None if required fields are missing.
        """
        song_id = str(raw.get("id") or raw.get("song_id") or "").strip()
        title = self._unescape(raw.get("title") or raw.get("song") or "")

        if not song_id or not title:
            return None

        more_info: Dict[str, Any] = raw.get("more_info") or {}

        # Primary artists (comma-separated string; use first one)
        primary_artists_raw = (
            more_info.get("primary_artists")
            or more_info.get("artist")
            or raw.get("primary_artists")
            or raw.get("artist")
            or ""
        )
        primary_artists_str = self._unescape(str(primary_artists_raw))
        # Take the first artist (split on comma, strip, filter empty)
        parts = [p.strip() for p in primary_artists_str.split(",") if p.strip()]
        artist = parts[0] if parts else ""
        if not artist:
            return None

        # Duration: stored as string seconds in more_info
        duration_str = str(more_info.get("duration") or raw.get("duration") or "0")
        try:
            duration_ms = int(duration_str) * 1000
        except (ValueError, TypeError):
            duration_ms = 0

        # Language (optional)
        language = (
            more_info.get("language")
            or raw.get("language")
            or ""
        )
        if isinstance(language, str):
            language = language.lower().strip()
        else:
            language = ""

        return {
            "id": song_id,
            "title": title,
            "artist": artist,
            "duration_ms": duration_ms,
            "language": language,
        }
