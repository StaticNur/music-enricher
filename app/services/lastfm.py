"""
Last.fm API client.

Uses the Last.fm REST API directly via httpx (no third-party Last.fm library)
so we stay fully async and consistent with the rest of the codebase.

Implemented endpoints:
- ``tag.getTopTracks``   — top tracks for a genre tag, paginated
- ``chart.getTopTracks`` — global weekly chart, paginated

Rate limit: 4 req/s (configurable via ``LASTFM_RATE_LIMIT_RPS``).
All calls are retried with exponential backoff on transient errors.
"""
from __future__ import annotations

import asyncio
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

LASTFM_BASE_URL = "https://ws.audioscrobbler.com/2.0/"


class LastFmError(Exception):
    """Raised for non-retryable Last.fm API errors."""
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"LastFm {status}: {message}")
        self.status = status


class LastFmRateLimit(Exception):
    """Raised on 429 — tenacity will retry."""


class LastFmClient:
    """
    Async Last.fm REST API client.

    Manages its own httpx.AsyncClient lifecycle — use as async context manager
    or call ``aclose()`` explicitly.
    """

    def __init__(self, settings: Settings) -> None:
        self._api_key = settings.lastfm_api_key
        self._max_retries = settings.lastfm_max_retries
        self._limiter = RateLimiter(
            rate=settings.lastfm_rate_limit_rps,
            capacity=settings.lastfm_rate_limit_rps * 2,
        )
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "LastFmClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    def _retry(self, func):  # type: ignore[no-untyped-def]
        return retry(
            retry=retry_if_exception_type(
                (LastFmRateLimit, httpx.TransportError, httpx.TimeoutException)
            ),
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(initial=2, max=60, jitter=3),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    async def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make a rate-limited GET request to the Last.fm API."""
        await self._limiter.acquire()
        params = {**params, "api_key": self._api_key, "format": "json"}
        resp = await self._http.get(LASTFM_BASE_URL, params=params)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "5"))
            logger.warning("lastfm_rate_limited", retry_after=retry_after)
            await asyncio.sleep(retry_after)
            raise LastFmRateLimit()

        if resp.status_code >= 500:
            raise LastFmError(resp.status_code, f"Server error: {resp.text[:200]}")

        if not resp.is_success:
            raise LastFmError(resp.status_code, resp.text[:200])

        data = resp.json()
        # Last.fm returns 200 with an error body for bad API keys etc.
        if "error" in data:
            error_code = data.get("error", 0)
            message = data.get("message", "unknown error")
            # error 29 = rate limit exceeded
            if error_code == 29:
                await asyncio.sleep(5)
                raise LastFmRateLimit()
            raise LastFmError(error_code, message)

        return data

    # ── Public API methods ─────────────────────────────────────────────────────

    async def get_tag_top_tracks(
        self, tag: str, page: int = 1, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Fetch top tracks for a Last.fm genre tag.

        Returns a list of track dicts with ``name`` and ``artist.name``.
        Empty list if no results or on error.
        """
        async def _fetch() -> Dict[str, Any]:
            return await self._get({
                "method": "tag.getTopTracks",
                "tag": tag,
                "page": page,
                "limit": min(limit, 1000),
            })

        try:
            data = await self._retry(_fetch)()
            tracks = data.get("tracks", {}).get("track", [])
            # API may return a single dict instead of list when only 1 result
            if isinstance(tracks, dict):
                tracks = [tracks]
            return tracks
        except Exception as exc:
            logger.warning(
                "lastfm_tag_tracks_failed",
                tag=tag, page=page, error=str(exc),
            )
            return []

    async def get_chart_top_tracks(
        self, page: int = 1, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Fetch the global weekly top tracks chart.

        Returns a list of track dicts with ``name`` and ``artist.name``.
        """
        async def _fetch() -> Dict[str, Any]:
            return await self._get({
                "method": "chart.getTopTracks",
                "page": page,
                "limit": min(limit, 1000),
            })

        try:
            data = await self._retry(_fetch)()
            tracks = data.get("tracks", {}).get("track", [])
            if isinstance(tracks, dict):
                tracks = [tracks]
            return tracks
        except Exception as exc:
            logger.warning(
                "lastfm_chart_tracks_failed",
                page=page, error=str(exc),
            )
            return []

    @staticmethod
    def parse_track(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Normalise a raw Last.fm track dict to ``{title, artist, duration_ms}``.

        Returns ``None`` if required fields are missing.
        """
        name: str = (raw.get("name") or "").strip()
        # artist may be a dict or a plain string depending on the endpoint
        artist_raw = raw.get("artist") or {}
        if isinstance(artist_raw, str):
            artist_name = artist_raw.strip()
        else:
            artist_name = (artist_raw.get("name") or "").strip()

        if not name or not artist_name:
            return None

        # Duration is given in seconds by Last.fm
        duration_sec = raw.get("duration")
        duration_ms: Optional[int] = None
        try:
            duration_ms = int(duration_sec) * 1000 if duration_sec else None
        except (TypeError, ValueError):
            pass

        return {"title": name, "artist": artist_name, "duration_ms": duration_ms}
