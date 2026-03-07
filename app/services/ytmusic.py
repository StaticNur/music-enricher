"""
YouTube Music API client.

Wraps ``ytmusicapi`` (a synchronous library) in an async interface by running
all blocking calls in the default thread-pool executor.

No authentication is required for public endpoints:
- ``search(filter='songs')``  — search for songs by query
- ``get_charts(country)``     — country-level top songs chart

Rate limit: 10 req/s (configurable). Each ``run_in_executor`` call counts
as one request for rate-limiting purposes.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

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


class YtMusicError(Exception):
    """Raised for unrecoverable YTMusic API errors."""


class YtMusicClient:
    """
    Async wrapper around the synchronous ``ytmusicapi.YTMusic`` client.

    A single ``YTMusic`` instance is reused across all calls. Blocking calls
    are offloaded to the thread-pool executor so the event loop is never blocked.
    """

    def __init__(self, settings: Settings) -> None:
        self._rate = settings.ytmusic_rate_limit_rps
        self._max_retries = settings.ytmusic_max_retries
        self._limiter = RateLimiter(rate=self._rate, capacity=self._rate * 2)
        self._yt: Optional[Any] = None  # ytmusicapi.YTMusic instance
        self._init_lock = asyncio.Lock()  # guards lazy init against concurrent callers

    async def _ensure_client(self) -> None:
        """
        Lazily initialise the ytmusicapi client exactly once, even when multiple
        coroutines call it concurrently on the first iteration.
        """
        if self._yt is not None:
            return
        async with self._init_lock:
            if self._yt is not None:  # re-check inside lock (double-checked locking)
                return
            try:
                from ytmusicapi import YTMusic  # type: ignore[import-untyped]
                loop = asyncio.get_running_loop()
                self._yt = await loop.run_in_executor(None, YTMusic)
                logger.info("ytmusic_client_initialized")
            except Exception as exc:
                logger.error("ytmusic_client_init_failed", error=str(exc))
                raise YtMusicError(f"YTMusic init failed: {exc}") from exc

    def _init_client(self) -> None:
        """Kept for backwards compatibility; prefer ``_ensure_client()``."""
        if self._yt is None:
            try:
                from ytmusicapi import YTMusic  # type: ignore[import-untyped]
                self._yt = YTMusic()
                logger.info("ytmusic_client_initialized")
            except Exception as exc:
                logger.error("ytmusic_client_init_failed", error=str(exc))
                raise YtMusicError(f"YTMusic init failed: {exc}") from exc

    def _retry(self, func):  # type: ignore[no-untyped-def]
        return retry(
            retry=retry_if_exception_type(Exception),
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(initial=2, max=30, jitter=2),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    async def _run(self, fn, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        """
        Run a synchronous ytmusicapi call in the thread-pool executor.

        Applies rate limiting before dispatching.
        Uses ``get_running_loop()`` (Python 3.10+ preferred API).
        """
        await self._limiter.acquire()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    # ── Public API methods ─────────────────────────────────────────────────────

    async def search_songs(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Search YouTube Music for songs matching ``query``.

        Returns a list of raw ytmusicapi result dicts.
        Empty list on any error so the worker can continue.
        """
        await self._ensure_client()

        async def _fetch() -> List[Dict[str, Any]]:
            assert self._yt is not None
            results = await self._run(
                self._yt.search, query, filter="songs", limit=limit
            )
            return results or []

        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.warning("ytmusic_search_failed", query=query, error=str(exc))
            return []

    async def search_track_video(
        self, artist: str, title: str, limit: int = 5
    ) -> Optional[Dict[str, Any]]:
        """
        Search YouTube Music for a specific track and return the best raw result.

        Query: ``"{artist} {title} official audio"`` with ``filter="songs"``
        to prefer music content over general videos.

        Returns the first (best-ranked) result dict from ytmusicapi, or
        ``None`` if nothing was found or an error occurred.

        The caller is responsible for confidence-scoring the result.
        """
        await self._ensure_client()
        query = f"{artist} {title} official audio"

        async def _fetch() -> List[Dict[str, Any]]:
            assert self._yt is not None
            results = await self._run(
                self._yt.search, query, filter="songs", limit=limit
            )
            return results or []

        try:
            results = await self._retry(_fetch)()
            return results[0] if results else None
        except Exception as exc:
            logger.warning(
                "ytmusic_track_video_search_failed",
                artist=artist, title=title, error=str(exc),
            )
            return None

    async def get_charts(self, country: str = "US") -> Dict[str, Any]:
        """
        Fetch the top-songs chart for a given country.

        Returns the raw ytmusicapi chart dict, or ``{}`` on error.
        """
        await self._ensure_client()

        async def _fetch() -> Dict[str, Any]:
            assert self._yt is not None
            charts = await self._run(self._yt.get_charts, country=country)
            return charts or {}

        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.warning("ytmusic_charts_failed", country=country, error=str(exc))
            return {}

    # ── Static parsing helpers ─────────────────────────────────────────────────

    @staticmethod
    def parse_song(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Normalise a raw ytmusicapi song result to
        ``{title, artist, duration_ms, youtube_video_id}``.

        Returns ``None`` if required fields are missing.
        """
        title: str = (raw.get("title") or "").strip()
        if not title:
            return None

        # Artist can be a list of dicts or a string
        artists_raw = raw.get("artists") or []
        if isinstance(artists_raw, list) and artists_raw:
            first = artists_raw[0]
            artist_name = (
                first.get("name", "") if isinstance(first, dict) else str(first)
            ).strip()
        else:
            artist_name = ""

        if not artist_name:
            return None

        video_id: Optional[str] = raw.get("videoId") or None

        # ytmusicapi provides duration as "M:SS" string or duration_seconds int
        duration_ms: Optional[int] = None
        dur_sec = raw.get("duration_seconds")
        if dur_sec is not None:
            try:
                duration_ms = int(dur_sec) * 1000
            except (TypeError, ValueError):
                pass

        if duration_ms is None:
            dur_str = raw.get("duration") or ""
            duration_ms = _parse_duration_string(dur_str)

        return {
            "title": title,
            "artist": artist_name,
            "duration_ms": duration_ms,
            "youtube_video_id": video_id,
        }

    @staticmethod
    def extract_chart_songs(charts: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Pull the song list out of a ``get_charts`` response.

        ytmusicapi may structure charts differently across API versions;
        this handles the two most common shapes gracefully.
        """
        songs_section = charts.get("songs") or charts.get("Songs") or {}
        items = songs_section.get("items") or songs_section.get("content") or []
        if not isinstance(items, list):
            return []
        return items


def _parse_duration_string(duration: str) -> Optional[int]:
    """Parse 'M:SS' or 'H:MM:SS' into milliseconds. Returns ``None`` on failure."""
    if not duration:
        return None
    try:
        parts = [int(p) for p in duration.split(":")]
        if len(parts) == 2:
            return (parts[0] * 60 + parts[1]) * 1000
        if len(parts) == 3:
            return (parts[0] * 3600 + parts[1] * 60 + parts[2]) * 1000
    except (ValueError, IndexError):
        pass
    return None
