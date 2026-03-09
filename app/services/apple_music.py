"""
Apple Music / iTunes discovery client.

Two data sources — both free, no authentication required:

1. **iTunes Search API** — search all songs by an artist name.
   Returns up to 200 tracks per query, includes duration, album, artwork.
   Undocumented rate limit; 8 rps sustained is safe in practice.
   Endpoint: https://itunes.apple.com/search

2. **Apple Music RSS Feeds** — top chart songs per country.
   Updated regularly, no rate limit, JSON response.
   Endpoint: https://rss.marketingtools.apple.com/api/v2/{country}/music/{chart}/100/songs.json

Neither source provides ISRC. Deduplication falls back to
compute_candidate_fingerprint (title + artist + duration bucket).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List

import httpx
import structlog
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.core.config import Settings
from app.utils.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)

ITUNES_SEARCH_URL = "https://itunes.apple.com/search"
APPLE_RSS_BASE = "https://rss.marketingtools.apple.com/api/v2"

# Chart types available via Apple RSS
CHART_TYPES = ["most-played", "hot-tracks", "new-music"]

# Countries to pull Apple Music RSS charts from — full Eurasian coverage.
CHART_COUNTRIES = [
    # Global majors
    "us", "gb", "de", "fr", "au", "ca", "mx", "br", "za",
    # CIS
    "ru", "ua", "kz", "by", "uz", "ge", "am", "az", "md",
    # Central Asia
    "tj", "kg",
    # MENA
    "sa", "ae", "eg", "ma", "dz", "iq", "tn", "lb", "qa", "kw", "tr", "ir",
    # Eastern Europe
    "pl", "ro", "hu", "cz", "sk", "bg", "rs", "hr", "gr",
    # South Asia
    "in", "bd", "lk", "np", "pk",
    # East Asia
    "jp", "kr", "tw", "hk", "cn", "mn",
    # Southeast Asia
    "id", "th", "ph", "vn", "my", "sg", "mm",
]


class AppleMusicClient:
    """
    Async iTunes Search API + Apple Music RSS client.

    Manages its own httpx.AsyncClient — call ``aclose()`` when done.
    """

    def __init__(self, settings: Settings) -> None:
        self._limiter = RateLimiter(
            rate=settings.itunes_rate_limit_rps,
            capacity=settings.itunes_rate_limit_rps,
        )
        self._403_backoff_after = settings.itunes_403_backoff_after
        self._403_backoff_seconds = settings.itunes_403_backoff_seconds
        self._consecutive_403s = 0
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
                "Accept": "application/json"
            },
            follow_redirects=True,  # Apple RSS feeds use 301 redirects
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    def _retry(self, func):  # type: ignore[no-untyped-def]
        return retry(
            retry=retry_if_exception_type(
                (httpx.TransportError, httpx.TimeoutException)
            ),
            stop=stop_after_attempt(3),
            wait=wait_exponential_jitter(initial=2, max=30, jitter=2),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    # ── iTunes Search API ─────────────────────────────────────────────────────

    async def search_artist_tracks(
        self,
        artist_name: str,
        limit: int = 200,
        country: str = "us",
    ) -> List[Dict[str, Any]]:
        """
        Search all songs by ``artist_name`` on iTunes.

        Returns up to ``limit`` track dicts with keys: trackId, trackName,
        artistName, collectionName, trackTimeMillis, trackExplicitness,
        artworkUrl100, releaseDate, artistId, collectionId.
        """
        await self._limiter.acquire()

        async def _fetch() -> List[Dict[str, Any]]:
            resp = await self._http.get(
                ITUNES_SEARCH_URL,
                params={
                    "term": artist_name,
                    "entity": "song",
                    "media": "music",
                    "limit": min(limit, 200),
                    "country": country,
                },
            )
            if resp.status_code == 429:
                logger.warning("itunes_rate_limited", sleeping_for=15)
                await asyncio.sleep(15)
                raise httpx.TransportError("Rate limited")
            if resp.status_code == 403:
                self._consecutive_403s += 1
                if self._consecutive_403s >= self._403_backoff_after:
                    logger.warning(
                        "itunes_403_backoff",
                        consecutive=self._consecutive_403s,
                        sleeping_for=self._403_backoff_seconds,
                    )
                    await asyncio.sleep(self._403_backoff_seconds)
                    self._consecutive_403s = 0
                return []
            self._consecutive_403s = 0
            resp.raise_for_status()
            return [
                r for r in resp.json().get("results", [])
                if r.get("wrapperType") == "track" and r.get("kind") == "song"
            ]

        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.warning(
                "itunes_search_failed", artist=artist_name, error=str(exc)
            )
            return []

    async def search_tracks(
        self,
        query: str,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Search iTunes for tracks matching a combined artist+title query.

        Used by candidate_match_worker as a fallback source.
        Returns track dicts with keys: trackId, trackName, artistName,
        artistId, trackTimeMillis, trackExplicitness.
        """
        await self._limiter.acquire()

        async def _fetch() -> List[Dict[str, Any]]:
            resp = await self._http.get(
                ITUNES_SEARCH_URL,
                params={
                    "term": query,
                    "entity": "song",
                    "media": "music",
                    "limit": min(limit, 25),
                },
            )
            if resp.status_code in (403, 429):
                return []
            if not resp.is_success:
                return []
            return [
                r for r in resp.json().get("results", [])
                if r.get("wrapperType") == "track" and r.get("kind") == "song"
            ]

        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.debug("itunes_track_search_failed", query=query, error=str(exc))
            return []

    # ── Apple Music RSS Charts ────────────────────────────────────────────────

    async def get_rss_chart(
        self,
        country: str,
        chart_type: str = "most-played",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch Apple Music RSS chart for a country (no rate limit applied).

        Returns track dicts with keys: id, name, artistName, releaseDate,
        artworkUrl100, url.  No ISRC or duration.
        """
        url = f"{APPLE_RSS_BASE}/{country}/music/{chart_type}/{limit}/songs.json"

        async def _fetch() -> List[Dict[str, Any]]:
            resp = await self._http.get(url)
            if not resp.is_success:
                return []
            return resp.json().get("feed", {}).get("results", [])

        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.debug(
                "apple_rss_failed",
                country=country,
                chart_type=chart_type,
                error=str(exc),
            )
            return []
