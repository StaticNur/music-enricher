"""
Shazam Chart API client.

Uses Shazam's public discovery endpoint — no authentication required.
Returns up to 200 chart tracks per country with title, artist, and ISRC.

Endpoint:
    GET https://www.shazam.com/discovery/v5/en-US/{COUNTRY}/web/-/tracks/top-200-tracks

Rate limit: conservative 1–2 rps (undocumented public API).

Track data extracted:
  - title, artist (from heading.title / heading.subtitle)
  - ISRC (from stores.apple.isrc — available in ~95% of chart tracks)
  - shazam_key (Shazam's internal track ID, used as placeholder)
  - apple_track_id (iTunes track ID when available — for cross-referencing)
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

_BASE_URL = "https://www.shazam.com"
_CHART_PATH = "/shazam/v2/charts/track"

class ShazamClient:
    """
    Async Shazam chart API client.

    Fetches top-200 chart tracks per country from Shazam's public discovery API.
    Manages its own httpx.AsyncClient — call ``aclose()`` when done.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        rate = settings.shazam_rate_limit_rps
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
                "Referer": "https://www.shazam.com/",
            },
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def get_chart_tracks(self, country_code: str) -> List[Dict[str, Any]]:
        """
        Fetch top chart tracks for a country.

        Returns a list of dicts with keys:
          title, artist, isrc, shazam_key, apple_track_id
        Returns empty list on 404 (country not supported) or any error.
        """
        url = _BASE_URL + _CHART_PATH
        try:
            return await self._fetch_chart(url, country_code)
        except Exception as exc:
            logger.error(
                "shazam_chart_error",
                country=country_code,
                error=str(exc),
            )
            return []

    @retry(
        retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=2, max=30, jitter=2),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def _fetch_chart(
        self, url: str, country_code: str
    ) -> List[Dict[str, Any]]:
        await self._limiter.acquire()

        resp = await self._http.get(
            url,
            params={
                "locale": "en-US",
                "countryCode": country_code.upper(),
                "limit": 200,
            },
        )

        if resp.status_code == 404:
            logger.debug("shazam_country_not_supported", country=country_code)
            return []

        if resp.status_code == 429:
            logger.warning("shazam_rate_limited", country=country_code)
            await asyncio.sleep(60)
            raise httpx.TransportError("Rate limited — retrying")

        if resp.status_code >= 500:
            raise httpx.TransportError(
                f"Shazam server error {resp.status_code}"
            )

        if not resp.is_success:
            logger.warning(
                "shazam_unexpected_status",
                country=country_code,
                status=resp.status_code,
            )
            return []

        try:
            data = resp.json()
        except Exception:
            logger.warning("shazam_invalid_json", country=country_code)
            return []

        return self._parse_chart(data, country_code)

    def _parse_chart(
        self, data: Dict[str, Any], country_code: str
    ) -> List[Dict[str, Any]]:
        """Parse Shazam chart JSON into normalized track list."""
        # Shazam returns either {"chart": [...]} or {"tracks": {"hits": [...]}}
        chart_items: List[Any] = (
            data.get("chart")
            or data.get("tracks", {}).get("hits", [])
            or []
        )

        tracks = []
        for item in chart_items:
            track = self._parse_item(item)
            if track:
                tracks.append(track)

        return tracks

    def _parse_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a single Shazam chart entry. Returns None if required fields absent."""
        # Some responses wrap the track under a "track" key
        track_obj = item.get("track") or item

        heading = track_obj.get("heading") or {}
        title = (heading.get("title") or "").strip()
        artist = (heading.get("subtitle") or "").strip()

        if not title or not artist:
            return None

        stores = track_obj.get("stores") or {}
        apple = stores.get("apple") or {}

        isrc: Optional[str] = (apple.get("isrc") or "").strip() or None
        apple_track_id: Optional[str] = (
            str(apple.get("trackid") or "").strip() or None
        )
        shazam_key: str = str(track_obj.get("key") or item.get("key") or "")

        if not shazam_key:
            return None

        return {
            "title": title,
            "artist": artist,
            "isrc": isrc,
            "shazam_key": shazam_key,
            "apple_track_id": apple_track_id,
        }
