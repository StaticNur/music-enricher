"""
Discogs API client.

Uses the Discogs REST API via httpx (no third-party Discogs library) to keep
the codebase fully async and consistent.

Rate limit: 60 req/min authenticated = **1 req/s** (enforced via token bucket).
This is a hard limit — exceeding it will result in a 429 ban period.
The ``discogs_worker`` must run with exactly **one** replica.

Implemented endpoints:
- ``database/search?type=release&style=STYLE`` — search releases by style
- ``releases/{id}``                            — get release tracklist

Authentication: User-Token (``Authorization: Discogs token=TOKEN``).
Without a token the rate limit drops to 25 req/min. Set ``DISCOGS_TOKEN``.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

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

DISCOGS_BASE_URL = "https://api.discogs.com"
DISCOGS_USER_AGENT = "MusicEnricher/3.0 +https://github.com/music-enricher"


class DiscogsError(Exception):
    """Raised for non-retryable Discogs API errors."""
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"Discogs {status}: {message}")
        self.status = status


class DiscogsRateLimit(Exception):
    """Raised on 429 — tenacity will retry."""


class DiscogsClient:
    """
    Async Discogs REST API client.

    Manages its own httpx.AsyncClient lifecycle — use as async context manager
    or call ``aclose()`` explicitly.
    """

    def __init__(self, settings: Settings) -> None:
        self._token = settings.discogs_token
        self._max_retries = settings.discogs_max_retries
        self._limiter = RateLimiter(
            rate=settings.discogs_rate_limit_rps,
            capacity=settings.discogs_rate_limit_rps,
        )
        headers: Dict[str, str] = {"User-Agent": DISCOGS_USER_AGENT}
        if self._token:
            headers["Authorization"] = f"Discogs token={self._token}"
        self._http = httpx.AsyncClient(
            base_url=DISCOGS_BASE_URL,
            headers=headers,
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "DiscogsClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    def _retry(self, func):  # type: ignore[no-untyped-def]
        return retry(
            retry=retry_if_exception_type(
                (DiscogsRateLimit, httpx.TransportError, httpx.TimeoutException)
            ),
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(initial=5, max=120, jitter=5),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Make a rate-limited GET request to the Discogs API."""
        await self._limiter.acquire()
        resp = await self._http.get(path, params=params)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "60"))
            logger.warning("discogs_rate_limited", retry_after=retry_after)
            await asyncio.sleep(retry_after)
            raise DiscogsRateLimit()

        if resp.status_code == 404:
            raise DiscogsError(404, f"Not found: {path}")

        if resp.status_code >= 500:
            raise DiscogsError(resp.status_code, f"Server error: {resp.text[:200]}")

        if not resp.is_success:
            raise DiscogsError(resp.status_code, resp.text[:200])

        return resp.json()

    # ── Public API methods ─────────────────────────────────────────────────────

    async def search_releases(
        self, style: str, page: int = 1, per_page: int = 50
    ) -> Dict[str, Any]:
        """
        Search Discogs for releases by genre style.

        Returns the full API response including ``results`` list and
        ``pagination`` metadata. Returns ``{}`` on error.
        """
        async def _fetch() -> Dict[str, Any]:
            return await self._get(
                "/database/search",
                params={
                    "type": "release",
                    "style": style,
                    "page": page,
                    "per_page": min(per_page, 100),
                },
            )

        try:
            return await self._retry(_fetch)()
        except DiscogsError as exc:
            logger.warning("discogs_search_failed", style=style, page=page, error=str(exc))
            return {}
        except Exception as exc:
            logger.warning("discogs_search_error", style=style, page=page, error=str(exc))
            return {}

    async def get_release(self, release_id: int) -> Dict[str, Any]:
        """
        Fetch a single release by its Discogs ID to get the tracklist.

        Returns the full release dict or ``{}`` on error.
        """
        async def _fetch() -> Dict[str, Any]:
            return await self._get(f"/releases/{release_id}")

        try:
            return await self._retry(_fetch)()
        except DiscogsError as exc:
            if exc.status == 404:
                return {}
            logger.warning("discogs_release_failed", release_id=release_id, error=str(exc))
            return {}
        except Exception as exc:
            logger.warning("discogs_release_error", release_id=release_id, error=str(exc))
            return {}

    # ── Static parsing helpers ─────────────────────────────────────────────────

    @staticmethod
    def parse_search_result(raw: Dict[str, Any]) -> Optional[Tuple[str, str, int]]:
        """
        Parse a Discogs search result into ``(artist, title, release_id)``.

        Discogs search results return ``title`` as "Artist - Release Title".
        Returns ``None`` if the result can't be meaningfully parsed.
        """
        title_raw: str = raw.get("title") or ""
        release_id: int = raw.get("id") or 0

        if not title_raw or not release_id:
            return None

        if " - " in title_raw:
            parts = title_raw.split(" - ", 1)
            artist = parts[0].strip()
            title = parts[1].strip()
        else:
            # Compilations or releases without clear artist prefix
            artist = "Various Artists"
            title = title_raw.strip()

        if not artist or not title:
            return None

        return artist, title, release_id

    @staticmethod
    def parse_tracklist(
        release: Dict[str, Any], artist_fallback: str
    ) -> List[Dict[str, Any]]:
        """
        Extract individual tracks from a full Discogs release document.

        Returns a list of ``{title, artist, duration_ms, discogs_release_id}``.
        """
        release_id: int = release.get("id") or 0
        tracklist = release.get("tracklist") or []

        # Primary artist(s) from the release-level artists field
        release_artists = release.get("artists") or []
        release_artist = (
            release_artists[0].get("name", artist_fallback).strip()
            if release_artists
            else artist_fallback
        )

        result = []
        for track in tracklist:
            # Skip index tracks (disc headers, etc.)
            if track.get("type_") in ("heading", "index"):
                continue

            track_title: str = (track.get("title") or "").strip()
            if not track_title:
                continue

            # Track-level artist overrides release artist if specified
            track_artists = track.get("artists") or []
            if track_artists:
                artist = track_artists[0].get("name", release_artist).strip()
            else:
                artist = release_artist

            duration_ms = _parse_discogs_duration(track.get("duration") or "")

            result.append({
                "title": track_title,
                "artist": artist,
                "duration_ms": duration_ms,
                "discogs_release_id": release_id,
            })

        return result


def _parse_discogs_duration(duration: str) -> Optional[int]:
    """Parse Discogs duration string 'M:SS' into milliseconds."""
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
