"""
Deezer API client.

Uses the public Deezer API — no authentication required for read endpoints.
Rate limit: 50 requests per 5 seconds (unauthenticated).

Key endpoints used:
- GET /genre                        → list all genres
- GET /genre/{id}/artists           → top artists for a genre
- GET /artist/{id}/top?limit=N      → top tracks for an artist
- GET /artist/{id}/albums?limit=N   → artist album list (paginated)
- GET /album/{id}/tracks            → all tracks in an album
- GET /chart/0/tracks?limit=N       → global top chart

Deezer track objects include ISRC, duration, explicit flag, and album
metadata — everything needed for direct pipeline insertion.
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

BASE_URL = "https://api.deezer.com"


class DeezerError(Exception):
    """Raised for non-retryable Deezer API errors."""
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"Deezer {status}: {message}")
        self.status = status


class DeezerClient:
    """
    Async Deezer API client with rate limiting and retry.

    Manages its own httpx.AsyncClient — call ``aclose()`` when done.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Deezer: 50 req / 5 s = 10 rps per IP.
        # Use settings.deezer_rate_limit_rps so multiple replicas on the same
        # host don't collectively exceed the IP-level cap.
        rate = settings.deezer_rate_limit_rps
        self._limiter = RateLimiter(rate=rate, capacity=rate)
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    def _retry(self, func):  # type: ignore[no-untyped-def]
        return retry(
            retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
            stop=stop_after_attempt(3),
            wait=wait_exponential_jitter(initial=2, max=30, jitter=2),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        await self._limiter.acquire()
        url = f"{BASE_URL}{path}"
        resp = await self._http.get(url, params=params)

        if resp.status_code == 429:
            logger.warning("deezer_rate_limited", sleeping_for=5)
            await asyncio.sleep(5)
            raise httpx.TransportError("Rate limited")

        if resp.status_code >= 500:
            raise DeezerError(resp.status_code, f"Server error: {resp.text[:100]}")

        if not resp.is_success:
            raise DeezerError(resp.status_code, resp.text[:100])

        data = resp.json()
        # Deezer wraps errors in a 200 response with {"error": {...}}
        if isinstance(data, dict) and data.get("error"):
            err = data["error"]
            raise DeezerError(err.get("code", 0), err.get("message", "Unknown"))

        return data

    # ── Genres ────────────────────────────────────────────────────────────────

    async def get_genres(self) -> List[Dict[str, Any]]:
        """Return all Deezer genres (id, name, picture)."""
        async def _fetch() -> List[Dict[str, Any]]:
            data = await self._get("/genre")
            return data.get("data", [])
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.error("deezer_get_genres_failed", error=str(exc))
            return []

    async def get_genre_artists(self, genre_id: int) -> List[Dict[str, Any]]:
        """Return top artists for a genre (Deezer returns ~20-25, no pagination)."""
        async def _fetch() -> List[Dict[str, Any]]:
            data = await self._get(f"/genre/{genre_id}/artists")
            return data.get("data", [])
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.error("deezer_get_genre_artists_failed", genre_id=genre_id, error=str(exc))
            return []

    # ── Artists ───────────────────────────────────────────────────────────────

    async def get_artist_top_tracks(
        self, artist_id: int, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Return up to ``limit`` top tracks for an artist."""
        async def _fetch() -> List[Dict[str, Any]]:
            data = await self._get(
                f"/artist/{artist_id}/top",
                params={"limit": min(limit, 100)},
            )
            return data.get("data", [])
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.error("deezer_get_top_tracks_failed", artist_id=artist_id, error=str(exc))
            return []

    async def get_artist_albums(
        self, artist_id: int, limit: int = 25, index: int = 0
    ) -> Dict[str, Any]:
        """Fetch one page of albums for an artist."""
        async def _fetch() -> Dict[str, Any]:
            return await self._get(
                f"/artist/{artist_id}/albums",
                params={"limit": min(limit, 25), "index": index},
            )
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.error("deezer_get_artist_albums_failed", artist_id=artist_id, error=str(exc))
            return {}

    # ── Albums ────────────────────────────────────────────────────────────────

    async def get_album_tracks(self, album_id: int) -> List[Dict[str, Any]]:
        """Return all tracks from an album (Deezer returns up to 2000, no pagination needed)."""
        async def _fetch() -> List[Dict[str, Any]]:
            data = await self._get(f"/album/{album_id}/tracks")
            return data.get("data", [])
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.error("deezer_get_album_tracks_failed", album_id=album_id, error=str(exc))
            return []

    async def get_album(self, album_id: int) -> Optional[Dict[str, Any]]:
        """Fetch full album metadata (includes ISRC on individual tracks)."""
        async def _fetch() -> Dict[str, Any]:
            return await self._get(f"/album/{album_id}")
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.error("deezer_get_album_failed", album_id=album_id, error=str(exc))
            return None

    # ── Chart ─────────────────────────────────────────────────────────────────

    async def get_chart_tracks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Return global top chart tracks."""
        async def _fetch() -> List[Dict[str, Any]]:
            data = await self._get("/chart/0/tracks", params={"limit": min(limit, 100)})
            return data.get("data", [])
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.error("deezer_get_chart_failed", error=str(exc))
            return []

    async def search_artists(
        self, query: str, limit: int = 25
    ) -> List[Dict[str, Any]]:
        """Search artists by free-text query. Supports Cyrillic and Latin queries."""
        async def _fetch() -> List[Dict[str, Any]]:
            data = await self._get(
                "/search/artist",
                params={"q": query, "limit": min(limit, 100)},
            )
            return data.get("data", [])
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.debug("deezer_search_artists_failed", query=query, error=str(exc))
            return []

    async def get_track(self, track_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single track (includes isrc in the response)."""
        async def _fetch() -> Dict[str, Any]:
            return await self._get(f"/track/{track_id}")
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.debug("deezer_get_track_failed", track_id=track_id, error=str(exc))
            return None

    # ── Search ────────────────────────────────────────────────────────────────

    async def search_tracks(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search tracks by free-text query (artist + title)."""
        async def _fetch() -> List[Dict[str, Any]]:
            data = await self._get(
                "/search",
                params={"q": query, "type": "track", "limit": min(limit, 50)},
            )
            return data.get("data", [])
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.warning("deezer_search_tracks_failed", query=query, error=str(exc))
            return []

    async def get_artist_related(self, artist_id: int) -> List[Dict[str, Any]]:
        """
        Fetch up to 20 related artists for BFS graph expansion.

        Deezer still supports this endpoint unlike Spotify, which deprecated it
        in November 2024.
        """
        async def _fetch() -> List[Dict[str, Any]]:
            data = await self._get(
                f"/artist/{artist_id}/related",
                params={"limit": 20},
            )
            return data.get("data", [])
        try:
            return await self._retry(_fetch)()
        except Exception as exc:
            logger.debug("deezer_get_related_failed", artist_id=artist_id, error=str(exc))
            return []
