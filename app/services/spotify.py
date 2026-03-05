"""
Spotify Web API client.

Handles:
- Client Credentials OAuth token lifecycle (auto-refresh)
- Rate-limited async HTTP via httpx + token bucket
- Automatic retry with exponential backoff and jitter (tenacity)
- Graceful handling of deprecated endpoints (audio-features → 403/404)
- All pagination transparently handled in helper methods

All public methods raise ``SpotifyError`` on unrecoverable errors
and let ``tenacity`` handle transient ones (429, 5xx).
"""
from __future__ import annotations

import asyncio
import base64
import logging
import time
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
    before_sleep_log,
    RetryError,
)

from app.core.config import Settings
from app.utils.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)

BASE_URL = "https://api.spotify.com/v1"
AUTH_URL = "https://accounts.spotify.com/api/token"


class SpotifyError(Exception):
    """Raised for unrecoverable Spotify API errors (4xx except 429)."""
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"Spotify {status}: {message}")
        self.status = status


class SpotifyRateLimit(Exception):
    """Raised on 429 — tenacity will retry after the Retry-After delay."""
    def __init__(self, retry_after: int = 1) -> None:
        super().__init__(f"Rate limited, retry after {retry_after}s")
        self.retry_after = retry_after


class _TokenManager:
    """Manages Spotify access token with automatic refresh."""

    def __init__(self, client_id: str, client_secret: str) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = asyncio.Lock()

    async def get_token(self, http: httpx.AsyncClient) -> str:
        """Return a valid access token, refreshing if necessary."""
        async with self._lock:
            if self._access_token and time.monotonic() < self._expires_at - 60:
                return self._access_token
            await self._refresh(http)
            return self._access_token  # type: ignore[return-value]

    async def _refresh(self, http: httpx.AsyncClient) -> None:
        credentials = base64.b64encode(
            f"{self._client_id}:{self._client_secret}".encode()
        ).decode()
        resp = await http.post(
            AUTH_URL,
            headers={"Authorization": f"Basic {credentials}"},
            data={"grant_type": "client_credentials"},
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._expires_at = time.monotonic() + data["expires_in"]
        logger.info("spotify_token_refreshed")


class SpotifyClient:
    """
    Async Spotify Web API client.

    Manages its own httpx.AsyncClient lifecycle — use as an async context
    manager or call ``aclose()`` explicitly.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._token_manager = _TokenManager(
            settings.spotify_client_id,
            settings.spotify_client_secret,
        )
        self._limiter = RateLimiter(
            rate=settings.spotify_rate_limit_rps,
            capacity=settings.spotify_rate_limit_rps,
        )
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "SpotifyClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    # ── Low-level request ─────────────────────────────────────────────────────

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a GET request with rate limiting.

        Raises ``SpotifyRateLimit`` on 429 so tenacity can retry.
        Raises ``SpotifyError`` on other 4xx/5xx.
        """
        await self._limiter.acquire()
        token = await self._token_manager.get_token(self._http)
        url = f"{BASE_URL}{path}"
        resp = await self._http.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
        )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "1"))
            logger.warning("spotify_rate_limited", retry_after=retry_after)
            await asyncio.sleep(retry_after)
            raise SpotifyRateLimit(retry_after)

        if resp.status_code in (401, 403):
            # Token might have expired concurrently — force refresh next call
            self._token_manager._expires_at = 0.0
            if resp.status_code == 403:
                # Some endpoints are deprecated / restricted
                raise SpotifyError(403, resp.text[:200])
            raise SpotifyError(401, "Unauthorized")

        if resp.status_code == 404:
            raise SpotifyError(404, f"Not found: {url}")

        if resp.status_code >= 500:
            raise SpotifyError(resp.status_code, f"Server error: {resp.text[:200]}")

        if not resp.is_success:
            raise SpotifyError(resp.status_code, resp.text[:200])

        return resp.json()

    def _retry(self, func):  # type: ignore[no-untyped-def]
        """Decorator: retry on transient errors."""
        return retry(
            retry=retry_if_exception_type((SpotifyRateLimit, httpx.TransportError, httpx.TimeoutException)),
            stop=stop_after_attempt(self._settings.spotify_max_retries),
            wait=wait_exponential_jitter(initial=1, max=60, jitter=2),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    # ── Track endpoints ───────────────────────────────────────────────────────

    async def get_tracks(self, track_ids: List[str]) -> List[Optional[Dict]]:
        """
        Fetch up to 50 tracks in a single batch request.

        Returns a list of track dicts (or None for missing IDs).
        """
        if not track_ids:
            return []

        async def _fetch(ids: List[str]) -> List[Optional[Dict]]:
            data = await self._get("/tracks", params={"ids": ",".join(ids)})
            return data.get("tracks", [])

        results: List[Optional[Dict]] = []
        for i in range(0, len(track_ids), 50):
            chunk = track_ids[i : i + 50]
            try:
                results.extend(await self._retry(_fetch)(chunk))
            except (SpotifyError, RetryError) as exc:
                logger.error("get_tracks_failed", ids=chunk[:3], error=str(exc))
                results.extend([None] * len(chunk))
        return results

    async def get_audio_features(
        self, track_ids: List[str]
    ) -> List[Optional[Dict]]:
        """
        Fetch audio features for up to 100 tracks.

        Returns None items for unavailable tracks.
        Note: This endpoint may return 403 for restricted API tiers.
        """
        if not track_ids:
            return []

        async def _fetch(ids: List[str]) -> List[Optional[Dict]]:
            data = await self._get(
                "/audio-features", params={"ids": ",".join(ids)}
            )
            return data.get("audio_features", [])

        results: List[Optional[Dict]] = []
        for i in range(0, len(track_ids), 100):
            chunk = track_ids[i : i + 100]
            try:
                results.extend(await self._retry(_fetch)(chunk))
            except SpotifyError as exc:
                if exc.status == 403:
                    logger.warning(
                        "audio_features_forbidden",
                        message="Audio features endpoint restricted for this app",
                    )
                    results.extend([None] * len(chunk))
                else:
                    logger.error("audio_features_failed", error=str(exc))
                    results.extend([None] * len(chunk))
            except (RetryError, Exception) as exc:
                logger.error("audio_features_failed", error=str(exc))
                results.extend([None] * len(chunk))
        return results

    # ── Playlist endpoints ────────────────────────────────────────────────────

    async def get_featured_playlists(
        self, limit: int = 50, offset: int = 0
    ) -> Dict:
        """Fetch Spotify's featured playlists."""
        async def _fetch() -> Dict:
            return await self._get(
                "/browse/featured-playlists",
                params={"limit": min(limit, 50), "offset": offset},
            )
        return await self._retry(_fetch)()

    async def search_playlists(
        self, query: str, limit: int = 50, offset: int = 0
    ) -> Dict:
        """Search for playlists by query string."""
        async def _fetch() -> Dict:
            return await self._get(
                "/search",
                params={
                    "q": query,
                    "type": "playlist",
                    "limit": min(limit, 50),
                    "offset": offset,
                },
            )
        return await self._retry(_fetch)()

    async def get_playlist_tracks(
        self, playlist_id: str, limit: int = 100, offset: int = 0
    ) -> Dict:
        """Fetch one page of tracks from a playlist."""
        async def _fetch() -> Dict:
            return await self._get(
                f"/playlists/{playlist_id}/tracks",
                params={
                    "limit": min(limit, 100),
                    "offset": offset,
                    "fields": (
                        "items(track(id,name,artists,album,popularity,"
                        "duration_ms,explicit,external_ids,available_markets)),"
                        "next,total,offset"
                    ),
                },
            )
        return await self._retry(_fetch)()

    async def iter_playlist_tracks(
        self, playlist_id: str
    ) -> AsyncGenerator[Dict, None]:
        """
        Async generator yielding all track objects from a playlist.

        Handles pagination automatically.
        """
        offset = 0
        limit = 100
        while True:
            try:
                page = await self.get_playlist_tracks(playlist_id, limit, offset)
            except (SpotifyError, httpx.TransportError, httpx.TimeoutException) as exc:
                logger.error(
                    "playlist_tracks_failed",
                    playlist_id=playlist_id,
                    error=str(exc),
                )
                return

            items = page.get("items", [])
            for item in items:
                track = item.get("track")
                if track and track.get("id"):
                    yield track

            if not page.get("next"):
                break
            offset += limit

    # ── Artist endpoints ──────────────────────────────────────────────────────

    async def get_artists(self, artist_ids: List[str]) -> List[Optional[Dict]]:
        """Fetch up to 50 artists by ID."""
        if not artist_ids:
            return []

        async def _fetch(ids: List[str]) -> List[Optional[Dict]]:
            data = await self._get("/artists", params={"ids": ",".join(ids)})
            return data.get("artists", [])

        results: List[Optional[Dict]] = []
        for i in range(0, len(artist_ids), 50):
            chunk = artist_ids[i : i + 50]
            try:
                results.extend(await self._retry(_fetch)(chunk))
            except (SpotifyError, RetryError) as exc:
                logger.error("get_artists_failed", error=str(exc))
                results.extend([None] * len(chunk))
        return results

    async def get_artist_albums(
        self,
        artist_id: str,
        include_groups: str = "album,single",
        limit: int = 50,
        offset: int = 0,
    ) -> Dict:
        """Fetch one page of albums for an artist."""
        async def _fetch() -> Dict:
            return await self._get(
                f"/artists/{artist_id}/albums",
                params={
                    "include_groups": include_groups,
                    "limit": min(limit, 50),
                    "offset": offset,
                    "market": "US",
                },
            )
        return await self._retry(_fetch)()

    async def iter_artist_albums(
        self, artist_id: str, include_groups: str = "album,single"
    ) -> AsyncGenerator[Dict, None]:
        """Async generator yielding all albums for an artist."""
        offset = 0
        limit = 50
        while True:
            try:
                page = await self.get_artist_albums(
                    artist_id, include_groups, limit, offset
                )
            except (SpotifyError, httpx.TransportError, httpx.TimeoutException) as exc:
                logger.error(
                    "artist_albums_failed",
                    artist_id=artist_id,
                    error=str(exc),
                )
                return

            items = page.get("items", [])
            for album in items:
                if album:
                    yield album

            if not page.get("next"):
                break
            offset += limit

    async def get_album_tracks(
        self, album_id: str, limit: int = 50, offset: int = 0
    ) -> Dict:
        """Fetch one page of tracks from an album."""
        async def _fetch() -> Dict:
            return await self._get(
                f"/albums/{album_id}/tracks",
                params={"limit": min(limit, 50), "offset": offset, "market": "US"},
            )
        return await self._retry(_fetch)()

    async def iter_album_tracks(self, album_id: str) -> AsyncGenerator[Dict, None]:
        """Async generator yielding all tracks from an album."""
        offset = 0
        limit = 50
        while True:
            try:
                page = await self.get_album_tracks(album_id, limit, offset)
            except (SpotifyError, httpx.TransportError, httpx.TimeoutException) as exc:
                logger.error(
                    "album_tracks_failed", album_id=album_id, error=str(exc)
                )
                return

            items = page.get("items", [])
            for track in items:
                if track and track.get("id"):
                    yield track

            if not page.get("next"):
                break
            offset += limit

    # ── Search endpoint ───────────────────────────────────────────────────────

    async def search_tracks(
        self, query: str, limit: int = 50, offset: int = 0
    ) -> Dict:
        """Search for tracks. Supports genre: operator in query."""
        async def _fetch() -> Dict:
            return await self._get(
                "/search",
                params={
                    "q": query,
                    "type": "track",
                    "limit": min(limit, 50),
                    "offset": offset,
                    "market": "US",
                },
            )
        return await self._retry(_fetch)()

    # ── Browse categories ─────────────────────────────────────────────────────

    async def get_categories(self, limit: int = 50, offset: int = 0) -> Dict:
        """Fetch browse categories (may be deprecated on some API tiers)."""
        async def _fetch() -> Dict:
            return await self._get(
                "/browse/categories",
                params={"limit": min(limit, 50), "offset": offset},
            )
        try:
            return await self._retry(_fetch)()
        except SpotifyError as exc:
            if exc.status in (403, 404):
                logger.warning("browse_categories_unavailable", status=exc.status)
                return {"categories": {"items": []}}
            raise

    async def get_category_playlists(
        self, category_id: str, limit: int = 50, offset: int = 0
    ) -> Dict:
        """Fetch playlists for a browse category.

        Raises SpotifyError on 403/404 so callers can detect endpoint-wide
        deprecation and bail out early instead of retrying all categories.
        """
        async def _fetch() -> Dict:
            return await self._get(
                f"/browse/categories/{category_id}/playlists",
                params={"limit": min(limit, 50), "offset": offset},
            )
        return await self._retry(_fetch)()
