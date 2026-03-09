"""
SoundCloud API client (v9).

Uses SoundCloud's API v2 which requires a client_id.
The client_id is auto-discovered from SoundCloud's web app JS bundles
or can be set via SOUNDCLOUD_CLIENT_ID env var.

The client_id is a 32-char alphanumeric string embedded in SoundCloud's
JavaScript bundle. It changes infrequently (monthly at most).
Auto-discovery fetches soundcloud.com, finds script URLs, downloads
one bundle, and extracts the client_id via regex.

Key endpoints:
  Charts: GET https://api-v2.soundcloud.com/charts?kind=trending&genre=soundcloud:genres:{genre}&limit=200
  Search: GET https://api-v2.soundcloud.com/search/tracks?q={q}&limit=200
  User tracks: GET https://api-v2.soundcloud.com/users/{user_id}/tracks?limit=200
  Related: GET https://api-v2.soundcloud.com/tracks/{track_id}/related?limit=50

Rate limit: 2 rps (conservative, SoundCloud throttles aggressively).
"""
from __future__ import annotations

import asyncio
import logging
import re
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

_API_BASE = "https://api-v2.soundcloud.com"
_WEB_BASE = "https://soundcloud.com"


class SoundCloudError(Exception):
    """Raised for non-retryable SoundCloud API errors."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"SoundCloud {status}: {message}")
        self.status = status


class SoundCloudClient:
    """
    Async SoundCloud API v2 client.

    Requires a client_id, either set via ``soundcloud_client_id`` config
    or auto-discovered from the SoundCloud web app JavaScript bundles.
    Manages its own httpx.AsyncClient — call ``aclose()`` when done.
    """

    _CLIENT_ID_RE = re.compile(r'client_id:"([A-Za-z0-9]{20,40})"')
    _SC_WEB_SCRIPT_RE = re.compile(r'<script[^>]+src="(https://[^"]+\.js)"')

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Always start with empty — auto-discovery runs on first ensure_client_id().
        # SOUNDCLOUD_CLIENT_ID env var is kept as an override hint only if discovery fails.
        self._client_id: str = ""
        self._client_id_hint: str = settings.soundcloud_client_id or ""
        rate = settings.soundcloud_rate_limit_rps
        self._limiter = RateLimiter(rate=rate, capacity=rate)
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://soundcloud.com/",
                "Origin": "https://soundcloud.com",
            },
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    # ── Client ID management ──────────────────────────────────────────────────

    async def ensure_client_id(self) -> bool:
        """
        Ensure we have a valid client_id via auto-discovery.

        Always discovers fresh from SoundCloud JS bundles so the client_id
        is never stale. Falls back to the SOUNDCLOUD_CLIENT_ID env hint only
        if discovery fails (e.g. network unreachable).
        Returns True if client_id is available, False if all sources failed.
        """
        discovered = await self._discover_client_id()
        if discovered:
            self._client_id = discovered
            logger.info(
                "soundcloud_client_id_discovered",
                client_id=discovered[:8] + "...",
            )
            return True

        # Discovery failed — fall back to env hint if available
        if self._client_id_hint:
            logger.warning(
                "soundcloud_client_id_discovery_failed_using_hint",
                hint=self._client_id_hint[:8] + "...",
            )
            self._client_id = self._client_id_hint
            return True

        logger.error("soundcloud_client_id_not_found")
        return False

    async def _discover_client_id(self) -> Optional[str]:
        """
        Extract client_id from SoundCloud web app JavaScript bundles.

        SoundCloud embeds the client_id as ``client_id:"<value>"`` in their
        bundled JS. We fetch the homepage to find script URLs, then scan
        the last few app bundle scripts for the pattern.
        """
        try:
            resp = await self._http.get(_WEB_BASE + "/", timeout=20.0)
            if not resp.is_success:
                logger.warning(
                    "soundcloud_homepage_fetch_failed", status=resp.status_code
                )
                return None

            script_urls = self._SC_WEB_SCRIPT_RE.findall(resp.text)
            if not script_urls:
                logger.warning("soundcloud_no_script_urls_found")
                return None

            # Check the last 8 scripts — app bundles tend to be last.
            # Skip scripts that look like vendor/chunk-only files and check those too.
            candidates = list(reversed(script_urls[-8:]))
            for url in candidates:
                try:
                    r = await self._http.get(url, timeout=15.0)
                    if not r.is_success:
                        continue
                    match = self._CLIENT_ID_RE.search(r.text)
                    if match:
                        return match.group(1)
                except Exception:
                    continue

        except Exception as exc:
            logger.warning("soundcloud_client_id_discovery_failed", error=str(exc))

        return None

    # ── HTTP helper ───────────────────────────────────────────────────────────

    async def _get(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Rate-limited GET with retry on transport errors.

        On 401/403 (expired client_id): immediately re-discovers a fresh
        client_id and retries the same request once — no restart needed.
        """
        for _id_attempt in range(2):  # allow one client_id refresh per call
            # Ensure we have a client_id (re-discover if cleared)
            if not self._client_id:
                discovered = await self._discover_client_id()
                if discovered:
                    self._client_id = discovered
                    logger.info(
                        "soundcloud_client_id_refreshed",
                        client_id=discovered[:8] + "...",
                    )
                else:
                    raise SoundCloudError(0, "client_id unavailable")

            await self._limiter.acquire()

            merged_params: Dict[str, Any] = {"client_id": self._client_id}
            if params:
                merged_params.update(params)

            @retry(
                retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
                stop=stop_after_attempt(3),
                wait=wait_exponential_jitter(initial=2, max=30, jitter=2),
                before_sleep=before_sleep_log(logger, logging.WARNING),
                reraise=True,
            )
            async def _fetch() -> Any:
                url = f"{_API_BASE}{path}"
                resp = await self._http.get(url, params=merged_params)

                if resp.status_code == 429:
                    logger.warning("soundcloud_rate_limited", sleeping_for=30)
                    await asyncio.sleep(30)
                    raise httpx.TransportError("Rate limited")

                if resp.status_code >= 500:
                    raise httpx.TransportError(f"SoundCloud server error {resp.status_code}")

                if not resp.is_success:
                    raise SoundCloudError(resp.status_code, resp.text[:100])

                try:
                    return resp.json()
                except Exception:
                    raise SoundCloudError(0, "Invalid JSON response")

            try:
                return await _fetch()
            except SoundCloudError as exc:
                if exc.status in (401, 403) and _id_attempt == 0:
                    # client_id expired — clear and retry with a fresh one
                    logger.warning(
                        "soundcloud_client_id_expired_refreshing",
                        status=exc.status,
                    )
                    self._client_id = ""
                    continue
                raise

        raise SoundCloudError(0, "client_id refresh failed")

    # ── Charts ────────────────────────────────────────────────────────────────

    async def get_chart_tracks(
        self,
        genre: str = "all-music",
        kind: str = "trending",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Fetch trending or top tracks for a SoundCloud genre.

        Returns list of normalized track dicts.
        """
        try:
            data = await self._get(
                "/charts",
                params={
                    "kind": kind,
                    "genre": f"soundcloud:genres:{genre}",
                    "limit": min(limit, 200),
                    "linked_partitioning": "1",
                },
            )
            collection = data.get("collection") or []
            tracks = []
            for item in collection:
                # Chart items wrap the track under a "track" key
                track_obj = item.get("track") or item
                parsed = self._parse_track(track_obj)
                if parsed:
                    tracks.append(parsed)
            return tracks
        except Exception as exc:
            logger.error(
                "soundcloud_chart_failed", genre=genre, kind=kind, error=str(exc)
            )
            return []

    # ── Search ────────────────────────────────────────────────────────────────

    async def search_tracks(
        self, query: str, limit: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Search for tracks by free-text query.

        Returns list of normalized track dicts.
        """
        try:
            data = await self._get(
                "/search/tracks",
                params={
                    "q": query,
                    "limit": min(limit, 200),
                    "linked_partitioning": "1",
                },
            )
            collection = data.get("collection") or []
            return [t for t in (self._parse_track(item) for item in collection) if t]
        except Exception as exc:
            logger.error("soundcloud_search_failed", query=query, error=str(exc))
            return []

    # ── User tracks ───────────────────────────────────────────────────────────

    async def get_user_tracks(
        self, user_id: int, limit: int = 200
    ) -> List[Dict[str, Any]]:
        """Fetch recent tracks for a SoundCloud user (artist)."""
        try:
            data = await self._get(
                f"/users/{user_id}/tracks",
                params={"limit": min(limit, 200), "linked_partitioning": "1"},
            )
            collection = data.get("collection") or []
            return [t for t in (self._parse_track(item) for item in collection) if t]
        except Exception as exc:
            logger.error(
                "soundcloud_user_tracks_failed", user_id=user_id, error=str(exc)
            )
            return []

    # ── Related tracks ────────────────────────────────────────────────────────

    async def get_related_tracks(self, track_id: int) -> List[Dict[str, Any]]:
        """Fetch tracks related to a given track (SoundCloud recommendation)."""
        try:
            data = await self._get(
                f"/tracks/{track_id}/related",
                params={"limit": 50, "linked_partitioning": "1"},
            )
            collection = data.get("collection") or []
            return [t for t in (self._parse_track(item) for item in collection) if t]
        except Exception as exc:
            logger.error(
                "soundcloud_related_failed", track_id=track_id, error=str(exc)
            )
            return []

    # ── Parsing ───────────────────────────────────────────────────────────────

    def _parse_track(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a raw SoundCloud track object into a normalized dict.

        Returns None if required fields are missing.

        SoundCloud track duration is already in milliseconds.
        """
        track_id = raw.get("id")
        title = (raw.get("title") or "").strip()

        if not track_id or not title:
            return None

        # Artist = uploader username
        user = raw.get("user") or {}
        artist = (user.get("username") or user.get("full_name") or "").strip()
        if not artist:
            return None

        # Duration is in ms already
        duration_ms: int = raw.get("duration") or 0
        try:
            duration_ms = int(duration_ms)
        except (ValueError, TypeError):
            duration_ms = 0

        genre: str = (raw.get("genre") or "").strip()
        permalink_url: str = (raw.get("permalink_url") or "").strip()

        return {
            "id": int(track_id),
            "title": title,
            "artist": artist,
            "duration_ms": duration_ms,
            "genre": genre,
            "permalink_url": permalink_url,
        }
