"""
Yandex Music API client (v7).

Uses the unofficial yandex-music-api library (pip install yandex-music).
Requires a free Yandex account OAuth token — set YANDEX_MUSIC_TOKEN in .env.

Rate limit: self-imposed 2 rps (no official limit published; conservative
to avoid detection as a scraper on an unofficial endpoint).

Key capabilities used:
- chart(country)              → CIS country top charts
- artists_tracks(id, page)   → paginated artist discography
- artists_brief_info(id)     → artist metadata + similar artists list
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

import structlog

from app.core.config import Settings
from app.utils.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)

# CIS/Central Asia countries supported by Yandex Music charts.
# These map to the `tag` parameter of ClientAsync.chart().
CIS_CHART_COUNTRIES: List[str] = [
    "ru",  # Russia
    "kz",  # Kazakhstan
    "by",  # Belarus
    "uz",  # Uzbekistan
    "am",  # Armenia
    "az",  # Azerbaijan
    "ge",  # Georgia
    "ua",  # Ukraine
    "md",  # Moldova
]


class YandexMusicClient:
    """
    Async Yandex Music client with rate limiting.

    All methods return plain dicts (not yandex-music model objects) so the
    rest of the pipeline doesn't depend on the library's internal types.

    Initialization is lazy — the underlying ClientAsync is created on the
    first real API call, guarded by an asyncio.Lock for thread safety.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        rate = settings.yandex_music_rate_limit_rps
        self._limiter = RateLimiter(rate=rate, capacity=rate)
        self._client: Any = None
        self._lock = asyncio.Lock()
        self._init_failed = False

    async def _ensure_client(self) -> bool:
        """Lazy-initialize ClientAsync. Returns True if ready."""
        if self._client is not None:
            return True
        if self._init_failed:
            return False
        if not self._settings.yandex_music_token:
            return False

        async with self._lock:
            if self._client is not None:
                return True
            if self._init_failed:
                return False
            try:
                from yandex_music import ClientAsync  # type: ignore[import]
                client = await ClientAsync(
                    token=self._settings.yandex_music_token
                ).init()
                self._client = client
                logger.info("yandex_music_client_initialized")
                return True
            except Exception as exc:
                logger.error("yandex_music_init_failed", error=str(exc))
                self._init_failed = True
                return False

    async def aclose(self) -> None:
        pass  # yandex-music-api has no explicit close

    # ── Charts ────────────────────────────────────────────────────────────────

    async def get_chart(self, country: str = "ru") -> List[Dict[str, Any]]:
        """
        Fetch top chart tracks for a CIS country.

        Returns a list of track dicts (see _track_to_dict for schema).
        Returns [] on any error so the caller can safely iterate.
        """
        if not await self._ensure_client():
            return []
        await self._limiter.acquire()
        try:
            chart_info = await self._client.chart(country)
            if not chart_info or not chart_info.chart:
                return []
            return [
                self._track_to_dict(item.track)
                for item in (chart_info.chart.tracks or [])
                if item and item.track
            ]
        except Exception as exc:
            logger.warning("yandex_chart_failed", country=country, error=str(exc))
            return []

    # ── Artist discovery ──────────────────────────────────────────────────────

    async def get_artist_tracks(
        self,
        artist_id: int,
        page: int = 0,
        page_size: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Fetch one page of an artist's tracks.

        Paginates until empty result — caller should increment page until
        this returns an empty list.
        """
        if not await self._ensure_client():
            return []
        await self._limiter.acquire()
        try:
            result = await self._client.artists_tracks(
                artist_id, page=page, page_size=page_size
            )
            if not result or not result.tracks:
                return []
            return [self._track_to_dict(t) for t in result.tracks if t]
        except Exception as exc:
            logger.debug(
                "yandex_artist_tracks_failed",
                artist_id=artist_id,
                page=page,
                error=str(exc),
            )
            return []

    async def get_artist_brief_info(
        self, artist_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch artist metadata including similar artists for BFS expansion.

        Returns None on error.
        """
        if not await self._ensure_client():
            return None
        await self._limiter.acquire()
        try:
            info = await self._client.artists_brief_info(artist_id)
            if not info:
                return None
            similar: List[Dict[str, Any]] = []
            if info.similar_artists:
                for a in info.similar_artists:
                    if a and getattr(a, "id", None):
                        similar.append({
                            "id": a.id,
                            "name": getattr(a, "name", "") or "",
                        })
            artist_obj = info.artist
            return {
                "id": artist_id,
                "name": artist_obj.name if artist_obj else "",
                "similar": similar,
            }
        except Exception as exc:
            logger.debug(
                "yandex_artist_info_failed", artist_id=artist_id, error=str(exc)
            )
            return None

    # ── Conversion ────────────────────────────────────────────────────────────

    @staticmethod
    def _track_to_dict(track: Any) -> Dict[str, Any]:
        """
        Convert a yandex-music Track object to a plain dict.

        Schema:
          id           — Yandex Music track ID (int)
          title        — track title (str)
          artists      — list of {id: int, name: str}
          album        — {id, title, year, cover_uri} | None
          duration_ms  — duration in milliseconds (int | None)
        """
        artists: List[Dict[str, Any]] = []
        for a in (track.artists or []):
            if a and getattr(a, "id", None) and getattr(a, "name", None):
                artists.append({"id": a.id, "name": a.name})

        album_data: Optional[Dict[str, Any]] = None
        if track.albums:
            alb = track.albums[0]
            if alb and getattr(alb, "id", None):
                album_data = {
                    "id": alb.id,
                    "title": getattr(alb, "title", "") or "",
                    "year": getattr(alb, "year", None),
                    "cover_uri": getattr(alb, "cover_uri", None),
                }

        return {
            "id": track.id,
            "title": track.title or "",
            "artists": artists,
            "album": album_data,
            "duration_ms": getattr(track, "duration_ms", None),
        }
