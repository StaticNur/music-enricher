"""
NetEase Cloud Music API client (v9).

Uses the pyncm library which handles NetEase's proprietary encryption.
No authentication required for public content (charts, search, playlists).

Key capabilities:
- Search songs by keyword (genre, artist, etc.)
- Fetch top chart playlists (热歌榜, 新歌榜, 飙升榜, etc.)
- Fetch playlist tracks
- BFS via artist top songs and related artists

Rate limit: 2 rps (self-imposed, unofficial API, conservative).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

import structlog

from app.core.config import Settings
from app.utils.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)

# Known NetEase Cloud Music chart playlist IDs (hardcoded).
# These are stable IDs for major Chinese music charts.
_NETEASE_CHART_PLAYLISTS: List[tuple[int, str]] = [
    (3778678, "热歌榜"),        # Hot songs
    (3779629, "新歌榜"),        # New songs
    (19723756, "飙升榜"),      # Rising songs
    (2884035, "原创榜"),        # Original songs
    (180106, "华语"),           # Chinese songs
    (2809513713, "国风热歌"),   # Chinese folk trending
    (5059644681, "抖音热歌"),   # Douyin/TikTok trending
    (3136952023, "电音"),       # Electronic
    (2250011882, "说唱"),       # Hip-hop/Rap
    (2809577409, "轻音乐"),     # Instrumental
    (5082060500, "ACG"),        # Anime/Comic/Game
    (3082010842, "影视金曲"),   # Movie & TV
    (351952284, "古风"),         # Ancient style
]


class NeteaseMusicClient:
    """
    Async NetEase Cloud Music client using pyncm library.

    pyncm calls are synchronous — they are dispatched via
    ``loop.run_in_executor`` so as not to block the event loop.
    Manages its own rate limiter — call ``aclose()`` when done (no-op).
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        rate = settings.netease_rate_limit_rps
        self._limiter = RateLimiter(rate=rate, capacity=rate)

    async def aclose(self) -> None:
        """No persistent connection to close."""
        pass

    async def _run_in_executor(self, func, *args):  # type: ignore[no-untyped-def]
        """Acquire rate-limit token then run synchronous pyncm call in thread pool."""
        await self._limiter.acquire()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    # ── Search ────────────────────────────────────────────────────────────────

    async def search_songs(
        self, query: str, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Search songs by keyword. Returns normalized track dicts."""
        try:
            from pyncm import apis  # type: ignore[import]

            # pyncm >= 2.0 renamed apis.search → apis.cloudsearch
            search_fn = getattr(apis, "cloudsearch", None) or getattr(apis, "search", None)
            if search_fn is None:
                raise AttributeError("pyncm has no cloudsearch or search module")

            result = await self._run_in_executor(
                search_fn.GetSearchResult, query, 1, limit, offset
            )
            songs = result.get("result", {}).get("songs", [])
            return [t for t in (self._parse_song(s) for s in songs) if t]
        except Exception as exc:
            logger.error("netease_search_failed", query=query, error=str(exc))
            return []

    # ── Charts ────────────────────────────────────────────────────────────────

    def get_chart_playlists(self) -> List[Dict[str, Any]]:
        """
        Return the list of known NetEase chart playlists.

        Returns ``[{id, name}, ...]`` — IDs are hardcoded (they are stable).
        This is synchronous (no API call needed).
        """
        return [
            {"id": playlist_id, "name": name}
            for playlist_id, name in _NETEASE_CHART_PLAYLISTS
        ]

    async def get_playlist_tracks(self, playlist_id: int) -> List[Dict[str, Any]]:
        """Fetch all tracks from a NetEase playlist."""
        try:
            from pyncm import apis  # type: ignore[import]

            result = await self._run_in_executor(
                apis.playlist.GetPlaylistInfo, playlist_id
            )
            # pyncm returns {"playlist": {"tracks": [...]}}
            playlist_data = result.get("playlist") or {}
            tracks = playlist_data.get("tracks") or []
            return [t for t in (self._parse_song(s) for s in tracks) if t]
        except Exception as exc:
            logger.error(
                "netease_get_playlist_failed", playlist_id=playlist_id, error=str(exc)
            )
            return []

    # ── Artist ────────────────────────────────────────────────────────────────

    async def get_artist_top_songs(
        self, artist_id: int, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Fetch top songs for a NetEase artist."""
        try:
            from pyncm import apis  # type: ignore[import]

            result = await self._run_in_executor(
                apis.artist.GetArtistTopSongs, artist_id
            )
            songs = result.get("songs") or []
            return [t for t in (self._parse_song(s) for s in songs[:limit]) if t]
        except Exception as exc:
            logger.error(
                "netease_get_artist_top_songs_failed",
                artist_id=artist_id,
                error=str(exc),
            )
            return []

    async def get_related_artists(self, artist_id: int) -> List[Dict[str, Any]]:
        """Fetch related/similar artists for BFS expansion."""
        try:
            from pyncm import apis  # type: ignore[import]

            result = await self._run_in_executor(
                apis.artist.GetArtistSimi, artist_id
            )
            artists = result.get("artists") or []
            return [
                {"id": a.get("id"), "name": a.get("name", "")}
                for a in artists
                if a.get("id")
            ]
        except Exception as exc:
            logger.error(
                "netease_get_related_artists_failed",
                artist_id=artist_id,
                error=str(exc),
            )
            return []

    # ── Parsing ───────────────────────────────────────────────────────────────

    def _parse_song(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a raw pyncm song object into a normalized dict.

        Returns None if required fields are missing.
        """
        song_id = raw.get("id")
        name = (raw.get("name") or "").strip()

        if not song_id or not name:
            return None

        # First artist name from the ``ar`` array
        artists_raw = raw.get("ar") or raw.get("artists") or []
        artist_name = ""
        if isinstance(artists_raw, list) and artists_raw:
            first = artists_raw[0]
            if isinstance(first, dict):
                artist_name = (first.get("name") or "").strip()

        if not artist_name:
            return None

        # Duration in ms (``dt`` field in pyncm responses)
        duration_ms: int = raw.get("dt") or raw.get("duration") or 0
        try:
            duration_ms = int(duration_ms)
        except (ValueError, TypeError):
            duration_ms = 0

        return {
            "id": int(song_id),
            "name": name,
            "artist_name": artist_name,
            "duration_ms": duration_ms,
        }
