"""
Genius lyrics client.

Workflow for each track:
1. Search Genius API: ``GET /search?q={track_name} {artist_name}``
2. Select the best hit using fuzzy string matching (rapidfuzz).
3. If confidence ≥ threshold, scrape the song page for lyrics text.
4. Detect language with langdetect.
5. Return structured ``LyricsResult``.

Lyrics are scraped from the Genius HTML page because the Genius API
does not expose lyrics in a machine-readable field — only the web URL.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone

import httpx
import structlog
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
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

GENIUS_API_BASE = "https://api.genius.com"


class GeniusError(Exception):
    """Raised for non-retryable Genius API errors."""
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"Genius {status}: {message}")
        self.status = status


class GeniusRateLimit(Exception):
    """Raised on 429 for retry logic."""


@dataclass
class LyricsResult:
    """Container for fetched lyrics data."""
    text: str
    language: Optional[str]
    genius_url: str
    genius_song_id: int
    confidence_score: float
    fetched_at: datetime


class GeniusClient:
    """
    Async Genius API + HTML scraping client.

    Uses the API for search and metadata, then scrapes lyrics HTML.
    Rate-limited with a token bucket; all network errors retried.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._limiter = RateLimiter(
            rate=settings.genius_rate_limit_rps,
            capacity=settings.genius_rate_limit_rps * 3,
        )
        self._headers = {
            "Authorization": f"Bearer {settings.genius_access_token}",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        }
        self._scrape_headers = {
            "User-Agent": self._headers["User-Agent"],
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://genius.com/",
            "Connection": "keep-alive",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "upgrade-insecure-requests": "1",
        }
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            follow_redirects=True,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "GeniusClient":
        return self

    async def __aexit__(self, *_) -> None:  # type: ignore[no-untyped-def]
        await self.aclose()

    # ── Retry decorator ───────────────────────────────────────────────────────

    def _retry(self, func):  # type: ignore[no-untyped-def]
        return retry(
            retry=retry_if_exception_type(
                (GeniusRateLimit, httpx.TransportError, httpx.TimeoutException)
            ),
            stop=stop_after_attempt(self._settings.genius_max_retries),
            wait=wait_exponential_jitter(initial=2, max=30, jitter=3),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )(func)

    # ── Internal HTTP helpers ─────────────────────────────────────────────────

    async def _api_get(self, path: str, params: dict | None = None) -> dict:
        """Make a rate-limited Genius API call."""
        await self._limiter.acquire()
        url = f"{GENIUS_API_BASE}{path}"
        resp = await self._http.get(url, params=params, headers=self._headers)

        if resp.status_code == 429:
            logger.warning("genius_rate_limited")
            await asyncio.sleep(5)
            raise GeniusRateLimit()

        if resp.status_code >= 400:
            raise GeniusError(resp.status_code, resp.text[:200])

        return resp.json()

    async def _fetch_page(self, url: str) -> str:
        """Fetch a Genius song HTML page."""
        await self._limiter.acquire()
        resp = await self._http.get(url, headers=self._scrape_headers)
        if resp.status_code == 429:
            await asyncio.sleep(5)
            raise GeniusRateLimit()
        if resp.status_code >= 400:
            raise GeniusError(resp.status_code, f"Page fetch failed: {url}")
        return resp.text

    # ── Search ─────────────────────────────────────────────────────────────────

    async def search(self, track_name: str, artist_name: str) -> list:
        """
        Search Genius for a track.

        Returns raw list of hit dicts from the Genius search response.
        """
        query = f"{track_name} {artist_name}"

        async def _do_search() -> dict:
            return await self._api_get("/search", params={"q": query})

        try:
            resp = await self._retry(_do_search)()
            return resp.get("response", {}).get("hits", [])
        except (GeniusError, Exception) as exc:
            logger.warning("genius_search_failed", query=query, error=str(exc))
            return []

    # ── Fuzzy matching ────────────────────────────────────────────────────────

    @staticmethod
    def _compute_confidence(
        hit: dict,
        track_name: str,
        artist_name: str,
    ) -> float:
        """
        Compute match confidence for a Genius search hit.

        Weighted average of:
        - Title similarity (60%)
        - Artist similarity (40%)
        """
        result = hit.get("result", {})
        hit_title = result.get("title", "")
        hit_artist = result.get("primary_artist", {}).get("name", "")

        title_score = fuzz.token_sort_ratio(
            track_name.lower(), hit_title.lower()
        ) / 100.0
        artist_score = fuzz.token_sort_ratio(
            artist_name.lower(), hit_artist.lower()
        ) / 100.0

        return round(0.6 * title_score + 0.4 * artist_score, 4)

    # ── Lyrics scraping ───────────────────────────────────────────────────────

    @staticmethod
    def _extract_lyrics_from_html(html: str) -> str:
        """
        Parse lyrics from a Genius song page.

        Genius stores lyrics in ``<div data-lyrics-container="true">`` elements.
        Handles multiple containers (for songs with multiple sections).
        """
        soup = BeautifulSoup(html, "lxml")
        containers = soup.find_all("div", {"data-lyrics-container": "true"})
        if not containers:
            # Fallback: look for older page structure
            containers = soup.find_all("div", class_=re.compile(r"Lyrics__Container"))

        parts: list[str] = []
        for container in containers:
            # Replace <br> tags with newlines
            for br in container.find_all("br"):
                br.replace_with("\n")
            # Remove annotation links but keep their text
            for a in container.find_all("a"):
                a.unwrap()
            text = container.get_text(separator="\n")
            parts.append(text.strip())

        return "\n\n".join(parts).strip()

    @staticmethod
    def _detect_language(text: str) -> Optional[str]:
        """Detect lyrics language. Returns ISO 639-1 code or None."""
        try:
            from langdetect import detect, LangDetectException
            return detect(text[:500])
        except Exception:
            return None

    # ── Main public method ────────────────────────────────────────────────────

    async def get_lyrics(
        self,
        track_name: str,
        artist_name: str,
    ) -> Optional[LyricsResult]:
        """
        Search for and retrieve lyrics for a track.

        Args:
            track_name: Spotify track name.
            artist_name: Primary artist name.

        Returns:
            ``LyricsResult`` if a confident match is found, else ``None``.
        """
        hits = await self.search(track_name, artist_name)
        if not hits:
            logger.debug(
                "genius_no_hits",
                track=track_name,
                artist=artist_name,
            )
            return None

        # Find best match above confidence threshold
        best_hit = None
        best_confidence = 0.0

        for hit in hits[:5]:  # only consider top 5 results
            if hit.get("type") != "song":
                continue
            conf = self._compute_confidence(hit, track_name, artist_name)
            if conf > best_confidence:
                best_confidence = conf
                best_hit = hit

        if best_hit is None or best_confidence < self._settings.genius_min_confidence:
            logger.debug(
                "genius_low_confidence",
                track=track_name,
                artist=artist_name,
                confidence=best_confidence,
            )
            return None

        result = best_hit["result"]
        song_url: str = result.get("url", "")
        song_id: int = result.get("id", 0)

        if not song_url:
            return None

        # Scrape lyrics from the Genius page
        try:
            async def _fetch_page() -> str:
                return await self._fetch_page(song_url)

            html = await self._retry(_fetch_page)()
            lyrics_text = self._extract_lyrics_from_html(html)
        except Exception as exc:
            logger.warning(
                "genius_lyrics_scrape_failed",
                url=song_url,
                error=str(exc),
            )
            return None

        if not lyrics_text or len(lyrics_text) < 20:
            # Empty or trivially short lyrics — likely a scraping miss
            return None

        language = self._detect_language(lyrics_text)

        return LyricsResult(
            text=lyrics_text,
            language=language,
            genius_url=song_url,
            genius_song_id=song_id,
            confidence_score=best_confidence,
            fetched_at=datetime.now(timezone.utc),
        )
