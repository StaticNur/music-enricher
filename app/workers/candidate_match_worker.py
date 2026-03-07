"""
Candidate Match Worker.

Reads unprocessed ``track_candidates`` (discovered by lastfm_worker,
ytmusic_worker, discogs_worker), searches Spotify or Deezer for each one,
and inserts high-confidence matches into the main ``tracks`` collection with
``status=base_collected``.

Match scoring (weighted average):
    60% title similarity  (rapidfuzz token_sort_ratio)
    40% artist similarity (rapidfuzz token_sort_ratio)

Only candidates with ``confidence >= CANDIDATE_MATCH_CONFIDENCE`` (default 0.8)
are inserted. Others are marked processed without a track insert.

Source priority:
    1. Spotify (if ``SPOTIFY_ENABLED=true``) — returns real Spotify IDs.
    2. Deezer search (fallback, or primary when ``SPOTIFY_ENABLED=false``) —
       inserts with ``deezer:{id}`` placeholder; real spotify_id is backfilled
       later by artist_graph_worker / deezer_direct_worker.

When a candidate has a ``youtube_video_id`` (from ytmusic_worker), it is
stored on the track document under the ``youtube`` sub-document.

Deduplication against existing tracks uses the same ISRC/fingerprint pattern
as all other workers — the upsert is a no-op if the track already exists.

Horizontally scalable: multiple replicas compete safely via optimistic locking
(``locked_at`` field on candidates).
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase
from rapidfuzz import fuzz

from app.core.config import Settings
from app.db.collections import TRACK_CANDIDATES_COL, TRACKS_COL, ARTIST_QUEUE_COL
from app.models.candidate import CandidateSource, QueueStatus
from app.models.track import (
    TrackDocument,
    TrackStatus,
    ArtistRef,
    AlbumRef,
    YoutubeData,
)
from app.services.deezer import DeezerClient
from app.services.spotify import SpotifyClient, SpotifyError
from app.utils.deduplication import (
    compute_candidate_fingerprint,
    compute_fingerprint,
    extract_isrc,
    normalize_text,
)
from app.workers.base import BaseWorker, WORKER_INSTANCE_ID, LOCK_TIMEOUT_SECONDS

logger = structlog.get_logger(__name__)

_SEARCH_LIMIT = 5  # candidates to score per search query


class CandidateMatchWorker(BaseWorker):
    """
    Matches external candidates to Spotify (primary) or Deezer (fallback)
    tracks and inserts them into the main enrichment pipeline.

    Locking: uses ``locked_at`` / ``locked_by`` on ``track_candidates``
    documents (same pattern as other workers but without a ``status`` enum —
    candidates use a ``processed`` boolean instead).
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        super().__init__(db, settings)
        self._spotify: Optional[SpotifyClient] = None
        self._deezer: Optional[DeezerClient] = None

    async def on_startup(self) -> None:
        if self.settings.spotify_enabled:
            self._spotify = SpotifyClient(self.settings)
        self._deezer = DeezerClient(self.settings)
        logger.info(
            "candidate_match_worker_started",
            spotify_enabled=self.settings.spotify_enabled,
        )

    async def on_shutdown(self) -> None:
        if self._spotify:
            await self._spotify.aclose()
        if self._deezer:
            await self._deezer.aclose()

    # ── Queue claiming ────────────────────────────────────────────────────────

    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Claim up to ``batch_size`` unprocessed candidates.

        Uses ``locked_at`` for stale-lock recovery (5-minute timeout, same as
        BaseWorker) rather than the ``status`` enum used by queue collections.
        """
        col = self.db[TRACK_CANDIDATES_COL]
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        batch = []

        for _ in range(self.settings.batch_size):
            doc = await col.find_one_and_update(
                {
                    "processed": False,
                    "$or": [
                        {"locked_at": None},
                        {"locked_at": {"$lt": cutoff}},
                    ],
                },
                {
                    "$set": {
                        "locked_at": now,
                        "locked_by": WORKER_INSTANCE_ID,
                        "updated_at": now,
                    }
                },
                return_document=True,
            )
            if doc is None:
                break
            batch.append(doc)

        return batch

    # ── Processing ────────────────────────────────────────────────────────────

    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        for candidate in batch:
            await self._process_candidate(candidate)

    async def _process_candidate(self, candidate: Dict[str, Any]) -> None:
        """
        Search Spotify (primary) then Deezer (fallback) for the candidate,
        insert if confident, mark processed.
        """
        col = self.db[TRACK_CANDIDATES_COL]
        now = datetime.now(timezone.utc)
        candidate_id = candidate["_id"]

        title: str = candidate.get("title") or ""
        artist: str = candidate.get("artist") or ""

        try:
            track_doc: Optional[TrackDocument] = None
            match_source = ""

            # ── 1. Try Spotify ────────────────────────────────────────────────
            if self._spotify is not None:
                spotify_raw = await self._find_spotify_match(title, artist)
                if spotify_raw is not None:
                    track_doc = self._parse_spotify_track(spotify_raw)
                    match_source = "spotify"

            # ── 2. Fallback to Deezer ─────────────────────────────────────────
            if track_doc is None and self._deezer is not None:
                deezer_raw = await self._find_deezer_match(title, artist)
                if deezer_raw is not None:
                    track_doc = self._parse_deezer_track(deezer_raw)
                    match_source = "deezer"

            matched_id: Optional[str] = None
            if track_doc is not None:
                # Attach YouTube video ID from the candidate if present
                youtube_video_id: Optional[str] = candidate.get("youtube_video_id")
                if youtube_video_id:
                    track_doc.youtube = YoutubeData(
                        video_id=youtube_video_id,
                        confidence=1.0,
                        source=(
                            "ytmusic"
                            if candidate.get("source") == CandidateSource.YTMUSIC.value
                            else "search"
                        ),
                    )

                inserted = await self._upsert_track(track_doc)
                matched_id = track_doc.spotify_id

                if inserted:
                    await self.increment_stat("total_discovered", 1)
                    await self.increment_stat("total_base_collected", 1)
                    await self.increment_stat("candidates_matched", 1)
                    # Only enqueue to artist_queue when we have a real Spotify ID
                    if match_source == "spotify":
                        for art in track_doc.artists:
                            await self._enqueue_artist(art.spotify_id, art.name)
                else:
                    if youtube_video_id:
                        await self._maybe_set_youtube(
                            track_doc.spotify_id, youtube_video_id,
                            candidate.get("source", "")
                        )

            # Mark candidate as processed
            await col.update_one(
                {"_id": candidate_id},
                {"$set": {
                    "processed": True,
                    "matched_spotify_id": matched_id,
                    "locked_at": None,
                    "locked_by": None,
                    "updated_at": now,
                }},
            )

        except Exception as exc:
            logger.error(
                "candidate_match_failed",
                title=title, artist=artist,
                error=str(exc), exc_info=True,
            )
            retry_count = candidate.get("retry_count", 0) + 1
            processed = retry_count >= self.settings.worker_retry_limit
            await col.update_one(
                {"_id": candidate_id},
                {"$set": {
                    "processed": processed,
                    "retry_count": retry_count,
                    "locked_at": None,
                    "locked_by": None,
                    "updated_at": now,
                }},
            )

    # ── Search + scoring helpers ──────────────────────────────────────────────

    def _best_match(
        self,
        norm_title: str,
        norm_artist: str,
        items: List[Dict[str, Any]],
        get_title: Any,
        get_artist: Any,
    ) -> tuple[Optional[Dict[str, Any]], float]:
        """Score a list of search results and return (best_item, best_score)."""
        threshold = self.settings.candidate_match_confidence
        best: Optional[Dict[str, Any]] = None
        best_score = 0.0
        for item in items:
            t_sim = fuzz.token_sort_ratio(norm_title, normalize_text(get_title(item))) / 100.0
            a_sim = fuzz.token_sort_ratio(norm_artist, normalize_text(get_artist(item))) / 100.0
            score = 0.6 * t_sim + 0.4 * a_sim
            if score > best_score:
                best_score = score
                best = item
        if best is None or best_score < threshold:
            return None, best_score
        return best, best_score

    async def _find_spotify_match(
        self, title: str, artist: str
    ) -> Optional[Dict[str, Any]]:
        """Search Spotify, return best-matching track dict or ``None``."""
        assert self._spotify is not None
        query = f"{artist} {title}"
        try:
            resp = await self._spotify.search_tracks(query, limit=_SEARCH_LIMIT)
        except SpotifyError as exc:
            logger.warning("spotify_search_error", query=query, error=str(exc))
            return None

        items = [i for i in (resp.get("tracks", {}).get("items") or []) if i and i.get("id")]
        if not items:
            return None

        best, score = self._best_match(
            normalize_text(title), normalize_text(artist), items,
            get_title=lambda i: i.get("name") or "",
            get_artist=lambda i: (i.get("artists") or [{}])[0].get("name") or "",
        )
        if best:
            logger.debug("candidate_matched_spotify", title=title, artist=artist, confidence=round(score, 3))
        return best

    async def _find_deezer_match(
        self, title: str, artist: str
    ) -> Optional[Dict[str, Any]]:
        """Search Deezer, return best-matching track dict or ``None``."""
        assert self._deezer is not None
        query = f"{artist} {title}"
        items = await self._deezer.search_tracks(query, limit=_SEARCH_LIMIT)
        if not items:
            return None

        best, score = self._best_match(
            normalize_text(title), normalize_text(artist), items,
            get_title=lambda i: i.get("title") or i.get("title_short") or "",
            get_artist=lambda i: (i.get("artist") or {}).get("name") or "",
        )
        if best:
            logger.debug("candidate_matched_deezer", title=title, artist=artist, confidence=round(score, 3))
        return best

    # ── Track parsing (mirrors playlist_worker / genre_worker pattern) ────────

    def _parse_spotify_track(self, raw: Dict[str, Any]) -> Optional[TrackDocument]:
        """
        Convert a raw Spotify track dict (from search) into a TrackDocument.
        Returns ``None`` if required fields are missing.
        """
        spotify_id: str = raw.get("id") or ""
        name: str = (raw.get("name") or "").strip()
        if not spotify_id or not name:
            return None

        artists = [
            ArtistRef(spotify_id=a["id"], name=a.get("name") or "")
            for a in raw.get("artists", [])
            if a.get("id")
        ]
        if not artists:
            return None

        album_raw = raw.get("album") or {}
        album: Optional[AlbumRef] = None
        if album_raw.get("id"):
            album = AlbumRef(
                spotify_id=album_raw["id"],
                name=album_raw.get("name") or "",
                release_date=album_raw.get("release_date"),
                album_type=album_raw.get("album_type"),
                total_tracks=album_raw.get("total_tracks"),
                images=album_raw.get("images", []),
            )

        isrc = extract_isrc(raw)
        duration_ms = raw.get("duration_ms", 0)
        fp = compute_fingerprint(name, artists[0].spotify_id, duration_ms)
        markets_list = raw.get("available_markets") or []

        return TrackDocument(
            spotify_id=spotify_id,
            isrc=isrc,
            fingerprint=fp,
            name=name,
            artists=artists,
            album=album,
            popularity=raw.get("popularity", 0),
            duration_ms=duration_ms,
            explicit=raw.get("explicit", False),
            markets_count=len(markets_list),
            markets=markets_list,
            status=TrackStatus.BASE_COLLECTED,
            appearance_score=1,
        )

    def _parse_deezer_track(self, raw: Dict[str, Any]) -> Optional[TrackDocument]:
        """
        Convert a raw Deezer search result into a TrackDocument.

        Uses ``deezer:{id}`` placeholders — the real Spotify ID is backfilled
        later by artist_graph_worker or deezer_direct_worker on ISRC match.
        """
        track_id = raw.get("id")
        title = (raw.get("title") or raw.get("title_short") or "").strip()
        if not track_id or not title:
            return None

        artist_raw = raw.get("artist") or {}
        artist_name = (artist_raw.get("name") or "").strip()
        if not artist_name:
            return None

        deezer_artist_id = artist_raw.get("id", 0)
        artist_ref = ArtistRef(
            spotify_id=f"deezer:{deezer_artist_id}",
            name=artist_name,
        )

        album_raw = raw.get("album") or {}
        album: Optional[AlbumRef] = None
        if album_raw.get("id"):
            album = AlbumRef(
                spotify_id=f"deezer_album:{album_raw['id']}",
                name=album_raw.get("title") or album_raw.get("name") or "",
                images=[{"url": album_raw["cover"]}] if album_raw.get("cover") else [],
            )

        duration_ms = (raw.get("duration") or 0) * 1000
        isrc: Optional[str] = raw.get("isrc") or None
        fp = compute_candidate_fingerprint(title, artist_name, duration_ms)

        return TrackDocument(
            spotify_id=f"deezer:{track_id}",
            isrc=isrc,
            fingerprint=fp,
            name=title,
            artists=[artist_ref],
            album=album,
            popularity=(raw.get("rank") or 0) // 10000,
            duration_ms=duration_ms,
            explicit=raw.get("explicit_lyrics") or False,
            markets_count=0,
            markets=[],
            status=TrackStatus.BASE_COLLECTED,
            appearance_score=1,
        )

    # ── DB helpers ────────────────────────────────────────────────────────────

    async def _upsert_track(self, track: TrackDocument) -> bool:
        """
        Insert track if new; increment appearance_score if duplicate.
        Returns ``True`` if the document was newly inserted.
        """
        col = self.db[TRACKS_COL]
        now = datetime.now(timezone.utc)
        doc = track.to_mongo()

        filter_q = (
            {"isrc": track.isrc} if track.isrc else {"fingerprint": track.fingerprint}
        )
        insert_doc = {
            k: v for k, v in doc.items()
            if k not in ("created_at", "updated_at", "appearance_score")
        }

        try:
            result = await col.update_one(
                filter_q,
                {
                    "$setOnInsert": {**insert_doc, "created_at": now},
                    "$inc": {"appearance_score": 1},
                    "$set": {"updated_at": now},
                },
                upsert=True,
            )
            # Backfill real spotify_id if existing doc has a deezer: placeholder
            if result.upserted_id is None:
                await col.update_one(
                    {**filter_q, "spotify_id": {"$regex": "^deezer:"}},
                    {"$set": {"spotify_id": track.spotify_id, "updated_at": now}},
                )
            return result.upserted_id is not None
        except Exception as exc:
            if "duplicate key" in str(exc).lower():
                # Race between workers — still bump appearance_score
                await col.update_one(
                    {"spotify_id": track.spotify_id},
                    {"$inc": {"appearance_score": 1}, "$set": {"updated_at": now}},
                )
                return False
            logger.error(
                "candidate_track_upsert_error",
                spotify_id=track.spotify_id, error=str(exc),
            )
            return False

    async def _maybe_set_youtube(
        self, spotify_id: str, video_id: str, source: str
    ) -> None:
        """
        Set ``youtube`` on an existing track if not already present.
        Uses ``$setOnInsert``-style logic via ``$exists`` check.
        """
        col = self.db[TRACKS_COL]
        try:
            await col.update_one(
                {"spotify_id": spotify_id, "youtube": {"$exists": False}},
                {"$set": {
                    "youtube": {
                        "video_id": video_id,
                        "confidence": 1.0,
                        "source": "ytmusic" if source == CandidateSource.YTMUSIC.value else "search",
                    },
                    "updated_at": datetime.now(timezone.utc),
                }},
            )
        except Exception as exc:
            logger.debug("youtube_set_failed", spotify_id=spotify_id, error=str(exc))

    async def _enqueue_artist(self, artist_id: str, name: str) -> None:
        """Add artist to artist_queue for discography expansion."""
        col = self.db[ARTIST_QUEUE_COL]
        try:
            await col.update_one(
                {"spotify_id": artist_id},
                {"$setOnInsert": {
                    "spotify_id": artist_id,
                    "name": name,
                    "status": QueueStatus.PENDING.value,
                    "retry_count": 0,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            )
        except Exception:
            pass
