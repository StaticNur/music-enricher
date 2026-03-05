"""
Exporter Worker.

Exports all tracks with status=enriched to:
- JSONL  (one JSON object per line, UTF-8)
- CSV    (flat with selected columns)

Output files are written to ``settings.export_dir`` with a timestamp
in the filename. The exporter runs once, writes the export, and exits.

The export is paginated (cursor-based) to handle millions of tracks
without loading everything into memory.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import Settings
from app.db.collections import TRACKS_COL
from app.models.track import TrackStatus

logger = structlog.get_logger(__name__)

# CSV columns to include in flat export (nested fields are serialized as JSON)
CSV_COLUMNS = [
    "spotify_id",
    "isrc",
    "name",
    "artists_names",       # flattened: "Artist A, Artist B"
    "primary_artist_id",
    "album_name",
    "album_type",
    "release_date",
    "popularity",
    "duration_ms",
    "explicit",
    "markets_count",
    "appearance_score",
    "quality_score",
    "artist_followers",
    "has_audio_features",
    "danceability",
    "energy",
    "key",
    "loudness",
    "mode",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
    "tempo",
    "time_signature",
    "has_lyrics",
    "lyrics_language",
    "genius_url",
    "lyrics_confidence",
    "status",
    "created_at",
    "updated_at",
]


def _flatten_track(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten a MongoDB track document into a CSV-friendly dict.

    Nested objects (artists, album, audio_features, lyrics) are
    decomposed into scalar columns.
    """
    artists = doc.get("artists") or []
    album = doc.get("album") or {}
    af = doc.get("audio_features") or {}
    lyrics = doc.get("lyrics") or {}

    return {
        "spotify_id": doc.get("spotify_id", ""),
        "isrc": doc.get("isrc", ""),
        "name": doc.get("name", ""),
        "artists_names": ", ".join(a.get("name", "") for a in artists),
        "primary_artist_id": artists[0].get("spotify_id", "") if artists else "",
        "album_name": album.get("name", ""),
        "album_type": album.get("album_type", ""),
        "release_date": album.get("release_date", ""),
        "popularity": doc.get("popularity", 0),
        "duration_ms": doc.get("duration_ms", 0),
        "explicit": doc.get("explicit", False),
        "markets_count": doc.get("markets_count", 0),
        "appearance_score": doc.get("appearance_score", 0),
        "quality_score": doc.get("quality_score", 0.0),
        "artist_followers": doc.get("artist_followers", 0),
        "has_audio_features": bool(af),
        "danceability": af.get("danceability"),
        "energy": af.get("energy"),
        "key": af.get("key"),
        "loudness": af.get("loudness"),
        "mode": af.get("mode"),
        "speechiness": af.get("speechiness"),
        "acousticness": af.get("acousticness"),
        "instrumentalness": af.get("instrumentalness"),
        "liveness": af.get("liveness"),
        "valence": af.get("valence"),
        "tempo": af.get("tempo"),
        "time_signature": af.get("time_signature"),
        "has_lyrics": bool(lyrics.get("text")),
        "lyrics_language": lyrics.get("language"),
        "genius_url": lyrics.get("genius_url"),
        "lyrics_confidence": lyrics.get("confidence_score"),
        "status": doc.get("status", ""),
        "created_at": str(doc.get("created_at", "")),
        "updated_at": str(doc.get("updated_at", "")),
    }


class ExporterWorker:
    """
    One-shot exporter — not a ``BaseWorker`` subclass.

    Reads all enriched tracks in batches and writes two output files.
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        self.db = db
        self.settings = settings

    async def run(self) -> None:
        """Run the export and exit."""
        export_dir = Path(self.settings.export_dir)
        export_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        jsonl_path = export_dir / f"tracks_{timestamp}.jsonl"
        csv_path = export_dir / f"tracks_{timestamp}.csv"

        logger.info("export_started", jsonl=str(jsonl_path), csv=str(csv_path))

        total = 0
        try:
            async with (
                aiofiles.open(jsonl_path, "w", encoding="utf-8") as jsonl_file,
                aiofiles.open(csv_path, "w", encoding="utf-8", newline="") as csv_file,
            ):
                # Write CSV header
                header_row = ",".join(CSV_COLUMNS) + "\n"
                await csv_file.write(header_row)

                # Stream documents in batches
                async for batch in self._iter_enriched_batches():
                    for doc in batch:
                        # JSONL: write full document (strip _id)
                        doc.pop("_id", None)
                        # Convert datetimes to ISO strings for JSON serialization
                        json_doc = self._make_json_serializable(doc)
                        await jsonl_file.write(json.dumps(json_doc, ensure_ascii=False) + "\n")

                        # CSV: write flattened row
                        flat = _flatten_track(doc)
                        csv_row = self._build_csv_row(flat)
                        await csv_file.write(csv_row + "\n")

                        total += 1

                    logger.info("export_progress", exported=total)

        except Exception as exc:
            logger.error("export_failed", error=str(exc), exc_info=True)
            raise

        logger.info(
            "export_completed",
            total=total,
            jsonl=str(jsonl_path),
            csv=str(csv_path),
        )

    async def _iter_enriched_batches(self):
        """Cursor-based iteration over enriched tracks in memory-safe batches."""
        col = self.db[TRACKS_COL]
        batch_size = self.settings.export_batch_size
        skip = 0

        while True:
            batch = []
            cursor = (
                col.find({"status": TrackStatus.ENRICHED.value})
                .sort("quality_score", -1)  # highest quality first
                .skip(skip)
                .limit(batch_size)
            )
            async for doc in cursor:
                batch.append(doc)

            if not batch:
                break

            yield batch
            skip += batch_size

            if len(batch) < batch_size:
                break

    @staticmethod
    def _make_json_serializable(obj: Any) -> Any:
        """Recursively convert non-JSON-serializable types."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: ExporterWorker._make_json_serializable(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [ExporterWorker._make_json_serializable(i) for i in obj]
        return obj

    @staticmethod
    def _build_csv_row(flat: Dict[str, Any]) -> str:
        """Build a CSV row string from a flattened track dict."""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writerow(flat)
        return buf.getvalue().rstrip("\r\n")
