"""
Pipeline entrypoint.

Selects and runs the appropriate worker based on the ``WORKER_TYPE``
environment variable. All workers share the same Docker image.

Available worker types:
    seeder
    playlist_worker
    artist_worker
    genre_worker
    audio_features_worker
    lyrics_worker
    quality_worker
    exporter
    # v2 additions (regional + MusicBrainz):
    musicbrainz_worker
    regional_seed_worker
    language_worker
    transliteration_worker

Usage:
    WORKER_TYPE=playlist_worker python -m app.entrypoint
"""
from __future__ import annotations

import asyncio
import os
import sys

from app.core.config import get_settings
from app.core.logging_config import configure_logging, get_logger
from app.db.collections import ensure_indexes
from app.db.mongodb import wait_for_mongo
from app.utils.signals import install_signal_handlers

logger = get_logger(__name__)

WORKER_TYPES = {
    # v1 core pipeline
    "seeder",
    "playlist_worker",
    "artist_worker",
    "genre_worker",
    "audio_features_worker",
    "lyrics_worker",
    "quality_worker",
    "exporter",
    # v2 regional + MusicBrainz extension
    "musicbrainz_worker",
    "regional_seed_worker",
    "language_worker",
    "transliteration_worker",
}


async def _run_worker(worker_type: str) -> None:
    """
    Bootstrap the requested worker and run it until completion or shutdown.
    """
    settings = get_settings()

    # Connect to MongoDB (retries until available)
    mongo = await wait_for_mongo(settings)
    db = mongo.db

    # Ensure all indexes exist (idempotent)
    await ensure_indexes(db)

    # Install SIGTERM / SIGINT handlers for graceful shutdown
    install_signal_handlers()

    try:
        if worker_type == "seeder":
            from app.workers.seeder import SeederWorker
            worker = SeederWorker(db, settings)
            await worker.run()

        elif worker_type == "playlist_worker":
            from app.workers.playlist_worker import PlaylistWorker
            worker = PlaylistWorker(db, settings)
            await worker.run()

        elif worker_type == "artist_worker":
            from app.workers.artist_worker import ArtistWorker
            worker = ArtistWorker(db, settings)
            await worker.run()

        elif worker_type == "genre_worker":
            from app.workers.genre_worker import GenreWorker
            worker = GenreWorker(db, settings)
            await worker.run()

        elif worker_type == "audio_features_worker":
            from app.workers.audio_features_worker import AudioFeaturesWorker
            worker = AudioFeaturesWorker(db, settings)
            await worker.run()

        elif worker_type == "lyrics_worker":
            from app.workers.lyrics_worker import LyricsWorker
            worker = LyricsWorker(db, settings)
            await worker.run()

        elif worker_type == "quality_worker":
            from app.workers.quality_worker import QualityWorker
            worker = QualityWorker(db, settings)
            await worker.run()

        elif worker_type == "exporter":
            from app.workers.exporter import ExporterWorker
            worker = ExporterWorker(db, settings)
            await worker.run()

        # ── v2: regional + MusicBrainz workers ───────────────────────────────

        elif worker_type == "musicbrainz_worker":
            from app.workers.musicbrainz_worker import MusicBrainzWorker
            worker = MusicBrainzWorker(db, settings)
            await worker.run()

        elif worker_type == "regional_seed_worker":
            from app.workers.regional_seed_worker import RegionalSeedWorker
            worker = RegionalSeedWorker(db, settings)
            await worker.run()

        elif worker_type == "language_worker":
            from app.workers.language_worker import LanguageWorker
            worker = LanguageWorker(db, settings)
            await worker.run()

        elif worker_type == "transliteration_worker":
            from app.workers.transliteration_worker import TransliterationWorker
            worker = TransliterationWorker(db, settings)
            await worker.run()

        else:
            logger.error("unknown_worker_type", worker_type=worker_type)
            sys.exit(1)

    finally:
        await mongo.close()


def main() -> None:
    worker_type = os.environ.get("WORKER_TYPE", "").strip().lower()

    if not worker_type:
        print(
            "ERROR: WORKER_TYPE environment variable is not set.\n"
            f"Available types: {', '.join(sorted(WORKER_TYPES))}",
            file=sys.stderr,
        )
        sys.exit(1)

    if worker_type not in WORKER_TYPES:
        print(
            f"ERROR: Unknown WORKER_TYPE '{worker_type}'.\n"
            f"Available types: {', '.join(sorted(WORKER_TYPES))}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Configure structured JSON logging before anything else
    configure_logging(worker_name=worker_type)

    logger.info("starting_worker", worker_type=worker_type)

    asyncio.run(_run_worker(worker_type))


if __name__ == "__main__":
    main()
