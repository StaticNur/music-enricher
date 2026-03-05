"""
Base worker class.

All pipeline workers inherit from ``BaseWorker``, which provides:
- Infinite polling loop with configurable sleep between iterations
- Graceful shutdown via the shared ``shutdown_event``
- Per-iteration error catching so one bad batch never stops the worker
- Periodic stats logging
- Atomic document claiming via ``claim_batch`` (findOneAndUpdate)
- Lock timeout recovery — claims stale locks left by crashed workers
"""
from __future__ import annotations

import asyncio
import os
import socket
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import Settings
from app.db.collections import STATS_COL
from app.models.stats import STATS_DOC_ID
from app.utils.signals import shutdown_event

logger = structlog.get_logger(__name__)

# How long a worker may hold a lock before it's considered stale
LOCK_TIMEOUT_SECONDS = 300  # 5 minutes

# Unique identifier for this worker instance (container hostname + pid)
WORKER_INSTANCE_ID = f"{socket.gethostname()}:{os.getpid()}"


class BaseWorker(ABC):
    """
    Abstract base for all pipeline workers.

    Subclasses implement:
    - ``process_batch(batch)`` — process a list of documents
    - ``claim_batch()``        — query DB for the next batch to process

    The base loop handles polling, sleeping, shutdown, and error recovery.
    """

    def __init__(self, db: AsyncIOMotorDatabase, settings: Settings) -> None:  # type: ignore[type-arg]
        self.db = db
        self.settings = settings
        self.worker_id = WORKER_INSTANCE_ID
        self._iteration = 0
        self._last_stats_log = datetime.now(timezone.utc)

    # ── Abstract interface ────────────────────────────────────────────────────

    @abstractmethod
    async def claim_batch(self) -> List[Dict[str, Any]]:
        """
        Atomically claim up to ``settings.batch_size`` documents to process.

        Must use ``findOneAndUpdate`` or similar to prevent two workers
        claiming the same document (MongoDB-as-queue pattern).
        """
        ...

    @abstractmethod
    async def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process the claimed batch, updating document status in DB."""
        ...

    # ── Optional lifecycle hooks ──────────────────────────────────────────────

    async def on_startup(self) -> None:
        """Called once before the main loop starts. Override if needed."""

    async def on_shutdown(self) -> None:
        """Called once after the main loop exits. Override if needed."""

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Start the worker's infinite polling loop.

        Exits when ``shutdown_event`` is set (SIGTERM/SIGINT received).
        """
        await self.on_startup()
        logger.info("worker_started", worker_id=self.worker_id)

        while not shutdown_event.is_set():
            self._iteration += 1
            try:
                batch = await self.claim_batch()
                if not batch:
                    # No work available — sleep before next poll
                    await self._interruptible_sleep(self.settings.worker_sleep_sec)
                    continue

                logger.debug(
                    "batch_claimed",
                    size=len(batch),
                    iteration=self._iteration,
                )
                await self.process_batch(batch)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception(
                    "worker_iteration_error",
                    iteration=self._iteration,
                    error=str(exc),
                )
                # Brief sleep to avoid tight error loops
                await self._interruptible_sleep(self.settings.worker_sleep_sec)

            # Periodic stats logging
            await self._maybe_log_stats()

        await self.on_shutdown()
        logger.info("worker_stopped", worker_id=self.worker_id)

    async def _interruptible_sleep(self, seconds: int) -> None:
        """Sleep but wake immediately if shutdown is requested."""
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=float(seconds)
            )
        except asyncio.TimeoutError:
            pass

    async def _maybe_log_stats(self) -> None:
        """Log pipeline-wide stats every N minutes."""
        now = datetime.now(timezone.utc)
        interval = timedelta(minutes=self.settings.stats_log_interval_min)
        if now - self._last_stats_log < interval:
            return

        self._last_stats_log = now
        try:
            stats_doc = await self.db[STATS_COL].find_one({"doc_id": STATS_DOC_ID})
            if stats_doc:
                stats_doc.pop("_id", None)
                logger.info("pipeline_stats", **stats_doc)
        except Exception as exc:
            logger.warning("stats_log_failed", error=str(exc))

    # ── Shared DB helpers ─────────────────────────────────────────────────────

    async def increment_stat(self, field: str, amount: int = 1) -> None:
        """Atomically increment a field in the system_stats document."""
        try:
            await self.db[STATS_COL].update_one(
                {"doc_id": STATS_DOC_ID},
                {
                    "$inc": {field: amount},
                    "$setOnInsert": {
                        "doc_id": STATS_DOC_ID,
                        "started_at": datetime.now(timezone.utc),
                    },
                    "$set": {"last_stats_at": datetime.now(timezone.utc)},
                },
                upsert=True,
            )
        except Exception as exc:
            logger.warning("increment_stat_failed", field=field, error=str(exc))

    @staticmethod
    def stale_lock_query() -> Dict[str, Any]:
        """
        MongoDB filter for reclaiming stale locks.

        A lock is stale if ``locked_at`` is older than LOCK_TIMEOUT_SECONDS.
        This lets the system recover from crashed workers without manual
        intervention.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        return {
            "status": "processing",
            "locked_at": {"$lt": cutoff},
        }
