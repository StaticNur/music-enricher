"""
MongoDB connection management.

Provides a singleton async Motor client with a clean connect/close lifecycle.
The ``Database`` instance is passed explicitly to all components — no globals.
"""
from __future__ import annotations

import asyncio
from typing import Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core.config import Settings

logger = structlog.get_logger(__name__)


class MongoDB:
    """
    Async MongoDB connection wrapper.

    Usage:
        mongo = MongoDB(settings)
        await mongo.connect()
        db = mongo.db
        ...
        await mongo.close()
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: Optional[AsyncIOMotorClient] = None  # type: ignore[type-arg]
        self._db: Optional[AsyncIOMotorDatabase] = None  # type: ignore[type-arg]

    async def connect(self) -> None:
        """Open connection pool and verify server is reachable."""
        self._client = AsyncIOMotorClient(
            self._settings.mongodb_uri,
            maxPoolSize=self._settings.mongodb_max_pool_size,
            serverSelectionTimeoutMS=30_000,
            connectTimeoutMS=10_000,
            socketTimeoutMS=60_000,
        )
        self._db = self._client[self._settings.mongodb_db]

        # Verify connectivity
        await self._client.admin.command("ping")
        logger.info(
            "mongodb_connected",
            uri=self._settings.mongodb_uri,
            db=self._settings.mongodb_db,
        )

    async def close(self) -> None:
        """Close the connection pool gracefully."""
        if self._client is not None:
            self._client.close()
            logger.info("mongodb_closed")

    @property
    def db(self) -> AsyncIOMotorDatabase:  # type: ignore[type-arg]
        if self._db is None:
            raise RuntimeError("MongoDB.connect() has not been called.")
        return self._db


async def wait_for_mongo(settings: Settings, max_attempts: int = 30) -> MongoDB:
    """
    Retry connecting to MongoDB until it becomes available.

    Useful at container startup when mongo may still be initializing.
    """
    mongo = MongoDB(settings)
    for attempt in range(1, max_attempts + 1):
        try:
            await mongo.connect()
            return mongo
        except Exception as exc:
            logger.warning(
                "mongodb_not_ready",
                attempt=attempt,
                max_attempts=max_attempts,
                error=str(exc),
            )
            if attempt == max_attempts:
                raise
            await asyncio.sleep(3)
    return mongo  # unreachable, satisfies type checker
