"""
Queue item models for playlists, artists, and genres.

These collections act as work queues without Redis — workers use
atomic findOneAndUpdate to claim items, process them, and mark them done.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class QueueStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


class PlaylistQueueItem(BaseModel):
    """A Spotify playlist pending track extraction."""

    spotify_id: str
    name: Optional[str] = None
    followers: int = 0
    tracks_total: int = 0
    status: QueueStatus = QueueStatus.PENDING
    retry_count: int = 0

    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        data["status"] = self.status.value
        return data


class ArtistQueueItem(BaseModel):
    """A Spotify artist pending album/track expansion."""

    spotify_id: str
    name: Optional[str] = None
    status: QueueStatus = QueueStatus.PENDING
    retry_count: int = 0

    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        data["status"] = self.status.value
        return data


class GenreQueueItem(BaseModel):
    """
    A genre pending Spotify search iteration.

    ``offset`` tracks pagination — when a worker resumes, it continues
    from where it left off rather than re-scanning from offset 0.
    """

    genre: str
    offset: int = 0
    status: QueueStatus = QueueStatus.PENDING
    retry_count: int = 0

    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        data["status"] = self.status.value
        return data
