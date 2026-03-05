"""
Artist data models.

Artists are stored separately so multiple tracks can share the same
artist info without redundancy. The quality_worker looks up
``artist_followers`` to compute quality scores.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pydantic import BaseModel, Field


class ArtistDocument(BaseModel):
    """MongoDB document for a Spotify artist."""

    spotify_id: str
    name: str
    genres: List[str] = Field(default_factory=list)
    followers: int = 0
    popularity: int = 0
    image_url: Optional[str] = None

    # ── Timestamps ────────────────────────────────────────────────────────────
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        return data

    @classmethod
    def from_mongo(cls, doc: dict) -> "ArtistDocument":
        if "_id" in doc:
            doc = dict(doc)
            doc.pop("_id", None)
        return cls.model_validate(doc)
