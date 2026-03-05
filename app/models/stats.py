"""
System statistics model.

A single document in the ``system_stats`` collection tracks pipeline-wide
counters. It is upserted (not appended) so there is always exactly one record.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field

STATS_DOC_ID = "global"


class SystemStats(BaseModel):
    """Pipeline-wide counters used for observability and monitoring."""

    doc_id: str = STATS_DOC_ID          # singleton document key

    # ── Discovery ────────────────────────────────────────────────────────────
    total_discovered: int = 0

    # ── Enrichment stages ────────────────────────────────────────────────────
    total_base_collected: int = 0
    audio_features_added: int = 0
    audio_features_unavailable: int = 0
    lyrics_added: int = 0
    lyrics_not_found: int = 0

    # ── Final outcomes ────────────────────────────────────────────────────────
    total_enriched: int = 0
    total_filtered_out: int = 0
    total_failed: int = 0

    # ── Queue sizes ───────────────────────────────────────────────────────────
    playlists_seeded: int = 0
    genres_seeded: int = 0
    artists_queued: int = 0

    # ── Timing ────────────────────────────────────────────────────────────────
    started_at: Optional[datetime] = None
    last_stats_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_mongo(self) -> dict:
        data = self.model_dump(mode="python")
        return data

    @classmethod
    def from_mongo(cls, doc: dict) -> "SystemStats":
        if "_id" in doc:
            doc = dict(doc)
            doc.pop("_id", None)
        return cls.model_validate(doc)
