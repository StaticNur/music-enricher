# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

An autonomous music metadata enrichment pipeline that collects and enriches 1M+ tracks via the Spotify and Genius APIs, storing everything in MongoDB. All workers share a single Docker image; the `WORKER_TYPE` env var selects which worker runs.

## Run tests

```bash
python -m pytest tests/ -v
```

## Commands

### Run locally (single worker, no Docker)

```bash
# Activate venv
source .venv/Scripts/activate   # Windows bash
# or
.venv/Scripts/python -m ...     # direct

# Run a specific worker
WORKER_TYPE=seeder python -m app.entrypoint
WORKER_TYPE=playlist_worker python -m app.entrypoint
```

### Docker (normal workflow)

```bash
# Full stack
docker-compose up -d

# Specific worker logs
docker-compose logs -f playlist-worker

# Run exporter once
docker-compose run --rm exporter

# Re-seed without duplicating (seeder uses $setOnInsert)
docker-compose restart seeder
```

### MongoDB inspection

```bash
# Pipeline counters
docker-compose exec mongo mongosh music_enricher --eval \
  'db.system_stats.findOne({doc_id: "global"}, {_id: 0})'

# Track counts by status
docker-compose exec mongo mongosh music_enricher --eval \
  'db.tracks.aggregate([{$group:{_id:"$status", count:{$sum:1}}}]).toArray()'
```

### Install dependencies

```bash
pip install -r requirements.txt
```

## Architecture

### Single-image, multi-worker design

All workers are built from the same `Dockerfile`. `app/entrypoint.py` reads `WORKER_TYPE` and imports + runs the appropriate worker class. Every worker inherits from `BaseWorker` (`app/workers/base.py`), which provides the polling loop, optimistic locking, graceful shutdown (SIGTERM), and stale-lock recovery.

### Track state machine (v1 pipeline)

```
base_collected -> audio_features_added -> lyrics_added -> enriched | filtered_out | failed
```

Workers claim batches atomically with `findOneAndUpdate` (MongoDB-as-queue, no Redis). Stale locks (>5 min) are auto-reclaimed.

### v3 external-source discovery pipeline

Four new workers that extend track discovery beyond Spotify:

```
lastfm_worker   →  track_candidates  ←  ytmusic_worker
discogs_worker  →  track_candidates  ←  (all three sources)
                         ↓
                candidate_match_worker
                         ↓ (Spotify search + confidence ≥ 0.8)
                       tracks  (status=base_collected)
                         ↓
                  existing v1 pipeline continues
```

New collections: `track_candidates`, `lastfm_seed_queue`, `ytmusic_seed_queue`, `discogs_seed_queue`

- `lastfm_worker` / `discogs_worker` — self-seeding, paginating (page tracked per queue item)
- `ytmusic_worker` — self-seeding, one-shot per query item, stores `youtube_video_id` per candidate
- `candidate_match_worker` — searches Spotify for candidates, scores title+artist similarity (60/40), threshold `CANDIDATE_MATCH_CONFIDENCE=0.8`; attaches `youtube_video_id` to matched tracks

Deduplication for candidates: `compute_candidate_fingerprint(title, artist, duration_ms?)` in `app/utils/deduplication.py`. Different from the Spotify fingerprint (uses artist name not artist ID).

Critical constraints:
- `discogs_worker`: **single replica only** — 1 req/s strict, Discogs bans on excess
- `musicbrainz_worker`: same single-replica rule (existing v2)
- All three discovery workers are horizontally safe except Discogs

### v2 supplementary workers (flag-driven, non-blocking)

These run orthogonally — they never change `status`, only set their own boolean completion flags:

| Worker | Flag set | What it does |
|---|---|---|
| `language_worker` | `language_detected` | Detects language/script via MB → langdetect → script heuristic |
| `transliteration_worker` | `transliteration_done` | Converts Cyrillic/Arabic names to Latin |
| `musicbrainz_worker` | `musicbrainz_enriched` | Enriches with composers, MBID, release data — **1 req/s strict, do not scale** |
| `regional_seed_worker` | — | Self-seeds `regional_seed_queue` with CIS/Central Asia/MENA queries |

### Key modules

- `app/core/config.py` — Pydantic `Settings` (all config from env, no global state)
- `app/db/collections.py` — collection names + index bootstrap (called at startup)
- `app/services/spotify.py` — token refresh, rate limiting, tenacity retry
- `app/services/genius.py` — search + HTML scrape, fuzzy match via rapidfuzz
- `app/services/musicbrainz.py` — ISRC lookup + fuzzy title/artist/duration matching
- `app/utils/rate_limiter.py` — async token-bucket limiter (one per API client)
- `app/utils/scoring.py` — quality score formula (popularity × followers × appearance × markets)
- `app/utils/deduplication.py` — ISRC primary, sha256 fingerprint fallback

## Configuration

Copy `.env.example` to `.env`. All settings have defaults in `app/core/config.py`.

| Required | Var | Notes |
|---|---|---|
| v1 | `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET` | Spotify Web API |
| v1 | `GENIUS_ACCESS_TOKEN` | Lyrics |
| v3 | `LASTFM_API_KEY` | Last.fm API — free account |
| v3 | `DISCOGS_TOKEN` | Discogs user token — 60 req/min (vs 25 without) |
| v3 | `YTMUSIC_*` | No token needed; ytmusicapi is unauthenticated |

Critical constraint: `MUSICBRAINZ_RATE_LIMIT_RPS` must stay at `1.0` — exceeding it causes a 24h+ IP ban. Never run multiple `musicbrainz-worker` replicas.

## Network

The compose stack joins the external `telegram-bot-network` (created by the parent infra repo). The MongoDB service is expected to be reachable at `MONGODB_URI` (default: `mongodb://mongo:27017`).
