# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

An autonomous music metadata enrichment pipeline targeting 10M–30M tracks. Discovers tracks via Spotify, Last.fm, YouTube Music, Discogs, Deezer, iTunes/Apple Music, **Yandex Music** and an artist collaboration graph, enriches them with quality scores, YouTube video IDs and supplementary metadata, and stores everything in MongoDB. All workers share a single Docker image; the `WORKER_TYPE` env var selects which worker runs.

## Run tests

```bash
python -m pytest tests/ -v
```

## Commands

### Docker (normal workflow)

```bash
# Full stack
docker-compose up -d

# Rebuild and restart one worker only
docker-compose up -d --build quality-worker

# Specific worker logs
docker-compose logs -f artist-graph-worker

# Run exporter once
docker-compose run --rm exporter
```

### MongoDB inspection

```bash
# Pipeline counters
docker-compose exec mongo mongosh MusicEnricher --eval \
  'db.system_stats.findOne({doc_id: "global"}, {_id: 0})'

# Track counts by status (most important health check)
docker-compose exec mongo mongosh MusicEnricher --eval \
  'db.tracks.aggregate([{$group:{_id:"$status",count:{$sum:1}}}]).toArray()'

# Artist graph progress
docker-compose exec mongo mongosh MusicEnricher --eval \
  'printjson({
    queue: db.artist_graph_queue.countDocuments({processed:false}),
    processed: db.artist_processed_cache.countDocuments({}),
    albums: db.album_processed_cache.countDocuments({})
  })'

# Deezer / iTunes queue progress
docker-compose exec mongo mongosh MusicEnricher --eval \
  'printjson({
    deezer_pending: db.deezer_seed_queue.countDocuments({processed:false}),
    itunes_pending: db.itunes_seed_queue.countDocuments({processed:false}),
    candidates: db.track_candidates.countDocuments({processed:false})
  })'
```

### Local dev (single worker, no Docker)

```bash
source .venv/Scripts/activate   # Windows bash
WORKER_TYPE=seeder python -m app.entrypoint
pip install -r requirements.txt  # if venv is fresh
```

## Architecture

### Single-image, multi-worker design

All workers are built from the same `Dockerfile`. `app/entrypoint.py` reads `WORKER_TYPE` and imports + runs the appropriate worker class. Every worker inherits from `BaseWorker` (`app/workers/base.py`), which provides the polling loop, optimistic locking, graceful shutdown (SIGTERM), and stale-lock recovery (locks older than 5 min are reclaimed automatically).

22 worker types are registered in `app/entrypoint.py`.

### Track state machine

```
base_collected → audio_features_added → lyrics_added → enriched | filtered_out | failed
                        ↓ (shortcut)
                      enriched   ← quality_worker also accepts audio_features_added
                                   so the pipeline never waits on Genius
```

**Important**: `quality_worker` processes **both** `audio_features_added` and `lyrics_added`.
At 10M+ scale, Genius lyrics enrichment takes weeks — quality scoring must not be gated on it.
Tracks that lyrics_worker processes first get `lyrics_added → enriched`.
Tracks that quality_worker reaches first skip directly to `enriched`. Both paths are valid.

Workers claim batches atomically with `findOneAndUpdate` / `find+update_many` (MongoDB-as-queue, no Redis).

### v1 core pipeline workers

| Worker | Input status | Output status | Notes |
|---|---|---|---|
| `seeder` | — | playlist_queue + genre_queue | Runs once, exits. Seeds playlists + 150+ genres |
| `playlist_worker` | playlist_queue | base_collected | Discovers tracks from playlists |
| `artist_worker` | artist_queue | — | Expands artist discographies (simple) |
| `genre_worker` | genre_queue | base_collected | Searches by genre |
| `audio_features_worker` | base_collected | audio_features_added | Spotify 403 is expected — advances anyway |
| `lyrics_worker` | audio_features_added | lyrics_added | Advances regardless of Genius hit/miss |
| `quality_worker` | audio_features_added **or** lyrics_added | enriched / filtered_out | — |
| `exporter` | enriched | — | Runs on-demand |

### v2 supplementary workers (flag-driven, non-blocking)

Never change `status`. Run orthogonally on any track regardless of pipeline position.

| Worker | Flag set | Notes |
|---|---|---|
| `language_worker` | `language_detected` | langdetect → script heuristic |
| `transliteration_worker` | `transliteration_done` | Cyrillic/Arabic → Latin |
| `musicbrainz_worker` | `musicbrainz_enriched` | MBID, composers, release data — **1 req/s, single replica only**. Priority queue: CIS > Central Asia > MENA > general |
| `regional_seed_worker` | — | Self-seeds CIS/Central Asia/MENA search queries |

### v3 external-source discovery pipeline

Extends discovery beyond Spotify playlists via external APIs.

```
lastfm_worker   ┐
ytmusic_worker  ├─→  track_candidates  →  candidate_match_worker
discogs_worker  ┘         (29K+)            (Spotify search → Deezer fallback, score ≥ 0.8)
                                                      ↓
                                             tracks (base_collected)
                                                      ↓
                                            existing pipeline continues
```

| Worker | Source | Notes |
|---|---|---|
| `lastfm_worker` | Last.fm tag/chart top tracks | Self-seeding, paginates (page stored per queue item) |
| `ytmusic_worker` | YTMusic search + country charts | Self-seeding, one-shot per item, stores `youtube_video_id` |
| `discogs_worker` | Discogs genre style releases | Self-seeding, paginates — **single replica only** (1 req/s) |
| `candidate_match_worker` | track_candidates | rapidfuzz 60/40 title/artist scoring, threshold 0.8. Primary: Spotify search; fallback: Deezer search |

New collections: `track_candidates`, `lastfm_seed_queue`, `ytmusic_seed_queue`, `discogs_seed_queue`

Candidate deduplication: `compute_candidate_fingerprint(title, artist, duration_ms?)` — uses artist *name* (not Spotify ID) since Spotify ID is unknown at this stage.

### v4 artist graph expansion (10M–30M tracks)

BFS expansion of the artist collaboration graph. Expected scale at depth=5: 50k–100k artists, 500k–1M albums, 10M–30M tracks.

```
existing tracks (top 500 artists by occurrence)
  OR artist_queue (fallback if tracks aggregation returns nothing)
        ↓ [bootstrap on first run]
  artist_graph_queue  (priority DESC — popular artists first)
        ↓
  artist_graph_worker
    ├─ GET /artists/{id}  (metadata)
    ├─ iter_artist_albums (album + single + compilation + appears_on)
    │    └─ iter_album_tracks → get_tracks (batch 50, for ISRC)
    │         ├─ upsert into tracks (ISRC/fingerprint dedup, $addToSet version_album_ids)
    │         └─ featured artists → enqueue (depth + 1)
    └─ get_related_artists → enqueue (depth + 1, priority × 0.6)

youtube_enrichment_worker
    └─ poll: youtube=∅ AND youtube_searched≠True AND status∈eligible
    └─ YTMusic search → rapidfuzz score ≥ 0.65 → store YoutubeData
```

New collections: `artist_graph_queue`, `artist_processed_cache`, `album_processed_cache`

**Seeding behaviour**: on first run `artist_graph_worker` aggregates top artists from `tracks`. If `tracks` is empty (system just started), falls back to `artist_queue` which is populated by v1 workers within minutes.

**Priority formula**: `0.4 × (popularity/100) + 0.3 × min(log(followers+1)/14, 1.0)` — see `compute_artist_priority()` in `app/models/artist_graph.py`.

**Track versions**: the same Spotify recording appears on multiple releases (album + compilation + appears_on). Instead of creating duplicates, all album IDs are stored in `version_album_ids: List[str]` via `$addToSet`. The canonical track document is not duplicated.

### v5 Deezer direct discovery (Spotify-independent)

Inserts tracks directly into `tracks` as `base_collected` without Spotify API.
Uses `"deezer:{id}"` as placeholder `spotify_id`; ISRC dedup prevents conflicts.
When `artist_graph_worker` / `candidate_match_worker` finds the same track via
Spotify, they backfill the real `spotify_id` via `$set` (not `$setOnInsert`).

| Worker | Source | Notes |
|---|---|---|
| `deezer_direct_worker` | Deezer genre→artist→top tracks + albums | No auth, 8 rps, ~90M tracks |

Queue: `deezer_seed_queue` (one item per Deezer artist)
Bootstrap: genre list → genre artists → populate queue → reset when exhausted.

### v6 iTunes / Apple Music discovery

Two-phase discovery. No authentication required.

```
Phase 1: Apple Music RSS charts (40+ countries) → 5K–15K tracks per cycle
Phase 2: iTunes artist search queue → 50K+ artists × 200 songs ≈ 3M candidates, ~1.5h per cycle at 8 rps
```

Inserts tracks as `base_collected` with `spotify_id="itunes:{trackId}"`. Deduplication via ISRC or fingerprint. Real `spotify_id` backfilled by `artist_graph_worker` or `candidate_match_worker` later.

| Worker | Source | Notes |
|---|---|---|
| `itunes_worker` | iTunes Search API + Apple Music RSS Feeds | No auth. 2 replicas safe. 403 backoff: after 5 requests → sleep 300s |

Queue: `itunes_seed_queue` (unique by `artist_name`)
Bootstrap: RSS charts fetched on startup → artist names extracted → queue populated.

### Key modules

| Module | Purpose |
|---|---|
| `app/core/config.py` | Pydantic `Settings` — all config from env, no global state |
| `app/db/collections.py` | Collection name constants + `ensure_indexes()` called at every startup |
| `app/models/track.py` | `TrackDocument`, `TrackStatus`, `YoutubeData`, `ArtistRef`, `AlbumRef`, `MusicBrainzData`, `RegionData` |
| `app/models/candidate.py` | `CandidateDocument`, seed queue models (v3) |
| `app/models/artist_graph.py` | `ArtistGraphItem`, `GraphSource`, `compute_artist_priority()` (v4) |
| `app/models/artist.py` | `ArtistDocument` — Spotify artist metadata cache |
| `app/services/spotify.py` | Token refresh, rate limiting, circuit breaker, tenacity retry, all Spotify endpoints |
| `app/services/genius.py` | Search + HTML scrape, fuzzy match via rapidfuzz |
| `app/services/lastfm.py` | Last.fm API client (httpx, 4 req/s) |
| `app/services/ytmusic.py` | ytmusicapi wrapper (executor), `search_track_video()` for enrichment |
| `app/services/discogs.py` | Discogs API client (httpx, 1 req/s) |
| `app/services/deezer.py` | Deezer public API (httpx, 8 rps, no auth) |
| `app/services/apple_music.py` | iTunes Search API + Apple Music RSS Feeds (8 rps, no auth) |
| `app/services/musicbrainz.py` | ISRC lookup + fuzzy matching |
| `app/utils/rate_limiter.py` | Async token-bucket `RateLimiter` (one instance per API client) |
| `app/utils/circuit_breaker.py` | CLOSED→OPEN→HALF_OPEN state machine for Spotify API |
| `app/utils/deduplication.py` | `compute_fingerprint` (Spotify ID), `compute_candidate_fingerprint` (name) |
| `app/utils/scoring.py` | Quality score formula, regional boost |
| `app/utils/regional.py` | Regional scoring for CIS/Central Asia/MENA |
| `app/utils/transliteration.py` | Cyrillic/Arabic → Latin |
| `app/utils/signals.py` | SIGTERM/SIGINT shutdown event handler |

## Configuration

Copy `.env.example` to `.env`. All settings have defaults in `app/core/config.py`.

| Version | Required var | Notes |
|---|---|---|
| v1 | `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET` | Spotify Web API |
| v1 | `GENIUS_ACCESS_TOKEN` | Lyrics (optional in practice — pipeline advances without it) |
| v3 | `LASTFM_API_KEY` | Free at last.fm/api/account/create |
| v3 | `DISCOGS_TOKEN` | Personal access token from discogs.com/settings/developers — **not** Consumer Key/Secret (those are OAuth) |
| v3 | `YTMUSIC_*` | No token needed; ytmusicapi is unauthenticated |
| v5 | — | No credentials — Deezer public API |
| v6 | — | No credentials — iTunes Search API + Apple Music RSS |

### Critical rate-limit constraints

| Worker | Limit | Consequence of exceeding |
|---|---|---|
| `discogs_worker` | 1 req/s | IP throttle / ban — **single replica only** |
| `musicbrainz_worker` | 1 req/s | 24h+ IP ban — **single replica only** |
| `itunes_worker` | 8 rps, 403 backoff | After 5 consecutive 403s: sleep 300s (configurable) |
| All others | Configurable | Safe to scale horizontally |

## Known behaviours

- **Spotify audio features (403)**: The `/audio-features` endpoint is restricted for most developer apps. `audio_features_worker` detects this and advances tracks to `audio_features_added` with `null` audio_features. Stat counter `audio_features_added` counts *data found*, not *tracks advanced* — the pipeline is not blocked.
- **`artist_graph_worker` bootstrap fallback**: If the `tracks` collection is empty on first run (other workers haven't collected yet), the worker seeds from `artist_queue` instead. No manual intervention needed.
- **`youtube_enrichment_worker`**: Sets `youtube_searched=True` after every attempt (hit or miss) so tracks are never re-queried. Flag persists in `TrackDocument`.
- **Spotify 429 sleep cap**: `spotify.py` caps `Retry-After` sleep to `SPOTIFY_MAX_RATE_LIMIT_SLEEP` (default 60s). Without this cap, a single 429 could stall all workers for hours. `SPOTIFY_RATE_LIMIT_RPS` defaults to 3.0 (was 10.0) to reduce 429 frequency.
- **Spotify circuit breaker**: After 5 consecutive failures (429/5xx/network), the circuit opens for 120s. 401/403/404 do not count as failures. Prevents thundering herd on degraded API.
- **`itunes_worker` 403 backoff**: iTunes returns 403 when rate-limited. Worker tracks consecutive 403s; after `ITUNES_403_BACKOFF_AFTER` (default 5), sleeps `ITUNES_403_BACKOFF_SECONDS` (default 300s).
- **Deezer placeholder spotify_id**: Tracks inserted by `deezer_direct_worker` or `itunes_worker` use `"deezer:{id}"` / `"itunes:{id}"` as placeholder. These are backfilled with real Spotify IDs when the same track is found via Spotify later.
- **MongoDB DB name**: Production uses `MusicEnricher` (set via `MONGODB_DB` in `.env`). Default in config is `music_enricher` — ensure `.env` matches your actual DB.

## Running without Spotify API

If Spotify revokes API access (or for new deployments where Spotify credentials are unavailable), the pipeline continues to function via Deezer + iTunes + Last.fm + YTMusic + Discogs.

### What still works

| Worker | Status | Notes |
|---|---|---|
| `deezer_direct_worker` | ✅ Full BFS | genre → artists → related artists (Deezer `/artist/{id}/related`) → ∞ |
| `itunes_worker` | ✅ | RSS charts 40+ countries + 200K+ artist search |
| `lastfm_worker` | ✅ | Last.fm tag/chart top tracks |
| `ytmusic_worker` | ✅ | YouTube Music charts and search |
| `discogs_worker` | ✅ | Discogs genre/style releases |
| `candidate_match_worker` | ✅ Deezer fallback | Set `SPOTIFY_ENABLED=false`, Deezer search takes over |
| `audio_features_worker` | ✅ Status advancer | Gets 403 immediately, advances tracks with null audio_features |
| `lyrics_worker` | ✅ | Genius, no Spotify dependency |
| `quality_worker` | ✅ | MongoDB only |
| `language_worker` | ✅ | langdetect, independent |
| `transliteration_worker` | ✅ | Independent |
| `musicbrainz_worker` | ✅ | Independent |

### What to disable

```yaml
# docker-compose.override.yml — paste this when Spotify access is gone
services:
  seeder:
    deploy:
      replicas: 0
  playlist-worker:
    deploy:
      replicas: 0
  genre-worker:
    deploy:
      replicas: 0
  artist-worker:
    deploy:
      replicas: 0
  artist-graph-worker:
    deploy:
      replicas: 0
  regional-seed-worker:
    deploy:
      replicas: 0
  candidate-match-worker:
    environment:
      SPOTIFY_ENABLED: "false"
```

### Discovery capacity without Spotify

- `deezer_direct_worker` BFS: ~90M tracks reachable (Deezer has full related-artists API unlike Spotify which deprecated it in Nov 2024)
- `itunes_worker`: ~3M candidates per cycle from 200K+ artists
- Last.fm + YTMusic + Discogs: additional millions of candidates
- Regional coverage: Deezer has good CIS/MENA coverage through its own genre system; regional artists are reached naturally through BFS

## Network

The compose stack joins the external `telegram-bot-network` (created by the parent infra repo). MongoDB is reachable at `MONGODB_URI` (set in `.env`).
