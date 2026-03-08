# Music Metadata Enrichment Pipeline

Autonomous data-harvesting system that collects and enriches **10M–30M music tracks** from multiple sources — Spotify, Last.fm, YouTube Music, Discogs, Deezer, iTunes/Apple Music — stores everything in **MongoDB**, and runs fully unattended. All workers share a single Docker image; `WORKER_TYPE` selects which one runs.

---

## Architecture Overview

### Discovery sources → single tracks collection

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DISCOVERY LAYER                                  │
│                                                                         │
│  v1  seeder ──────────────────────────────────────────────────────┐     │
│       ├─ playlist_worker  (Spotify playlists)                     │     │
│       ├─ genre_worker     (Spotify genre search)                  │     │
│       └─ artist_worker    (Spotify artist discographies)          │     │
│                                                                   │     │
│  v3  lastfm_worker  ──┐                                           │     │
│      ytmusic_worker ──┼─→ track_candidates → candidate_match_worker     │
│      discogs_worker ──┘                                           │     │
│                                                                   ▼     │
│  v4  artist_graph_worker  (BFS, depth 5, 10M–30M tracks) ──→  tracks   │
│                                                                         │
│  v5  deezer_direct_worker (Deezer, no auth) ──────────────→  tracks    │
│                                                                         │
│  v6  itunes_worker (iTunes API + Apple Music RSS) ────────→  tracks    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │       ENRICHMENT LAYER      │
                    │                             │
                    │  audio_features_worker      │
                    │  lyrics_worker              │
                    │  quality_worker → enriched  │
                    │                             │
                    │  Supplementary (v2):        │
                    │  language_worker            │
                    │  transliteration_worker     │
                    │  musicbrainz_worker         │
                    │  regional_seed_worker       │
                    │  youtube_enrichment_worker  │
                    └─────────────────────────────┘
```

### Track state machine (v1 core pipeline)

```
base_collected
    ↓
audio_features_added  ← Spotify /audio-features (403 expected, pipeline continues)
    ↓ (or shortcut ──────────────────────────────────────────────────┐
lyrics_added          ← Genius search + HTML scrape                  │
    ↓                                                                ↓
enriched              ← quality_score ≥ threshold      quality_worker accepts both
    OR                                                  audio_features_added
filtered_out          ← quality_score < threshold       and lyrics_added
    OR
failed                ← exceeded retry_count
```

**`quality_worker`** accepts **both** `audio_features_added` and `lyrics_added` as input, so at 10M+ scale the pipeline never waits weeks for Genius to finish.

### MongoDB-as-Queue (no Redis)

Workers use **`findOneAndUpdate`** with optimistic locking:

1. Worker atomically sets `locked_at = now`, `locked_by = host:pid`
2. Worker processes the document
3. Worker sets final status
4. Items with `locked_at` older than 5 minutes are automatically reclaimed — crash-safe

---

## Workers (22 total)

### v1 — Core pipeline

| Worker | Input | Output | Notes |
|---|---|---|---|
| `seeder` | — | playlist_queue + genre_queue | Runs once, exits. Seeds 50 playlists + 50 categories + 150+ genres |
| `playlist_worker` | playlist_queue | base_collected | Tracks from Spotify playlists, increments appearance_score |
| `artist_worker` | artist_queue | — | Expands artist discographies (simple) |
| `genre_worker` | genre_queue | base_collected | Paginated Spotify genre search |
| `audio_features_worker` | base_collected | audio_features_added | Spotify 403 expected → advances anyway |
| `lyrics_worker` | audio_features_added | lyrics_added | Genius search + HTML scrape, advances on miss |
| `quality_worker` | audio_features_added **or** lyrics_added | enriched / filtered_out | Scores and filters |
| `exporter` | enriched | — | On-demand CSV + JSONL export |

### v2 — Supplementary (flag-driven, never touch `status`)

| Worker | Flag set | Notes |
|---|---|---|
| `language_worker` | `language_detected` | langdetect → script heuristic. Source priority: MusicBrainz → Genius lyrics → track name |
| `transliteration_worker` | `transliteration_done` | Cyrillic/Arabic/Georgian/Armenian → Latin |
| `musicbrainz_worker` | `musicbrainz_enriched` | MBID, composers, release countries. Priority: CIS > Central Asia > MENA > general. **1 req/s, single replica only** |
| `regional_seed_worker` | — | Self-seeds regional_seed_queue with CIS/Central Asia/MENA Spotify queries |

### v3 — External-source discovery

```
lastfm_worker   ┐
ytmusic_worker  ├─→ track_candidates → candidate_match_worker → tracks (base_collected)
discogs_worker  ┘
```

| Worker | Source | Notes |
|---|---|---|
| `lastfm_worker` | Last.fm tag/chart top tracks | Self-seeding, paginates (page tracked per item) |
| `ytmusic_worker` | YTMusic search + country charts | Self-seeding, one-shot, stores `youtube_video_id` |
| `discogs_worker` | Discogs genre style releases | Self-seeding, paginates. **1 req/s, single replica only** |
| `candidate_match_worker` | track_candidates | Spotify search → Deezer fallback. rapidfuzz 60/40 title/artist scoring, threshold 0.8 |

New collections: `track_candidates`, `lastfm_seed_queue`, `ytmusic_seed_queue`, `discogs_seed_queue`

### v4 — Artist graph expansion (10M–30M tracks)

BFS traversal starting from top 500 artists in existing tracks, depth limit 5. At full depth: ~50K–100K artists, ~500K–1M albums, ~10M–30M tracks.

```
artist_graph_queue (priority DESC)
    ↓
artist_graph_worker
    ├─ full discography (album + single + compilation + appears_on)
    │    └─ batch-fetch tracks for ISRC → upsert, $addToSet version_album_ids
    │         └─ featured artists → enqueue (depth + 1)
    └─ related artists → enqueue (depth + 1, priority × 0.6)

youtube_enrichment_worker
    └─ YTMusic search for tracks missing youtube_video_id
    └─ rapidfuzz confidence ≥ 0.65, sets youtube_searched=True
```

Priority formula: `0.4 × (popularity/100) + 0.3 × min(log(followers+1)/14, 1.0)`

New collections: `artist_graph_queue`, `artist_processed_cache`, `album_processed_cache`

### v5 — Deezer direct discovery (Spotify-independent)

Inserts tracks directly as `base_collected` using `"deezer:{id}"` as placeholder `spotify_id`. Real Spotify ID is backfilled when `artist_graph_worker` or `candidate_match_worker` finds the same track.

| Worker | Source | Notes |
|---|---|---|
| `deezer_direct_worker` | Deezer genre → artists → top tracks + albums | No auth, ~8 rps, ~90M tracks reachable. 3 replicas |

New collection: `deezer_seed_queue`

### v6 — iTunes / Apple Music discovery

Two-phase, no authentication required.

| Phase | Source | Scale |
|---|---|---|
| Phase 1 (on startup) | Apple Music RSS charts, 40+ countries | ~5K–15K tracks per cycle |
| Phase 2 (queue) | iTunes Search API, artist name search | ~3M candidates per cycle, ~1.5h at 8 rps |

Placeholder `spotify_id="itunes:{trackId}"`, backfilled later. 403 backoff: after 5 consecutive 403s → sleep 300s.

| Worker | Notes |
|---|---|
| `itunes_worker` | 2 replicas safe. 8 rps. Auto-reseed when queue exhausted |

New collection: `itunes_seed_queue`

---

## Project Structure

```
music-enricher/
├── app/
│   ├── core/
│   │   ├── config.py                   # Pydantic Settings — all from env
│   │   └── logging_config.py           # Structured JSON logging (structlog)
│   ├── db/
│   │   ├── mongodb.py                  # Async Motor connection + retry
│   │   └── collections.py             # Collection names + index bootstrap
│   ├── models/
│   │   ├── track.py                    # TrackDocument, TrackStatus, YoutubeData, MusicBrainzData, RegionData
│   │   ├── artist.py                   # ArtistDocument
│   │   ├── candidate.py               # CandidateDocument + seed queue models (v3)
│   │   └── artist_graph.py            # ArtistGraphItem, compute_artist_priority() (v4)
│   ├── services/
│   │   ├── spotify.py                  # Token refresh, rate limit, circuit breaker, retry
│   │   ├── genius.py                   # Search + HTML scrape, fuzzy match
│   │   ├── lastfm.py                   # Last.fm API (httpx, 4 rps)
│   │   ├── ytmusic.py                  # ytmusicapi wrapper (async executor)
│   │   ├── discogs.py                  # Discogs API (httpx, 1 rps)
│   │   ├── deezer.py                   # Deezer public API (httpx, 8 rps, no auth)
│   │   ├── apple_music.py             # iTunes Search API + Apple Music RSS
│   │   └── musicbrainz.py             # ISRC lookup + fuzzy matching
│   ├── utils/
│   │   ├── rate_limiter.py            # Async token-bucket RateLimiter
│   │   ├── circuit_breaker.py         # CLOSED→OPEN→HALF_OPEN (Spotify)
│   │   ├── deduplication.py           # compute_fingerprint + compute_candidate_fingerprint
│   │   ├── scoring.py                  # Quality score + regional boost
│   │   ├── regional.py                # Regional scoring (CIS / Central Asia / MENA)
│   │   ├── transliteration.py         # Cyrillic/Arabic → Latin
│   │   └── signals.py                  # SIGTERM/SIGINT graceful shutdown
│   ├── workers/
│   │   ├── base.py                     # BaseWorker: polling loop, locking, stale-lock recovery
│   │   ├── seeder.py
│   │   ├── playlist_worker.py
│   │   ├── artist_worker.py
│   │   ├── genre_worker.py
│   │   ├── audio_features_worker.py
│   │   ├── lyrics_worker.py
│   │   ├── quality_worker.py
│   │   ├── exporter.py
│   │   ├── language_worker.py          # v2
│   │   ├── transliteration_worker.py   # v2
│   │   ├── musicbrainz_worker.py       # v2
│   │   ├── regional_seed_worker.py     # v2
│   │   ├── lastfm_worker.py            # v3
│   │   ├── ytmusic_worker.py           # v3
│   │   ├── discogs_worker.py           # v3
│   │   ├── candidate_match_worker.py   # v3
│   │   ├── artist_graph_worker.py      # v4
│   │   ├── youtube_enrichment_worker.py # v4
│   │   ├── deezer_direct_worker.py     # v5
│   │   └── itunes_worker.py            # v6
│   └── entrypoint.py                   # WORKER_TYPE dispatch (22 workers)
├── tests/
│   └── test_candidate.py              # normalize_text, fingerprint, circuit breaker, service parsers
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

---

## Quick Start

### 1. Prerequisites

- Docker ≥ 24
- Docker Compose ≥ 2.20
- Spotify Developer account → [developer.spotify.com](https://developer.spotify.com/dashboard)
- Genius API token → [genius.com/api-clients](https://genius.com/api-clients) (optional — pipeline advances without it)
- Last.fm API key → [last.fm/api/account/create](https://www.last.fm/api/account/create) (for v3)
- Discogs personal access token → [discogs.com/settings/developers](https://www.discogs.com/settings/developers) (for v3; optional — 25 rps without auth)

### 2. Configure

```bash
cp .env.example .env
# Required: SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
# Optional: GENIUS_ACCESS_TOKEN, LASTFM_API_KEY, DISCOGS_TOKEN
# Set MONGODB_DB=MusicEnricher (production convention)
```

### 3. Launch

```bash
docker-compose up -d
```

All workers start automatically. `seeder` runs once and seeds queues; all other workers poll indefinitely.

### 4. Monitor progress

```bash
# All logs
docker-compose logs -f

# Specific worker
docker-compose logs -f artist-graph-worker

# Track counts by status (most important health check)
docker-compose exec mongo mongosh MusicEnricher --eval \
  'db.tracks.aggregate([{$group:{_id:"$status",count:{$sum:1}}}]).toArray()'

# Pipeline counters
docker-compose exec mongo mongosh MusicEnricher --eval \
  'db.system_stats.findOne({doc_id: "global"}, {_id: 0})'

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

### 5. Export enriched tracks

```bash
docker-compose run --rm exporter
docker-compose exec exporter ls /data/exports/
```

### 6. Graceful stop

```bash
docker-compose down
```

Workers catch SIGTERM, finish the current batch, then exit. Resume anytime with `docker-compose up -d`.

---

## Configuration Reference

### Core (v1)

| Variable | Default | Description |
|---|---|---|
| `MONGODB_URI` | `mongodb://mongo:27017` | MongoDB connection string |
| `MONGODB_DB` | `music_enricher` | Database name (production: `MusicEnricher`) |
| `SPOTIFY_CLIENT_ID` | — | **Required** |
| `SPOTIFY_CLIENT_SECRET` | — | **Required** |
| `SPOTIFY_RATE_LIMIT_RPS` | `3.0` | Spotify requests/second per worker |
| `SPOTIFY_MAX_RATE_LIMIT_SLEEP` | `60` | Max seconds to sleep on 429 (prevents hour-long stalls) |
| `GENIUS_ACCESS_TOKEN` | — | Optional — pipeline advances without it |
| `GENIUS_RATE_LIMIT_RPS` | `3.0` | Genius requests/second |
| `GENIUS_MIN_CONFIDENCE` | `0.6` | Min rapidfuzz score for lyrics match |
| `BATCH_SIZE` | `50` | Documents per worker iteration |
| `WORKER_SLEEP_SEC` | `5` | Sleep when queue is empty |
| `WORKER_RETRY_LIMIT` | `3` | Max retries before `status=failed` |
| `QUALITY_THRESHOLD` | `0.1` | Min quality_score to keep track |
| `MIN_PLAYLIST_FOLLOWERS` | `10000` | Min playlist followers to seed |
| `STATS_LOG_INTERVAL_MIN` | `10` | Minutes between progress logs |
| `EXPORT_DIR` | `/data/exports` | Output directory |

### MusicBrainz (v2)

| Variable | Default | Description |
|---|---|---|
| `MUSICBRAINZ_ENABLED` | `true` | Enable MusicBrainz enrichment |
| `MUSICBRAINZ_USER_AGENT` | `MusicEnricher/1.0` | **Required by MusicBrainz** — include your contact |
| `MUSICBRAINZ_RATE_LIMIT_RPS` | `1.0` | **Do not increase** — 24h+ IP ban risk |
| `MUSICBRAINZ_MIN_CONFIDENCE` | `0.75` | Min match confidence to store MB data |
| `MUSICBRAINZ_DURATION_TOLERANCE_MS` | `3000` | ±ms for duration matching |
| `TARGET_REGIONS` | `cis,central_asia,mena` | Regions to target |
| `REGIONAL_BOOST_ENABLED` | `true` | Use regional scoring formula |
| `REGIONAL_BOOST_WEIGHT` | `0.15` | Regional score weight in final score |
| `LANGUAGE_DETECTION_ENABLED` | `true` | Enable language_worker |
| `TRANSLITERATION_ENABLED` | `true` | Enable transliteration_worker |

### External sources (v3)

| Variable | Default | Description |
|---|---|---|
| `LASTFM_API_KEY` | — | Free at last.fm |
| `LASTFM_RATE_LIMIT_RPS` | `4.0` | Last.fm requests/second |
| `LASTFM_MAX_PAGES` | `200` | Max pages per seed item |
| `DISCOGS_TOKEN` | — | Personal access token (60 rps auth vs 25 anon) |
| `DISCOGS_RATE_LIMIT_RPS` | `1.0` | **Do not increase** — IP throttle risk |
| `DISCOGS_MAX_PAGES` | `100` | Max pages per seed item |
| `CANDIDATE_MATCH_CONFIDENCE` | `0.8` | Min score to accept Spotify match |

### Artist graph (v4)

| Variable | Default | Description |
|---|---|---|
| `ARTIST_GRAPH_MAX_DEPTH` | `5` | BFS depth limit |
| `ARTIST_GRAPH_BATCH_SIZE` | `3` | Artists per iteration |
| `ARTIST_GRAPH_SEED_LIMIT` | `500` | Top artists to seed from existing tracks |
| `ARTIST_GRAPH_MAX_APPEARS_ON` | `50` | Max appears_on albums per artist |
| `YTMUSIC_VIDEO_MATCH_CONFIDENCE` | `0.65` | Min rapidfuzz score for YouTube match |

### Deezer (v5)

| Variable | Default | Description |
|---|---|---|
| `DEEZER_TOP_TRACKS_LIMIT` | `50` | Top tracks per artist |
| `DEEZER_CRAWL_ALBUMS` | `true` | Also crawl artist albums |
| `DEEZER_MAX_ALBUMS_PER_ARTIST` | `20` | Max albums to fetch per artist |

### iTunes / Apple Music (v6)

| Variable | Default | Description |
|---|---|---|
| `ITUNES_RATE_LIMIT_RPS` | `4.0` | iTunes Search API requests/second |
| `ITUNES_BATCH_SIZE` | `10` | Artists processed per iteration |
| `ITUNES_SEED_ARTIST_LIMIT` | `200000` | Max artists per queue cycle |
| `ITUNES_SKIP_IF_TRACKS_GTE` | `50` | Skip artist if already has ≥N tracks |
| `ITUNES_403_BACKOFF_AFTER` | `5` | Consecutive 403s before sleep |
| `ITUNES_403_BACKOFF_SECONDS` | `300` | Seconds to sleep after 403 backoff |

---

## Quality Scoring

### Base formula

```
quality_score =
    0.40 × (popularity / 100)
  + 0.30 × min(artist_followers / 50_000_000, 1.0)
  + 0.20 × min(appearance_score / 200, 1.0)
  + 0.10 × (markets_count / 185)
```

### Regional boost (v2, when `REGIONAL_BOOST_ENABLED=true`)

```
final_score = (1 - REGIONAL_BOOST_WEIGHT) × base_score
            + REGIONAL_BOOST_WEIGHT × regional_score
```

Tracks below `QUALITY_THRESHOLD` get `status=filtered_out` and are excluded from export.

---

## Deduplication

Tracks are never duplicated:

1. **ISRC** (primary) — globally unique identifier, used when available
2. **Fingerprint** (fallback) — `sha256(normalized_name | first_artist_id | duration_bucket(2s))`
3. **Candidate fingerprint** (v3) — `sha256(normalized_title | normalized_artist [| duration_bucket])` — uses artist *name* since Spotify ID is unknown at candidate stage

`normalize_text` strips: feat./ft./featuring, remaster/live/official video tags, track-number prefixes, punctuation → spaces.

---

## Rate Limiting & Safety

Each API client has its own async token-bucket limiter. On HTTP 429, the worker sleeps `min(Retry-After, SPOTIFY_MAX_RATE_LIMIT_SLEEP)` seconds before retrying — the cap prevents multi-hour stalls.

**Spotify circuit breaker**: after 5 consecutive failures (429/5xx/network errors), the circuit opens for 120s. 401/403/404 do not count as failures. Prevents thundering herd on degraded API.

| Worker | Rate limit | Safety constraint |
|---|---|---|
| `discogs_worker` | 1 req/s | IP throttle/ban — **single replica only** |
| `musicbrainz_worker` | 1 req/s | 24h+ IP ban — **single replica only** |
| `itunes_worker` | 8 rps + 403 backoff | 2 replicas safe |
| All others | Configurable | Safe to scale horizontally |

---

## Scaling

Workers compete for the same MongoDB queues. Optimistic locking handles concurrent workers safely — just add replicas:

```yaml
# docker-compose.override.yml
services:
  artist-graph-worker:
    deploy:
      replicas: 5   # default: 3
  deezer-direct-worker:
    deploy:
      replicas: 5   # default: 3
  lyrics-worker:
    deploy:
      replicas: 3
```

**Single-replica only**: `discogs-worker`, `musicbrainz-worker`.

---

## MongoDB Collections

| Collection | Version | Description |
|---|---|---|
| `tracks` | v1 | Main enriched track documents |
| `artists` | v1 | Spotify artist metadata cache |
| `system_stats` | v1 | Global pipeline counters |
| `playlist_queue` | v1 | Playlists to scrape |
| `artist_queue` | v1 | Artists to expand (simple) |
| `genre_queue` | v1 | Genre search queue with offset |
| `regional_seed_queue` | v2 | Regional Spotify search queries |
| `track_candidates` | v3 | Staging area for unmatched external tracks |
| `lastfm_seed_queue` | v3 | Last.fm tag/method pagination state |
| `ytmusic_seed_queue` | v3 | YTMusic query queue |
| `discogs_seed_queue` | v3 | Discogs style queue |
| `artist_graph_queue` | v4 | BFS artist expansion queue (priority-ordered) |
| `artist_processed_cache` | v4 | Processed artists (prevent re-processing) |
| `album_processed_cache` | v4 | Processed albums (prevent re-processing) |
| `deezer_seed_queue` | v5 | Deezer artist queue |
| `itunes_seed_queue` | v6 | iTunes artist name queue |

---

## Troubleshooting

**Worker shows "no work available" immediately**
- Check seeder completed: `docker-compose logs seeder`
- Verify MongoDB is healthy: `docker-compose ps`

**Spotify 403 on audio-features**
- Expected — this endpoint is restricted on most developer app tiers. Tracks advance through the pipeline with `null` audio_features.

**Spotify workers stalling for hours**
- Ensure `SPOTIFY_MAX_RATE_LIMIT_SLEEP=60` is set (default). Without this cap, a single 429 with large `Retry-After` stalls all workers.
- Lower `SPOTIFY_RATE_LIMIT_RPS` if 429s are frequent (default 3.0).

**Genius low confidence / no lyrics**
- Instrumental tracks, DJ mixes, and ambient music won't match. They still get scored and exported.

**MusicBrainz worker returns no matches for Cyrillic tracks**
- `transliteration_worker` must run first to generate `transliterated_name`. MusicBrainz worker uses both original and transliterated names.

**Language detection wrong for short names**
- Short names (< 20 chars) cannot be reliably detected. MusicBrainz data overrides langdetect when available.

**Discogs or MusicBrainz worker getting IP-banned**
- These workers must run as **single replicas**. Scale to 2+ instances causes 1 req/s to be exceeded, resulting in bans.

**iTunes 403 errors**
- Expected at high request rates. Worker auto-backs off after `ITUNES_403_BACKOFF_AFTER` consecutive 403s. Reduce `ITUNES_RATE_LIMIT_RPS` if this happens frequently.

**Re-seeding safely**
- All seed workers use `$setOnInsert` — safe to restart without duplicating queue items.
- `docker-compose restart seeder`
