# Music Metadata Enrichment Pipeline

Autonomous data-harvesting system that collects and enriches 1,000,000+ music tracks using the **Spotify Web API** and **Genius API**, stores everything in **MongoDB**, and runs fully unattended for days at a time.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose Network                   │
│                                                             │
│  ┌──────────┐  playlists   ┌──────────────────────────────┐ │
│  │          │─────────────▶│   playlist_worker            │ │
│  │  seeder  │  genres      │   artist_worker              │ │
│  │ (once)   │─────────────▶│   genre_worker               │ │
│  └──────────┘              └────────────┬─────────────────┘ │
│                                         │ tracks             │
│                                         ▼ base_collected     │
│                             ┌───────────────────────────┐   │
│                             │  audio_features_worker    │   │
│                             └───────────┬───────────────┘   │
│                                         │ audio_features_    │
│                                         ▼  added            │
│                             ┌───────────────────────────┐   │
│                             │     lyrics_worker         │   │
│                             └───────────┬───────────────┘   │
│                                         │ lyrics_added       │
│                                         ▼                    │
│                             ┌───────────────────────────┐   │
│                             │     quality_worker        │   │
│                             └───────────┬───────────────┘   │
│                                         │ enriched /         │
│                                         ▼ filtered_out       │
│                             ┌───────────────────────────┐   │
│                             │     exporter              │   │
│                             │  (CSV + JSONL output)     │   │
│                             └───────────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │                     MongoDB                          │  │
│  │  collections: tracks, artists, system_stats,         │  │
│  │               playlist_queue, artist_queue,          │  │
│  │               genre_queue                            │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### State Machine

Every track in the `tracks` collection progresses through these statuses:

```
discovered
    ↓
base_collected       ← inserted by playlist/artist/genre workers
    ↓
audio_features_added ← Spotify /audio-features batch API
    ↓
lyrics_added         ← Genius search + HTML scrape
    ↓
enriched             ← quality_score ≥ threshold
    OR
filtered_out         ← quality_score < threshold
    OR
failed               ← exceeded retry_count
```

### MongoDB-as-Queue (No Redis)

Workers use **`findOneAndUpdate`** with optimistic locking:

1. Worker atomically sets `status = "processing"` and `locked_at = now`
2. Worker processes the document
3. Worker sets final status (`audio_features_added`, etc.)
4. If a worker crashes, items with `locked_at` older than 5 minutes
   are automatically reclaimed by the next worker iteration

---

## Project Structure

```
music-enricher/
├── app/
│   ├── core/
│   │   ├── config.py          # Pydantic settings (all from env)
│   │   └── logging_config.py  # Structured JSON logging (structlog)
│   ├── db/
│   │   ├── mongodb.py         # Async Motor connection + retry
│   │   └── collections.py     # Collection names + index bootstrap
│   ├── models/
│   │   ├── track.py           # TrackDocument, TrackStatus, AudioFeatures, LyricsData
│   │   ├── artist.py          # ArtistDocument
│   │   ├── stats.py           # SystemStats
│   │   └── queue.py           # PlaylistQueueItem, ArtistQueueItem, GenreQueueItem
│   ├── services/
│   │   ├── spotify.py         # Spotify API client (token refresh, rate limit, retry)
│   │   └── genius.py          # Genius API + lyrics scraping client
│   ├── utils/
│   │   ├── rate_limiter.py    # Async token-bucket rate limiter
│   │   ├── deduplication.py   # ISRC + fingerprint dedup
│   │   ├── scoring.py         # Quality score formula
│   │   └── signals.py         # SIGTERM/SIGINT graceful shutdown
│   ├── workers/
│   │   ├── base.py            # BaseWorker (polling loop, locking, stats)
│   │   ├── seeder.py          # One-shot queue initializer
│   │   ├── playlist_worker.py # Extracts tracks from playlists
│   │   ├── artist_worker.py   # Expands artist discographies
│   │   ├── genre_worker.py    # Searches by genre with pagination
│   │   ├── audio_features_worker.py
│   │   ├── lyrics_worker.py
│   │   ├── quality_worker.py
│   │   └── exporter.py        # CSV + JSONL export
│   └── entrypoint.py          # WORKER_TYPE dispatch
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
- Genius API token → [genius.com/api-clients](https://genius.com/api-clients)

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env and fill in:
#   SPOTIFY_CLIENT_ID
#   SPOTIFY_CLIENT_SECRET
#   GENIUS_ACCESS_TOKEN
```

### 3. Launch the full pipeline

```bash
docker-compose up -d
```

All workers start automatically. The seeder runs once and seeds the work queues;
the other workers poll indefinitely.

### 4. Monitor progress

```bash
# Follow logs from all workers
docker-compose logs -f

# Follow a specific worker
docker-compose logs -f playlist-worker

# Check pipeline stats directly in MongoDB
docker-compose exec mongo mongosh music_enricher --eval \
  'db.system_stats.findOne({doc_id: "global"}, {_id: 0})'

# Count tracks by status
docker-compose exec mongo mongosh music_enricher --eval \
  'db.tracks.aggregate([{$group:{_id:"$status", count:{$sum:1}}}]).toArray()'
```

### 5. Export enriched tracks

```bash
# Run exporter once
docker-compose run --rm exporter

# Files land in the exports volume
docker-compose exec exporter ls /data/exports/
```

### 6. Graceful stop

```bash
docker-compose down
```

Workers catch SIGTERM, finish the current batch, and exit cleanly.
All progress is persisted in MongoDB — resume by running `docker-compose up -d` again.

---

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `MONGODB_URI` | `mongodb://mongo:27017` | MongoDB connection string |
| `MONGODB_DB` | `music_enricher` | Database name |
| `SPOTIFY_CLIENT_ID` | — | **Required** |
| `SPOTIFY_CLIENT_SECRET` | — | **Required** |
| `SPOTIFY_RATE_LIMIT_RPS` | `10.0` | Spotify requests/second |
| `GENIUS_ACCESS_TOKEN` | — | **Required** |
| `GENIUS_RATE_LIMIT_RPS` | `3.0` | Genius requests/second |
| `GENIUS_MIN_CONFIDENCE` | `0.6` | Min fuzzy match score (0–1) |
| `BATCH_SIZE` | `50` | Documents per worker iteration |
| `WORKER_SLEEP_SEC` | `5` | Seconds to sleep when queue empty |
| `WORKER_RETRY_LIMIT` | `3` | Max retries before status=failed |
| `QUALITY_THRESHOLD` | `0.1` | Min quality_score to keep |
| `MIN_PLAYLIST_FOLLOWERS` | `10000` | Min followers for playlist seeding |
| `GENRE_MAX_OFFSET` | `1000` | Max search pagination offset |
| `STATS_LOG_INTERVAL_MIN` | `10` | Minutes between progress logs |
| `EXPORT_DIR` | `/data/exports` | Output directory |

---

## Quality Scoring Formula

```
quality_score =
    0.40 × (popularity / 100)
    0.30 × (artist_followers / 50_000_000)
    0.20 × (appearance_score / 200)
    0.10 × (markets_count / 185)
```

All components clamped to [0, 1]. Tracks with `quality_score < QUALITY_THRESHOLD`
receive `status = filtered_out` and are excluded from the export.

---

## Data Sources

| Source | Priority | Description |
|---|---|---|
| **Playlist Mining** | Highest | Editorial + featured + category + keyword playlists. Increments `appearance_score` on each hit — the best proxy for all-time popularity. |
| **Artist Expansion** | Medium | Full discographies (albums + singles) of every discovered artist. |
| **Genre Expansion** | Background | Paginated search across 200+ genres to fill gaps. |

---

## Deduplication

Tracks are never duplicated:

1. **ISRC** (primary) — globally unique identifier, used when available
2. **Fingerprint** (fallback) — `sha256(normalized_name | first_artist_id | duration_bucket(2s))`

---

## Rate Limiting

Each API has its own **async token-bucket limiter**:

- Spotify: 10 req/s (configurable)
- Genius: 3 req/s (configurable)

On HTTP 429, the worker automatically sleeps for `Retry-After` seconds
before retrying. All network calls use **tenacity** with exponential
backoff + jitter.

---

## Scaling

To process faster, add more worker replicas:

```yaml
# docker-compose.override.yml
services:
  audio-features-worker:
    deploy:
      replicas: 3
  lyrics-worker:
    deploy:
      replicas: 2
```

Workers compete for the same MongoDB queue — no coordination needed.
The optimistic locking pattern handles concurrent workers safely.

---

## MongoDB Collections

| Collection | Description |
|---|---|
| `tracks` | Main track documents with full enrichment data |
| `artists` | Artist metadata (genres, followers, popularity) |
| `system_stats` | Single-document pipeline counters |
| `playlist_queue` | Work queue of playlists to scrape |
| `artist_queue` | Work queue of artists to expand |
| `genre_queue` | Work queue of genres + pagination offset |

---

## Troubleshooting

**Worker shows "no work available" immediately**
- Check that the seeder completed successfully: `docker-compose logs seeder`
- Verify MongoDB is healthy: `docker-compose ps`

**Spotify 403 on audio-features**
- This endpoint is restricted on some API tiers. The worker handles it
  gracefully — tracks still progress through the pipeline with null audio_features.

**Genius low confidence / no lyrics**
- Instrumental tracks, DJ mixes, and ambient music won't match Genius.
  They still get scored and exported; `has_lyrics` will be `false`.

**Re-seeding after adding genres or playlists**
- The seeder uses `$setOnInsert` — safe to re-run without duplicating.
- `docker-compose restart seeder`

---

---

# v2: Regional & MusicBrainz Extension

## Overview

v2 adds four supplementary workers that run **orthogonally** to the main pipeline:

```
Main pipeline (status-driven, unchanged):
  base_collected → audio_features_added → lyrics_added → enriched

v2 supplementary passes (flag-driven, non-blocking):
  language_worker       → sets language, script, regions, mb_priority
  transliteration_worker → sets normalized_name, transliterated_name
  musicbrainz_worker    → sets musicbrainz{}, composers, lyricists, language
  regional_seed_worker  → inserts regional tracks as base_collected
```

None of the v2 workers change the `status` field. They set their own boolean
completion flags (`language_detected`, `transliteration_done`, `musicbrainz_enriched`)
so they never interfere with the v1 pipeline state machine.

---

## New Worker Architecture

### `regional_seed_worker`
Self-seeding on first run. Populates `regional_seed_queue` with targeted
Spotify searches across 50+ regional queries covering:

| Region | Countries | Sample Queries |
|---|---|---|
| Central Asia | UZ, KZ, KG, TJ, TM | `uzbek pop`, `kazakh hip hop`, `kyrgyz music` |
| CIS | RU, UA, BY, AZ, GE, AM | `russian rap`, `ukrainian pop`, `shanson`, `estrada` |
| MENA | AE, SA, EG, QA, MA, TR | `khaleeji`, `arabic hip hop`, `mahraganat`, `rai` |

Pagination state is persisted in MongoDB (offset field) — crash-safe.

### `language_worker`
Runs on all tracks with `language_detected=False`. Source priority:
1. **MusicBrainz** `text-representation.language` (authoritative)
2. **Genius lyrics** text → `langdetect` (reliable, 500+ chars)
3. **Track name + artist** → script detection (fallback, low confidence)

Also classifies regional flags (`regions.cis`, `regions.central_asia`, `regions.mena`)
and sets `mb_priority` for MusicBrainz queue ordering.

### `transliteration_worker`
Converts non-Latin track names to Latin script for:
- Better MusicBrainz search recall (Genius/MB store names in Latin)
- Cross-script deduplication
- Downstream analytics

| Script | Method |
|---|---|
| Russian/Ukrainian Cyrillic | `transliterate` package (BGN/PCGN tables) |
| Uzbek Cyrillic | Custom mapping → official 1995 Uzbek Latin alphabet |
| Arabic | Simplified ALA-LC romanization (in-house mapping) |
| Georgian | `transliterate` package |
| Armenian | `transliterate` package |

### `musicbrainz_worker`
Enriches tracks with composer, lyricist, release country, and canonical metadata.

**Priority queue** (claimed first):

| mb_priority | Region |
|---|---|
| 3 | Central Asia (UZ, KZ, KG, TJ, TM) |
| 2 | CIS (RU, UA, BY, AZ, GE, AM, …) |
| 1 | MENA (AE, SA, EG, QA, …) |
| 0 | All other tracks |

**Eligibility**: `musicbrainz_enriched=False` AND (`quality_score ≥ MB_PRIORITY_THRESHOLD` OR `mb_priority > 0`)

Regional tracks are always enriched regardless of quality score.

**⚠️ Rate limit**: Strictly 1 req/s. Run exactly **one** container instance.
Do not scale this service horizontally.

---

## New Data Fields

### `tracks` collection additions

```json
{
  "markets": ["UZ", "KZ", "RU", "AE", ...],

  "musicbrainz": {
    "mbid": "recording-uuid",
    "canonical_title": "Official Track Title",
    "aliases": ["Alternative Title", "..."],
    "composers": ["Composer Name"],
    "lyricists": ["Lyricist Name"],
    "first_release_date": "2021-03-15",
    "release_countries": ["UZ", "KZ"],
    "labels": ["Label Name"],
    "language": "uz",
    "script": "latin",
    "tags": ["uzbek pop", "central asian"]
  },
  "musicbrainz_confidence_score": 0.91,
  "musicbrainz_enriched": true,

  "language": "uz",
  "script": "latin",
  "language_source": "musicbrainz",
  "language_detected": true,

  "normalized_name": "kecha va kunduz",
  "transliterated_name": "kecha va kunduz",
  "normalized_artist_name": "sherzod",
  "transliteration_done": true,

  "regions": {
    "cis": true,
    "central_asia": true,
    "mena": false,
    "countries": ["KZ", "UZ"],
    "artist_country": "UZ",
    "artist_origin_region": "central_asia"
  },

  "mb_priority": 3,
  "regional_score": 0.72,
  "quality_score": 0.48
}
```

---

## Regional Quality Scoring (v2)

When `REGIONAL_BOOST_ENABLED=true`:

```
quality_score = (1 - REGIONAL_BOOST_WEIGHT) × base_score
              + REGIONAL_BOOST_WEIGHT × regional_score

regional_score =
    market_overlap_with_target_regions  (0.0–0.5)
    + language_match_bonus              (0.0–0.3)
    + artist_country_match_bonus        (0.0–0.2)
```

With `REGIONAL_BOOST_WEIGHT=0.15`:
- A Central Asian track with `regional_score=1.0` gets +15% to its score
- A track with zero regional presence is penalized 15% of the weight
- Set `REGIONAL_BOOST_ENABLED=false` to revert to v1 formula exactly

---

## New Environment Variables (v2)

| Variable | Default | Description |
|---|---|---|
| `MUSICBRAINZ_ENABLED` | `true` | Enable MusicBrainz enrichment |
| `MUSICBRAINZ_USER_AGENT` | `MusicEnricher/1.0 (…)` | **Required by MusicBrainz** — use your email |
| `MUSICBRAINZ_RATE_LIMIT_RPS` | `1.0` | **Do not increase** — MusicBrainz policy |
| `MUSICBRAINZ_PRIORITY_THRESHOLD` | `0.3` | Min quality_score for non-regional tracks |
| `MUSICBRAINZ_MIN_CONFIDENCE` | `0.75` | Min match confidence to store MB data |
| `MUSICBRAINZ_DURATION_TOLERANCE_MS` | `3000` | ±ms for duration matching |
| `TARGET_REGIONS` | `cis,central_asia,mena` | Regions to target |
| `REGIONAL_BOOST_ENABLED` | `true` | Use regional scoring formula |
| `REGIONAL_BOOST_WEIGHT` | `0.15` | Regional score weight (0–1) |
| `REGIONAL_GENRE_MAX_OFFSET` | `500` | Regional search pagination limit |
| `LANGUAGE_DETECTION_ENABLED` | `true` | Enable language_worker |
| `LANGUAGE_MIN_TEXT_LEN` | `20` | Min chars for reliable langdetect |
| `TRANSLITERATION_ENABLED` | `true` | Enable transliteration_worker |

---

## New MongoDB Collections (v2)

| Collection | Description |
|---|---|
| `regional_seed_queue` | Regional search queries with offset pagination |

---

## New Indexes (v2)

```
tracks.language           — language_worker polling
tracks.script             — transliteration routing
tracks.language_detected  — language_worker polling
tracks.musicbrainz_enriched + mb_priority — MB worker priority queue
tracks.regions.cis        — regional analytics
tracks.regions.central_asia
tracks.regions.mena
```

---

## Running v2

```bash
# Start all workers including v2
docker-compose up -d

# Monitor MB enrichment progress
docker-compose exec mongo mongosh music_enricher --eval '
  db.tracks.aggregate([
    {$group: {
      _id: "$musicbrainz_enriched",
      count: {$sum: 1}
    }}
  ]).toArray()
'

# Check regional distribution
docker-compose exec mongo mongosh music_enricher --eval '
  db.tracks.aggregate([
    {$match: {"regions.central_asia": true}},
    {$group: {_id: "$language", count: {$sum: 1}}},
    {$sort: {count: -1}}
  ]).toArray()
'

# Count tracks by script
docker-compose exec mongo mongosh music_enricher --eval '
  db.tracks.aggregate([
    {$group: {_id: "$script", count: {$sum: 1}}},
    {$sort: {count: -1}}
  ]).toArray()
'
```

---

## MusicBrainz API Notes

- **No API key required** for read-only access
- **User-Agent is mandatory** — set `MUSICBRAINZ_USER_AGENT` to include your contact
- **Rate limit: 1 req/s** — strictly enforced. Exceeding it results in IP ban (24h+)
- **ISRC lookup** is the most precise path — always used when ISRC is available
- **Fuzzy matching** combines title similarity (65%), artist similarity (20%), duration (15%)
- **Confidence threshold** (`MUSICBRAINZ_MIN_CONFIDENCE=0.75`) prevents false matches

---

## Troubleshooting v2

**MusicBrainz worker returns no matches for CIS tracks**
- Cyrillic track names may not match MB's Latin-script index
- The transliteration_worker must run first to generate `transliterated_name`
- MB worker uses both original and transliterated names for search

**Language detection wrong for short track names**
- Very short names (< 4 chars) cannot be reliably detected
- Set `LANGUAGE_MIN_TEXT_LEN=20` to require minimum text before detection
- MusicBrainz data (if available) overrides langdetect results

**Regional seed queue not populated**
- regional_seed_worker self-seeds on first run
- Check: `db.regional_seed_queue.countDocuments({})` — should be 180+ items
- Restart the worker if count is 0: `docker-compose restart regional-seed-worker`
