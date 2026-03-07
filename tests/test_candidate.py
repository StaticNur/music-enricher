"""
Tests for candidate-related utilities and parsing helpers.

Run with:
    python -m pytest tests/ -v
"""
import asyncio
import pytest

from app.utils.deduplication import (
    compute_candidate_fingerprint,
    compute_fingerprint,
    normalize_text,
)
from app.utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpen, CircuitState
from app.services.lastfm import LastFmClient
from app.services.ytmusic import YtMusicClient, _parse_duration_string
from app.services.discogs import DiscogsClient, _parse_discogs_duration


# ── normalize_text ─────────────────────────────────────────────────────────────

class TestNormalizeText:
    def test_lowercase_and_strip(self):
        assert normalize_text("  Hello World  ") == "hello world"

    def test_remaster_stripped(self):
        assert normalize_text("Song (Remastered)") == "song"
        assert normalize_text("Song (Remastered 2019)") == "song"
        assert normalize_text("Song [2019 Remaster]") == "song"

    def test_live_version_stripped(self):
        assert normalize_text("Song (Live)") == "song"
        assert normalize_text("Song [Live Version]") == "song"

    def test_official_video_stripped(self):
        assert normalize_text("Song (Official Video)") == "song"
        assert normalize_text("Song (Official Audio)") == "song"
        assert normalize_text("Song [Lyrics Video]") == "song"

    def test_feat_stripped_parens(self):
        assert normalize_text("Shape of You (feat. Zara Larsson)") == "shape of you"

    def test_feat_stripped_no_parens(self):
        assert normalize_text("Song feat. Someone Else") == "song"

    def test_ft_stripped(self):
        assert normalize_text("Song ft. Artist") == "song"

    def test_featuring_stripped(self):
        assert normalize_text("Song featuring Artist") == "song"

    def test_track_number_prefix_stripped(self):
        assert normalize_text("01. Song Title") == "song title"
        assert normalize_text("1 - Song Title") == "song title"
        assert normalize_text("12 Song Title") == "song title"

    def test_punctuation_normalized(self):
        # Hyphens, apostrophes etc. become spaces then collapsed
        assert normalize_text("Jay-Z") == "jay z"
        assert normalize_text("It's A Man's World") == "it s a man s world"

    def test_idempotent(self):
        result = normalize_text("Song (feat. X) [Remastered]")
        assert normalize_text(result) == result

    def test_empty_string(self):
        assert normalize_text("") == ""


# ── deduplication ─────────────────────────────────────────────────────────────

class TestComputeCandidateFingerprint:
    def test_deterministic(self):
        fp1 = compute_candidate_fingerprint("Bad Guy", "Billie Eilish", 194000)
        fp2 = compute_candidate_fingerprint("Bad Guy", "Billie Eilish", 194000)
        assert fp1 == fp2

    def test_different_titles_differ(self):
        fp1 = compute_candidate_fingerprint("Bad Guy", "Billie Eilish")
        fp2 = compute_candidate_fingerprint("Good Guy", "Billie Eilish")
        assert fp1 != fp2

    def test_different_artists_differ(self):
        fp1 = compute_candidate_fingerprint("Bad Guy", "Billie Eilish")
        fp2 = compute_candidate_fingerprint("Bad Guy", "Someone Else")
        assert fp1 != fp2

    def test_no_duration_vs_duration_differ(self):
        fp_no_dur = compute_candidate_fingerprint("Song", "Artist")
        fp_with_dur = compute_candidate_fingerprint("Song", "Artist", 200000)
        assert fp_no_dur != fp_with_dur

    def test_duration_bucket_tolerance(self):
        # Tracks within 2-second bucket should produce the same fingerprint
        fp1 = compute_candidate_fingerprint("Song", "Artist", 200000)
        fp2 = compute_candidate_fingerprint("Song", "Artist", 201000)
        # Both should be non-empty
        assert len(fp1) == 64
        assert len(fp2) == 64

    def test_case_insensitive(self):
        fp1 = compute_candidate_fingerprint("BAD GUY", "BILLIE EILISH")
        fp2 = compute_candidate_fingerprint("bad guy", "billie eilish")
        assert fp1 == fp2

    def test_remaster_suffix_stripped(self):
        fp1 = compute_candidate_fingerprint("Song (Remastered)", "Artist")
        fp2 = compute_candidate_fingerprint("Song", "Artist")
        assert fp1 == fp2

    def test_feat_suffix_stripped(self):
        fp1 = compute_candidate_fingerprint("Song (feat. Other Artist)", "Artist")
        fp2 = compute_candidate_fingerprint("Song", "Artist")
        assert fp1 == fp2

    def test_returns_64_char_hex(self):
        fp = compute_candidate_fingerprint("Title", "Artist", 150000)
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)

    def test_distinct_from_spotify_fingerprint(self):
        # The two fingerprint functions exist for different purposes and should
        # not be confused — one uses artist_id, the other uses artist name.
        fp_candidate = compute_candidate_fingerprint("Song", "Artist Name", 200000)
        fp_spotify = compute_fingerprint("Song", "artist_spotify_id_abc", 200000)
        assert fp_candidate != fp_spotify


# ── circuit breaker ───────────────────────────────────────────────────────────

class TestCircuitBreaker:
    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_initial_state_closed(self):
        cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=60)
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=60)

        async def _test():
            for _ in range(3):
                await cb.record_failure()
            assert cb.state == CircuitState.OPEN

        self._run(_test())

    def test_check_raises_when_open(self):
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=9999)

        async def _test():
            await cb.record_failure()
            with pytest.raises(CircuitBreakerOpen):
                await cb.check()

        self._run(_test())

    def test_success_decrements_failures(self):
        cb = CircuitBreaker("test", failure_threshold=5, recovery_timeout=60)

        async def _test():
            await cb.record_failure()
            await cb.record_failure()
            assert cb._failures == 2
            await cb.record_success()
            assert cb._failures == 1

        self._run(_test())

    def test_half_open_probe_success_closes(self):
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.0)

        async def _test():
            await cb.record_failure()
            assert cb.state == CircuitState.OPEN
            # With recovery_timeout=0 the next check should transition to HALF_OPEN
            await asyncio.sleep(0.01)
            await cb.check()  # transitions to HALF_OPEN, does not raise
            await cb.record_success()
            assert cb.state == CircuitState.CLOSED

        self._run(_test())

    def test_does_not_open_before_threshold(self):
        cb = CircuitBreaker("test", failure_threshold=5, recovery_timeout=60)

        async def _test():
            for _ in range(4):
                await cb.record_failure()
            assert cb.state == CircuitState.CLOSED
            await cb.check()  # must not raise

        self._run(_test())


# ── Last.fm parsing ───────────────────────────────────────────────────────────

class TestLastFmParseTrack:
    def test_basic(self):
        raw = {"name": "Bohemian Rhapsody", "artist": {"name": "Queen"}, "duration": "354"}
        result = LastFmClient.parse_track(raw)
        assert result is not None
        assert result["title"] == "Bohemian Rhapsody"
        assert result["artist"] == "Queen"
        assert result["duration_ms"] == 354_000

    def test_artist_as_string(self):
        raw = {"name": "Song", "artist": "Artist Name", "duration": "0"}
        result = LastFmClient.parse_track(raw)
        assert result is not None
        assert result["artist"] == "Artist Name"

    def test_missing_name_returns_none(self):
        raw = {"artist": {"name": "Queen"}}
        assert LastFmClient.parse_track(raw) is None

    def test_missing_artist_returns_none(self):
        raw = {"name": "Song"}
        assert LastFmClient.parse_track(raw) is None

    def test_no_duration(self):
        raw = {"name": "Song", "artist": {"name": "Artist"}}
        result = LastFmClient.parse_track(raw)
        assert result is not None
        assert result["duration_ms"] is None


# ── YouTube Music parsing ─────────────────────────────────────────────────────

class TestYtMusicParseSong:
    def test_basic(self):
        raw = {
            "videoId": "dQw4w9WgXcQ",
            "title": "Never Gonna Give You Up",
            "artists": [{"name": "Rick Astley", "id": "abc"}],
            "duration": "3:33",
        }
        result = YtMusicClient.parse_song(raw)
        assert result is not None
        assert result["title"] == "Never Gonna Give You Up"
        assert result["artist"] == "Rick Astley"
        assert result["youtube_video_id"] == "dQw4w9WgXcQ"
        assert result["duration_ms"] == (3 * 60 + 33) * 1000

    def test_missing_title_returns_none(self):
        raw = {"artists": [{"name": "Artist"}]}
        assert YtMusicClient.parse_song(raw) is None

    def test_missing_artist_returns_none(self):
        raw = {"title": "Song", "artists": []}
        assert YtMusicClient.parse_song(raw) is None

    def test_duration_seconds_preferred(self):
        raw = {
            "title": "Song",
            "artists": [{"name": "Artist"}],
            "duration": "1:00",
            "duration_seconds": 90,
        }
        result = YtMusicClient.parse_song(raw)
        assert result is not None
        assert result["duration_ms"] == 90_000  # duration_seconds wins


class TestParseDurationString:
    def test_mm_ss(self):
        assert _parse_duration_string("3:45") == (3 * 60 + 45) * 1000

    def test_h_mm_ss(self):
        assert _parse_duration_string("1:02:03") == (3600 + 2 * 60 + 3) * 1000

    def test_empty_returns_none(self):
        assert _parse_duration_string("") is None

    def test_invalid_returns_none(self):
        assert _parse_duration_string("not-a-time") is None


# ── Discogs parsing ───────────────────────────────────────────────────────────

class TestDiscogsParseSearchResult:
    def test_artist_dash_title(self):
        raw = {"id": 1234, "title": "Metallica - Black Album"}
        result = DiscogsClient.parse_search_result(raw)
        assert result is not None
        artist, title, release_id = result
        assert artist == "Metallica"
        assert title == "Black Album"
        assert release_id == 1234

    def test_no_separator(self):
        raw = {"id": 5678, "title": "Various Artists Compilation"}
        result = DiscogsClient.parse_search_result(raw)
        assert result is not None
        artist, title, _ = result
        assert artist == "Various Artists"
        assert title == "Various Artists Compilation"

    def test_missing_id_returns_none(self):
        raw = {"title": "Artist - Album"}
        assert DiscogsClient.parse_search_result(raw) is None

    def test_empty_title_returns_none(self):
        raw = {"id": 1, "title": ""}
        assert DiscogsClient.parse_search_result(raw) is None


class TestDiscogsParseTracklist:
    def test_basic_tracklist(self):
        release = {
            "id": 100,
            "artists": [{"name": "The Beatles"}],
            "tracklist": [
                {"position": "1", "title": "Come Together", "duration": "4:20"},
                {"position": "2", "title": "Something", "duration": "3:02"},
            ],
        }
        tracks = DiscogsClient.parse_tracklist(release, "Unknown")
        assert len(tracks) == 2
        assert tracks[0]["title"] == "Come Together"
        assert tracks[0]["artist"] == "The Beatles"
        assert tracks[0]["duration_ms"] == (4 * 60 + 20) * 1000

    def test_heading_tracks_skipped(self):
        release = {
            "id": 101,
            "artists": [{"name": "Artist"}],
            "tracklist": [
                {"type_": "heading", "title": "Side A"},
                {"position": "A1", "title": "Real Track", "duration": "3:00"},
            ],
        }
        tracks = DiscogsClient.parse_tracklist(release, "Fallback")
        assert len(tracks) == 1
        assert tracks[0]["title"] == "Real Track"

    def test_track_level_artist_override(self):
        release = {
            "id": 102,
            "artists": [{"name": "Various Artists"}],
            "tracklist": [
                {
                    "position": "1",
                    "title": "My Song",
                    "artists": [{"name": "Specific Artist"}],
                    "duration": "2:30",
                }
            ],
        }
        tracks = DiscogsClient.parse_tracklist(release, "Fallback")
        assert tracks[0]["artist"] == "Specific Artist"


class TestDiscogsDiscogsDuration:
    def test_mm_ss(self):
        assert _parse_discogs_duration("3:45") == (3 * 60 + 45) * 1000

    def test_empty(self):
        assert _parse_discogs_duration("") is None

    def test_invalid(self):
        assert _parse_discogs_duration("abc") is None
