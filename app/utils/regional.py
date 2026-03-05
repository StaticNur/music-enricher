"""
Regional classification utilities.

Determines whether a track belongs to CIS, Central Asia, or MENA regions
based on three independent signals (OR-aggregated):
1. Spotify available_markets list
2. Detected language (ISO 639-1)
3. Artist country from MusicBrainz

All sets are uppercase ISO-3166-1 alpha-2 country codes.
"""
from __future__ import annotations

from typing import List, Optional, Set

# ── Region definitions ────────────────────────────────────────────────────────

# CIS: post-Soviet states (Commonwealth of Independent States + Baltic former SSRs)
CIS_COUNTRIES: Set[str] = {
    "RU",  # Russia
    "UA",  # Ukraine
    "BY",  # Belarus
    "KZ",  # Kazakhstan
    "UZ",  # Uzbekistan
    "TM",  # Turkmenistan
    "TJ",  # Tajikistan
    "KG",  # Kyrgyzstan
    "AZ",  # Azerbaijan
    "AM",  # Armenia
    "GE",  # Georgia
    "MD",  # Moldova
    # Historically associated
    "LV",  # Latvia
    "LT",  # Lithuania
    "EE",  # Estonia
}

CENTRAL_ASIA_COUNTRIES: Set[str] = {
    "UZ",  # Uzbekistan
    "KZ",  # Kazakhstan
    "KG",  # Kyrgyzstan
    "TJ",  # Tajikistan
    "TM",  # Turkmenistan
    "AF",  # Afghanistan (culturally adjacent)
}

# MENA: Middle East & North Africa
MENA_COUNTRIES: Set[str] = {
    "AE",  # UAE
    "SA",  # Saudi Arabia
    "EG",  # Egypt
    "QA",  # Qatar
    "KW",  # Kuwait
    "BH",  # Bahrain
    "OM",  # Oman
    "JO",  # Jordan
    "LB",  # Lebanon
    "SY",  # Syria
    "IQ",  # Iraq
    "IR",  # Iran
    "MA",  # Morocco
    "DZ",  # Algeria
    "TN",  # Tunisia
    "LY",  # Libya
    "YE",  # Yemen
    "PS",  # Palestine
    "IL",  # Israel
    "TR",  # Turkey (partly)
    "PK",  # Pakistan (partly)
}

# ── Language → region mapping ─────────────────────────────────────────────────

CIS_LANGUAGES: Set[str] = {
    "ru",  # Russian
    "uk",  # Ukrainian
    "be",  # Belarusian
    "az",  # Azerbaijani
    "hy",  # Armenian
    "ka",  # Georgian
    "mo",  # Moldovan (= Romanian)
    "ro",  # Romanian (Moldova context)
}

CENTRAL_ASIA_LANGUAGES: Set[str] = {
    "uz",  # Uzbek
    "kk",  # Kazakh
    "ky",  # Kyrgyz
    "tg",  # Tajik
    "tk",  # Turkmen
}

MENA_LANGUAGES: Set[str] = {
    "ar",  # Arabic
    "fa",  # Persian/Farsi
    "he",  # Hebrew
    "ur",  # Urdu
    "ps",  # Pashto
    "ku",  # Kurdish
    "tr",  # Turkish
}

# ── Spotify market codes by region (subset used for market-based detection) ───
CIS_MARKETS: Set[str] = CIS_COUNTRIES
CENTRAL_ASIA_MARKETS: Set[str] = CENTRAL_ASIA_COUNTRIES
MENA_MARKETS: Set[str] = MENA_COUNTRIES

# ── MusicBrainz priority assignment ──────────────────────────────────────────
MB_PRIORITY_MAP = {
    "central_asia": 3,
    "cis": 2,
    "mena": 1,
}


def classify_regions(
    markets: List[str],
    language: Optional[str],
    artist_country: Optional[str],
    mb_release_countries: Optional[List[str]] = None,
) -> dict:
    """
    Classify a track into regions based on all available signals.

    Args:
        markets: Spotify available_markets list (ISO-3166-1 alpha-2).
        language: Detected language (ISO 639-1), may be None.
        artist_country: Artist country from MusicBrainz, may be None.
        mb_release_countries: Release countries from MusicBrainz.

    Returns:
        Dict compatible with ``RegionData.model_validate()``.
    """
    markets_upper = {m.upper() for m in markets}
    all_countries = set(markets_upper)
    if artist_country:
        all_countries.add(artist_country.upper())
    if mb_release_countries:
        all_countries.update(c.upper() for c in mb_release_countries)

    is_cis = bool(
        all_countries & CIS_COUNTRIES
        or (language and language in CIS_LANGUAGES | CENTRAL_ASIA_LANGUAGES)
    )
    is_central_asia = bool(
        all_countries & CENTRAL_ASIA_COUNTRIES
        or (language and language in CENTRAL_ASIA_LANGUAGES)
    )
    is_mena = bool(
        all_countries & MENA_COUNTRIES
        or (language and language in MENA_LANGUAGES)
    )

    countries_present = sorted(
        all_countries & (CIS_COUNTRIES | CENTRAL_ASIA_COUNTRIES | MENA_COUNTRIES)
    )

    return {
        "cis": is_cis,
        "central_asia": is_central_asia,
        "mena": is_mena,
        "countries": countries_present,
        "artist_country": artist_country,
        "artist_begin_area": None,
        "artist_origin_region": _artist_region(artist_country),
    }


def _artist_region(country: Optional[str]) -> Optional[str]:
    """Map artist country code to region name."""
    if not country:
        return None
    c = country.upper()
    if c in CENTRAL_ASIA_COUNTRIES:
        return "central_asia"
    if c in CIS_COUNTRIES:
        return "cis"
    if c in MENA_COUNTRIES:
        return "mena"
    return None


def compute_mb_priority(
    regions: Optional[dict],
    language: Optional[str],
    markets: Optional[List[str]],
) -> int:
    """
    Determine MusicBrainz enrichment priority (0–3).

    Higher priority → claimed first by musicbrainz_worker.
    """
    if regions:
        if regions.get("central_asia"):
            return MB_PRIORITY_MAP["central_asia"]
        if regions.get("cis"):
            return MB_PRIORITY_MAP["cis"]
        if regions.get("mena"):
            return MB_PRIORITY_MAP["mena"]

    if language:
        if language in CENTRAL_ASIA_LANGUAGES:
            return MB_PRIORITY_MAP["central_asia"]
        if language in CIS_LANGUAGES:
            return MB_PRIORITY_MAP["cis"]
        if language in MENA_LANGUAGES:
            return MB_PRIORITY_MAP["mena"]

    if markets:
        markets_set = {m.upper() for m in markets}
        if markets_set & CENTRAL_ASIA_COUNTRIES:
            return MB_PRIORITY_MAP["central_asia"]
        if markets_set & CIS_COUNTRIES:
            return MB_PRIORITY_MAP["cis"]
        if markets_set & MENA_COUNTRIES:
            return MB_PRIORITY_MAP["mena"]

    return 0


def compute_regional_score(
    markets: List[str],
    language: Optional[str],
    artist_country: Optional[str],
    target_regions: List[str],
) -> float:
    """
    Compute a [0, 1] score based on regional presence.

    Used by quality_worker when REGIONAL_BOOST_ENABLED=true.
    A higher score means the track is more strongly represented in target regions.

    Scoring breakdown:
    - Market presence in target regions: up to 0.5
    - Language match: up to 0.3
    - Artist country match: up to 0.2
    """
    if not target_regions:
        return 0.0

    target_markets: Set[str] = set()
    target_languages: Set[str] = set()
    target_countries: Set[str] = set()

    for region in target_regions:
        if region == "cis":
            target_markets |= CIS_MARKETS
            target_languages |= CIS_LANGUAGES | CENTRAL_ASIA_LANGUAGES
            target_countries |= CIS_COUNTRIES
        elif region == "central_asia":
            target_markets |= CENTRAL_ASIA_MARKETS
            target_languages |= CENTRAL_ASIA_LANGUAGES
            target_countries |= CENTRAL_ASIA_COUNTRIES
        elif region == "mena":
            target_markets |= MENA_MARKETS
            target_languages |= MENA_LANGUAGES
            target_countries |= MENA_COUNTRIES

    score = 0.0

    # Market overlap (max 0.5)
    if markets and target_markets:
        markets_set = {m.upper() for m in markets}
        overlap = len(markets_set & target_markets)
        total_target = len(target_markets)
        market_score = min(overlap / max(total_target * 0.1, 1), 1.0) * 0.5
        score += market_score

    # Language match (max 0.3)
    if language and language in target_languages:
        # Central Asia languages get full boost (rarer, more targeted)
        if language in CENTRAL_ASIA_LANGUAGES:
            score += 0.3
        else:
            score += 0.2

    # Artist country match (max 0.2)
    if artist_country and artist_country.upper() in target_countries:
        score += 0.2

    return round(min(score, 1.0), 4)
