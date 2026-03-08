"""
Regional classification utilities.

Covers all of Eurasia:
  CIS · Central Asia · MENA · Eastern Europe · South Asia · East Asia · Southeast Asia

Each region is defined by three independent signals (OR-aggregated):
  1. Spotify available_markets list (ISO-3166-1 alpha-2)
  2. Detected language (ISO 639-1)
  3. Artist country from MusicBrainz
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
    "TR",  # Turkey
    "PK",  # Pakistan (also South Asia)
}

# Eastern Europe (non-CIS EU/Balkan countries)
EASTERN_EUROPE_COUNTRIES: Set[str] = {
    "PL",  # Poland
    "CZ",  # Czech Republic
    "SK",  # Slovakia
    "HU",  # Hungary
    "RO",  # Romania
    "BG",  # Bulgaria
    "HR",  # Croatia
    "RS",  # Serbia
    "SI",  # Slovenia
    "MK",  # North Macedonia
    "BA",  # Bosnia & Herzegovina
    "AL",  # Albania
    "ME",  # Montenegro
    "XK",  # Kosovo
    "GR",  # Greece
    "CY",  # Cyprus
}

# South Asia
SOUTH_ASIA_COUNTRIES: Set[str] = {
    "IN",  # India
    "BD",  # Bangladesh
    "LK",  # Sri Lanka
    "NP",  # Nepal
    "BT",  # Bhutan
    "MV",  # Maldives
    "PK",  # Pakistan (also MENA)
}

# East Asia
EAST_ASIA_COUNTRIES: Set[str] = {
    "CN",  # China
    "JP",  # Japan
    "KR",  # South Korea
    "TW",  # Taiwan
    "HK",  # Hong Kong
    "MO",  # Macau
    "MN",  # Mongolia
}

# Southeast Asia
SOUTHEAST_ASIA_COUNTRIES: Set[str] = {
    "TH",  # Thailand
    "VN",  # Vietnam
    "ID",  # Indonesia
    "MY",  # Malaysia
    "PH",  # Philippines
    "SG",  # Singapore
    "MM",  # Myanmar
    "KH",  # Cambodia
    "LA",  # Laos
    "BN",  # Brunei
    "TL",  # Timor-Leste
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
    "ur",  # Urdu (also South Asia)
    "ps",  # Pashto
    "ku",  # Kurdish
    "tr",  # Turkish
}

EASTERN_EUROPE_LANGUAGES: Set[str] = {
    "pl",  # Polish
    "cs",  # Czech
    "sk",  # Slovak
    "hu",  # Hungarian
    "ro",  # Romanian (also CIS/Moldova)
    "bg",  # Bulgarian
    "sr",  # Serbian
    "hr",  # Croatian
    "sl",  # Slovenian
    "mk",  # Macedonian
    "sq",  # Albanian
    "bs",  # Bosnian
    "el",  # Greek
}

SOUTH_ASIA_LANGUAGES: Set[str] = {
    "hi",  # Hindi
    "bn",  # Bengali
    "ta",  # Tamil
    "te",  # Telugu
    "ml",  # Malayalam
    "kn",  # Kannada
    "mr",  # Marathi
    "gu",  # Gujarati
    "pa",  # Punjabi
    "si",  # Sinhala
    "ne",  # Nepali
    "ur",  # Urdu (also MENA)
}

EAST_ASIA_LANGUAGES: Set[str] = {
    "zh",  # Chinese (Mandarin/Cantonese)
    "ja",  # Japanese
    "ko",  # Korean
    "mn",  # Mongolian
}

SOUTHEAST_ASIA_LANGUAGES: Set[str] = {
    "th",  # Thai
    "vi",  # Vietnamese
    "id",  # Indonesian
    "ms",  # Malay
    "tl",  # Tagalog/Filipino
    "my",  # Burmese
    "km",  # Khmer
    "lo",  # Lao
}

# ── Market sets (= country sets for Spotify market detection) ─────────────────
CIS_MARKETS: Set[str] = CIS_COUNTRIES
CENTRAL_ASIA_MARKETS: Set[str] = CENTRAL_ASIA_COUNTRIES
MENA_MARKETS: Set[str] = MENA_COUNTRIES
EASTERN_EUROPE_MARKETS: Set[str] = EASTERN_EUROPE_COUNTRIES
SOUTH_ASIA_MARKETS: Set[str] = SOUTH_ASIA_COUNTRIES
EAST_ASIA_MARKETS: Set[str] = EAST_ASIA_COUNTRIES
SOUTHEAST_ASIA_MARKETS: Set[str] = SOUTHEAST_ASIA_COUNTRIES

# ── All Eurasian countries (union) ────────────────────────────────────────────
ALL_EURASIAN_COUNTRIES: Set[str] = (
    CIS_COUNTRIES | CENTRAL_ASIA_COUNTRIES | MENA_COUNTRIES
    | EASTERN_EUROPE_COUNTRIES | SOUTH_ASIA_COUNTRIES
    | EAST_ASIA_COUNTRIES | SOUTHEAST_ASIA_COUNTRIES
)

# ── MusicBrainz priority assignment ──────────────────────────────────────────
# Higher = processed first by musicbrainz_worker.
# Central Asia (3) and CIS (2) are least covered in MB → highest priority.
# All other Eurasian regions share priority 1.
MB_PRIORITY_MAP = {
    "central_asia":   3,
    "cis":            2,
    "mena":           1,
    "eastern_europe": 1,
    "south_asia":     1,
    "east_asia":      1,
    "southeast_asia": 1,
}


def classify_regions(
    markets: List[str],
    language: Optional[str],
    artist_country: Optional[str],
    mb_release_countries: Optional[List[str]] = None,
) -> dict:
    """
    Classify a track into Eurasian regions based on all available signals.

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

    lang = language  # may be None

    is_cis = bool(
        all_countries & CIS_COUNTRIES
        or (lang and lang in CIS_LANGUAGES | CENTRAL_ASIA_LANGUAGES)
    )
    is_central_asia = bool(
        all_countries & CENTRAL_ASIA_COUNTRIES
        or (lang and lang in CENTRAL_ASIA_LANGUAGES)
    )
    is_mena = bool(
        all_countries & MENA_COUNTRIES
        or (lang and lang in MENA_LANGUAGES)
    )
    is_eastern_europe = bool(
        all_countries & EASTERN_EUROPE_COUNTRIES
        or (lang and lang in EASTERN_EUROPE_LANGUAGES)
    )
    is_south_asia = bool(
        all_countries & SOUTH_ASIA_COUNTRIES
        or (lang and lang in SOUTH_ASIA_LANGUAGES)
    )
    is_east_asia = bool(
        all_countries & EAST_ASIA_COUNTRIES
        or (lang and lang in EAST_ASIA_LANGUAGES)
    )
    is_southeast_asia = bool(
        all_countries & SOUTHEAST_ASIA_COUNTRIES
        or (lang and lang in SOUTHEAST_ASIA_LANGUAGES)
    )

    countries_present = sorted(all_countries & ALL_EURASIAN_COUNTRIES)

    return {
        "cis": is_cis,
        "central_asia": is_central_asia,
        "mena": is_mena,
        "eastern_europe": is_eastern_europe,
        "south_asia": is_south_asia,
        "east_asia": is_east_asia,
        "southeast_asia": is_southeast_asia,
        "countries": countries_present,
        "artist_country": artist_country,
        "artist_begin_area": None,
        "artist_origin_region": _artist_region(artist_country),
    }


def _artist_region(country: Optional[str]) -> Optional[str]:
    """Map artist country code to primary region name."""
    if not country:
        return None
    c = country.upper()
    if c in CENTRAL_ASIA_COUNTRIES:
        return "central_asia"
    if c in CIS_COUNTRIES:
        return "cis"
    if c in MENA_COUNTRIES:
        return "mena"
    if c in EASTERN_EUROPE_COUNTRIES:
        return "eastern_europe"
    if c in SOUTH_ASIA_COUNTRIES:
        return "south_asia"
    if c in EAST_ASIA_COUNTRIES:
        return "east_asia"
    if c in SOUTHEAST_ASIA_COUNTRIES:
        return "southeast_asia"
    return None


def compute_mb_priority(
    regions: Optional[dict],
    language: Optional[str],
    markets: Optional[List[str]],
) -> int:
    """
    Determine MusicBrainz enrichment priority (0–3).

    Higher priority → claimed first by musicbrainz_worker.
    Covers all Eurasian regions; Central Asia and CIS get highest priority
    because they are least represented in MusicBrainz.
    """
    # Check region flags (set by classify_regions)
    if regions:
        if regions.get("central_asia"):
            return MB_PRIORITY_MAP["central_asia"]
        if regions.get("cis"):
            return MB_PRIORITY_MAP["cis"]
        # All remaining Eurasian regions → priority 1
        for r in ("mena", "eastern_europe", "south_asia", "east_asia", "southeast_asia"):
            if regions.get(r):
                return MB_PRIORITY_MAP[r]

    # Language fallback
    if language:
        if language in CENTRAL_ASIA_LANGUAGES:
            return MB_PRIORITY_MAP["central_asia"]
        if language in CIS_LANGUAGES:
            return MB_PRIORITY_MAP["cis"]
        if language in (
            MENA_LANGUAGES | EASTERN_EUROPE_LANGUAGES
            | SOUTH_ASIA_LANGUAGES | EAST_ASIA_LANGUAGES | SOUTHEAST_ASIA_LANGUAGES
        ):
            return 1

    # Market fallback
    if markets:
        ms = {m.upper() for m in markets}
        if ms & CENTRAL_ASIA_COUNTRIES:
            return MB_PRIORITY_MAP["central_asia"]
        if ms & CIS_COUNTRIES:
            return MB_PRIORITY_MAP["cis"]
        if ms & (
            MENA_COUNTRIES | EASTERN_EUROPE_COUNTRIES | SOUTH_ASIA_COUNTRIES
            | EAST_ASIA_COUNTRIES | SOUTHEAST_ASIA_COUNTRIES
        ):
            return 1

    return 0


def compute_regional_score(
    markets: List[str],
    language: Optional[str],
    artist_country: Optional[str],
    target_regions: List[str],
) -> float:
    """
    Compute a [0, 1] score based on regional presence across all Eurasian regions.

    Scoring breakdown:
    - Market presence in target regions: up to 0.5
    - Language match: up to 0.3
    - Artist country match: up to 0.2
    """
    if not target_regions:
        return 0.0

    _region_map = {
        "cis":            (CIS_MARKETS,            CIS_LANGUAGES | CENTRAL_ASIA_LANGUAGES, CIS_COUNTRIES),
        "central_asia":   (CENTRAL_ASIA_MARKETS,   CENTRAL_ASIA_LANGUAGES,                 CENTRAL_ASIA_COUNTRIES),
        "mena":           (MENA_MARKETS,            MENA_LANGUAGES,                         MENA_COUNTRIES),
        "eastern_europe": (EASTERN_EUROPE_MARKETS,  EASTERN_EUROPE_LANGUAGES,               EASTERN_EUROPE_COUNTRIES),
        "south_asia":     (SOUTH_ASIA_MARKETS,      SOUTH_ASIA_LANGUAGES,                   SOUTH_ASIA_COUNTRIES),
        "east_asia":      (EAST_ASIA_MARKETS,       EAST_ASIA_LANGUAGES,                    EAST_ASIA_COUNTRIES),
        "southeast_asia": (SOUTHEAST_ASIA_MARKETS,  SOUTHEAST_ASIA_LANGUAGES,               SOUTHEAST_ASIA_COUNTRIES),
    }

    target_markets: Set[str] = set()
    target_languages: Set[str] = set()
    target_countries: Set[str] = set()

    for region in target_regions:
        if region in _region_map:
            mkts, langs, ctries = _region_map[region]
            target_markets |= mkts
            target_languages |= langs
            target_countries |= ctries

    score = 0.0

    # Market overlap (max 0.5)
    if markets and target_markets:
        markets_set = {m.upper() for m in markets}
        overlap = len(markets_set & target_markets)
        market_score = min(overlap / max(len(target_markets) * 0.1, 1), 1.0) * 0.5
        score += market_score

    # Language match (max 0.3)
    if language and language in target_languages:
        # Rarer / less-documented languages get full boost
        if language in CENTRAL_ASIA_LANGUAGES:
            score += 0.3
        elif language in (EAST_ASIA_LANGUAGES | SOUTHEAST_ASIA_LANGUAGES):
            score += 0.25
        else:
            score += 0.2

    # Artist country match (max 0.2)
    if artist_country and artist_country.upper() in target_countries:
        score += 0.2

    return round(min(score, 1.0), 4)
