"""
Text normalization and transliteration utilities.

Supports:
- Script detection (Cyrillic, Arabic, Georgian, Armenian, Latin, …)
- Cyrillic → Latin via the ``transliterate`` package
  (Russian, Ukrainian, Belarusian, Georgian, Armenian, Kazakh, etc.)
- Uzbek Cyrillic → Uzbek Latin (official 1995 alphabet)
- Arabic → Latin (simplified ALA-LC romanization)
- Generic text normalization (remove parentheticals, lowercase, strip)

Used by:
- transliteration_worker  — generates normalized/transliterated fields
- musicbrainz_worker      — normalizes queries before MusicBrainz search
- genius.py               — fallback query for non-Latin track names
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional


# ── Script detection ──────────────────────────────────────────────────────────

# Unicode block ranges for script detection
_SCRIPT_RANGES = [
    ("cyrillic", 0x0400, 0x04FF),
    ("cyrillic", 0x0500, 0x052F),   # Cyrillic Supplement
    ("arabic",   0x0600, 0x06FF),
    ("arabic",   0x0750, 0x077F),   # Arabic Supplement
    ("arabic",   0xFB50, 0xFDFF),   # Arabic Presentation Forms-A
    ("arabic",   0xFE70, 0xFEFF),   # Arabic Presentation Forms-B
    ("georgian", 0x10A0, 0x10FF),
    ("armenian", 0x0530, 0x058F),
    ("hebrew",   0x0590, 0x05FF),
    ("devanagari", 0x0900, 0x097F),
    ("cjk",      0x4E00, 0x9FFF),
    ("cjk",      0xAC00, 0xD7AF),   # Hangul
]


def detect_script(text: str) -> str:
    """
    Detect the dominant Unicode script of a text string.

    Returns one of: 'cyrillic', 'arabic', 'georgian', 'armenian',
    'hebrew', 'devanagari', 'cjk', 'latin', 'mixed', 'unknown'.
    """
    counts: dict[str, int] = {}
    latin_count = 0

    for ch in text:
        cp = ord(ch)
        matched = False
        for script, lo, hi in _SCRIPT_RANGES:
            if lo <= cp <= hi:
                counts[script] = counts.get(script, 0) + 1
                matched = True
                break
        if not matched and ch.isalpha():
            # Any unmatched letter is treated as Latin
            latin_count += 1

    if not counts and latin_count == 0:
        return "unknown"

    if latin_count:
        counts["latin"] = latin_count

    if not counts:
        return "unknown"

    dominant = max(counts, key=lambda k: counts[k])
    total = sum(counts.values())
    dominance = counts[dominant] / total if total > 0 else 0

    if dominance < 0.5 and len(counts) > 1:
        return "mixed"
    return dominant


# ── Text normalization ────────────────────────────────────────────────────────

# Suffixes to strip when normalizing track names
_SUFFIX_RE = re.compile(
    r"\s*[\(\[【]"
    r"(?:remaster(?:ed)?|live|radio\s+edit|explicit|clean|version|edit|"
    r"mix|remix|extended|acoustic|instrumental|feat\.?.*|ft\.?.*|"
    r"original\s+mix|club\s+mix|single\s+version|\d{4}\s+remaster)"
    r".*?[\)\]】]",
    flags=re.IGNORECASE,
)
_FEAT_RE = re.compile(r"\s*(?:feat\.?|ft\.?|featuring)\s+.+$", flags=re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    """
    Normalize a track/artist name for comparison and indexing.

    - Unicode NFKC normalization
    - Remove remaster/remix/live suffixes
    - Remove featuring clauses
    - Lowercase + strip
    """
    text = unicodedata.normalize("NFKC", text)
    text = _SUFFIX_RE.sub("", text)
    text = _FEAT_RE.sub("", text)
    text = text.lower().strip()
    text = _WHITESPACE_RE.sub(" ", text)
    return text


# ── Uzbek Cyrillic → Latin (official 1995 alphabet) ──────────────────────────

_UZBEK_CYR_LAT: dict[str, str] = {
    "А": "A", "а": "a",
    "Б": "B", "б": "b",
    "В": "V", "в": "v",
    "Г": "G", "г": "g",
    "Д": "D", "д": "d",
    "Е": "Ye", "е": "ye",
    "Ё": "Yo", "ё": "yo",
    "Ж": "J",  "ж": "j",
    "З": "Z",  "з": "z",
    "И": "I",  "и": "i",
    "Й": "Y",  "й": "y",
    "К": "K",  "к": "k",
    "Л": "L",  "л": "l",
    "М": "M",  "м": "m",
    "Н": "N",  "н": "n",
    "О": "O",  "о": "o",
    "П": "P",  "п": "p",
    "Р": "R",  "р": "r",
    "С": "S",  "с": "s",
    "Т": "T",  "т": "t",
    "У": "U",  "у": "u",
    "Ф": "F",  "ф": "f",
    "Х": "X",  "х": "x",
    "Ц": "Ts", "ц": "ts",
    "Ч": "Ch", "ч": "ch",
    "Ш": "Sh", "ш": "sh",
    "Щ": "Sh", "щ": "sh",
    "Ъ": "",   "ъ": "'",
    "Ы": "I",  "ы": "i",
    "Ь": "",   "ь": "",
    "Э": "E",  "э": "e",
    "Ю": "Yu", "ю": "yu",
    "Я": "Ya", "я": "ya",
    # Uzbek-specific
    "Ғ": "G'", "ғ": "g'",
    "Қ": "Q",  "қ": "q",
    "Ҳ": "H",  "ҳ": "h",
    "Ў": "O'", "ў": "o'",
    "Ҷ": "J",  "ҷ": "j",
}


def uzbek_cyrillic_to_latin(text: str) -> str:
    """Transliterate Uzbek Cyrillic to official Uzbek Latin (1995 standard)."""
    result = []
    for ch in text:
        result.append(_UZBEK_CYR_LAT.get(ch, ch))
    return "".join(result)


# ── Arabic → Latin (simplified ALA-LC romanization) ──────────────────────────

_ARABIC_LAT: dict[str, str] = {
    "ا": "a", "أ": "a", "إ": "i", "آ": "a", "ٱ": "a",
    "ب": "b", "ت": "t", "ث": "th", "ج": "j",
    "ح": "h", "خ": "kh", "د": "d", "ذ": "dh",
    "ر": "r", "ز": "z", "س": "s", "ش": "sh",
    "ص": "s", "ض": "d", "ط": "t", "ظ": "z",
    "ع": "",  "غ": "gh", "ف": "f", "ق": "q",
    "ك": "k", "ل": "l", "م": "m", "ن": "n",
    "ه": "h", "و": "w", "ي": "y", "ى": "a",
    "ة": "a", "ء": "",  "ئ": "i", "ؤ": "u",
    # Diacritics
    "َ": "a", "ِ": "i", "ُ": "u",
    "ً": "an", "ٍ": "in", "ٌ": "un",
    "ّ": "",  "ْ": "",
    # Persian letters (also appear in Arabic script)
    "پ": "p", "چ": "ch", "ژ": "zh", "گ": "g",
    # Kashida (elongation) — ignore
    "ـ": "",
    # Hamza variants
    "ٲ": "a",
}


def arabic_to_latin(text: str) -> str:
    """
    Simplified transliteration of Arabic-script text to Latin.

    Not linguistically perfect — designed for search/matching purposes,
    not for scholarly romanization.
    """
    result = []
    for ch in text:
        result.append(_ARABIC_LAT.get(ch, ch))
    # Collapse multiple spaces, strip
    joined = "".join(result)
    joined = _WHITESPACE_RE.sub(" ", joined).strip()
    return joined


# ── Cyrillic → Latin (multi-language via transliterate package) ───────────────

_TRANSLITERATE_LANG_MAP = {
    "ru": "ru",   # Russian
    "uk": "uk",   # Ukrainian
    "be": "ru",   # Belarusian — fallback to Russian tables
    "bg": "bg",   # Bulgarian
    "mk": "mk",   # Macedonian
    "sr": "sr",   # Serbian
    "mn": "mn",   # Mongolian
    "ka": "ka",   # Georgian
    "hy": "hy",   # Armenian
    "kk": "ru",   # Kazakh Cyrillic — Russian tables as approx.
}


def cyrillic_to_latin(text: str, language: Optional[str] = None) -> str:
    """
    Transliterate Cyrillic (or Georgian/Armenian) text to Latin.

    Dispatch order:
    1. Uzbek (uz) — use custom Uzbek Latin mapping
    2. Other known language codes — use ``transliterate`` package
    3. Unknown language — attempt Russian tables as default

    Args:
        text: Source text.
        language: ISO 639-1 language code (e.g. "ru", "uk", "uz").

    Returns:
        Transliterated Latin string.
    """
    lang = language or "ru"

    if lang == "uz":
        return uzbek_cyrillic_to_latin(text)

    try:
        from transliterate import translit
        tl_lang = _TRANSLITERATE_LANG_MAP.get(lang, "ru")
        return translit(text, tl_lang, reversed=True)
    except Exception:
        # Fallback: replace common chars with ASCII approximations
        return _naive_cyrillic_latin(text)


def _naive_cyrillic_latin(text: str) -> str:
    """Bare-minimum Cyrillic → ASCII fallback when transliterate fails."""
    _MAP = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
        "е": "e", "ё": "yo", "ж": "zh", "з": "z", "и": "i",
        "й": "y", "к": "k", "л": "l", "м": "m", "н": "n",
        "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
        "у": "u", "ф": "f", "х": "kh", "ц": "ts", "ч": "ch",
        "ш": "sh", "щ": "shch", "ъ": "", "ы": "y", "ь": "",
        "э": "e", "ю": "yu", "я": "ya",
    }
    return "".join(_MAP.get(ch.lower(), ch) for ch in text)


# ── Main public function ──────────────────────────────────────────────────────

def transliterate_track_name(
    name: str,
    script: Optional[str],
    language: Optional[str],
) -> Optional[str]:
    """
    Transliterate a track name to Latin script.

    Args:
        name: Original track name.
        script: Detected script ('cyrillic', 'arabic', 'georgian', 'armenian').
        language: Detected language (ISO 639-1).

    Returns:
        Transliterated string or ``None`` if no transliteration is needed
        (already Latin or unknown script).
    """
    if not name or not script:
        return None

    if script == "arabic":
        return arabic_to_latin(name)

    if script in ("cyrillic", "georgian", "armenian"):
        return cyrillic_to_latin(name, language)

    return None
