"""
Text normalization and transliteration utilities.

Supports:
- Script detection (Cyrillic, Arabic, Georgian, Armenian, Latin,
  CJK/Chinese, Hangul/Korean, Japanese Kana, Devanagari, Indic, Thai, …)
- Cyrillic → Latin via the ``transliterate`` package
  (Russian, Ukrainian, Belarusian, Georgian, Armenian, Kazakh, etc.)
- Uzbek Cyrillic → Uzbek Latin (official 1995 alphabet)
- Arabic → Latin (simplified ALA-LC romanization)
- Chinese → Pinyin via ``pypinyin`` (graceful fallback to unidecode)
- Japanese (Kana/Kanji) → Hepburn Romaji via ``pykakasi``
- Korean Hangul → Revised Romanization via ``unidecode``
- Devanagari / Indic scripts → ITRANS approximation via ``unidecode``
- Thai → phonetic Latin via ``unidecode``
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

# Unicode block ranges for script detection.
# Order matters: first match wins, so more specific ranges come first.
_SCRIPT_RANGES = [
    # ── Cyrillic ──────────────────────────────────────────────────────────
    ("cyrillic",  0x0400, 0x04FF),
    ("cyrillic",  0x0500, 0x052F),   # Cyrillic Supplement

    # ── Arabic / Persian / Urdu ───────────────────────────────────────────
    ("arabic",    0x0600, 0x06FF),
    ("arabic",    0x0750, 0x077F),   # Arabic Supplement
    ("arabic",    0xFB50, 0xFDFF),   # Arabic Presentation Forms-A
    ("arabic",    0xFE70, 0xFEFF),   # Arabic Presentation Forms-B

    # ── Other scripts ─────────────────────────────────────────────────────
    ("georgian",  0x10A0, 0x10FF),
    ("armenian",  0x0530, 0x058F),
    ("hebrew",    0x0590, 0x05FF),

    # ── Indic scripts (grouped; unidecode handles all) ────────────────────
    ("devanagari", 0x0900, 0x097F),  # Hindi, Marathi, Nepali, Sanskrit
    ("indic",     0x0980, 0x09FF),   # Bengali
    ("indic",     0x0A00, 0x0A7F),   # Gurmukhi (Punjabi)
    ("indic",     0x0A80, 0x0AFF),   # Gujarati
    ("indic",     0x0B00, 0x0B7F),   # Odia
    ("indic",     0x0B80, 0x0BFF),   # Tamil
    ("indic",     0x0C00, 0x0C7F),   # Telugu
    ("indic",     0x0C80, 0x0CFF),   # Kannada
    ("indic",     0x0D00, 0x0D7F),   # Malayalam
    ("indic",     0x0D80, 0x0DFF),   # Sinhala

    # ── Thai ──────────────────────────────────────────────────────────────
    ("thai",      0x0E00, 0x0E7F),

    # ── East Asian ────────────────────────────────────────────────────────
    # Japanese kana come before CJK (shorter ranges, need early match)
    ("hiragana",  0x3040, 0x309F),
    ("katakana",  0x30A0, 0x30FF),
    # CJK Unified Ideographs (Chinese Hanzi / Japanese Kanji)
    ("cjk",       0x3400, 0x4DBF),   # CJK Extension A
    ("cjk",       0x4E00, 0x9FFF),   # CJK Unified Ideographs (main block)
    ("cjk",       0x20000, 0x2A6DF), # CJK Extension B
    # Korean Hangul — separate from CJK so we can use different romanization
    ("hangul",    0xAC00, 0xD7AF),
    ("hangul",    0x1100, 0x11FF),   # Hangul Jamo
    ("hangul",    0xA960, 0xA97F),   # Hangul Jamo Extended-A
]


def detect_script(text: str) -> str:
    """
    Detect the dominant Unicode script of a text string.

    Returns one of:
      'cyrillic', 'arabic', 'georgian', 'armenian', 'hebrew',
      'devanagari', 'indic', 'thai',
      'cjk' (Chinese/mixed CJK), 'hangul' (Korean), 'hiragana', 'katakana',
      'latin', 'mixed', 'unknown'.

    'mixed' is returned when no single script exceeds 50% of alphabetic chars.
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

    # Japanese text often mixes hiragana/katakana/kanji — unify as "japanese"
    ja_count = counts.get("hiragana", 0) + counts.get("katakana", 0) + counts.get("cjk", 0)
    non_ja = total - ja_count
    if ja_count > 0 and (counts.get("hiragana", 0) + counts.get("katakana", 0)) > 0:
        if ja_count / total >= 0.5:
            return "japanese"

    if dominance < 0.5 and len(counts) > 1:
        return "mixed"
    return dominant


# ── Text normalization ────────────────────────────────────────────────────────

# Parenthetical suffixes to strip — supports ASCII, full-width, CJK brackets
_SUFFIX_RE = re.compile(
    r"\s*[\(\[（【〔「『《\[]"
    r"(?:remaster(?:ed)?(?:\s+\d{4})?|live(?:\s+version)?|radio\s+edit|"
    r"explicit|clean(?:\s+version)?|version|edit|mix|remix|extended|acoustic|"
    r"instrumental|original\s+mix|club\s+mix|single\s+version|\d{4}\s+remaster|"
    # Chinese tags
    r"现场版?|重制版?|重混版?|翻唱版?|混音版?|纯音乐|伴奏|精简版|完整版|原版|"
    # Japanese tags
    r"ライブ|リマスター|カバー|インスト|アコースティック|"
    # Korean tags
    r"라이브|리마스터|커버|어쿠스틱)"
    r".*?[\)\]）】〕」』》\]]",
    flags=re.IGNORECASE,
)
_FEAT_RE = re.compile(r"\s*(?:feat\.?|ft\.?|featuring)\s+.+$", flags=re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    """
    Normalize a track/artist name for comparison and indexing.

    - Unicode NFKC normalization (collapses full-width chars, ligatures)
    - Remove remaster/remix/live suffixes (ASCII + CJK brackets)
    - Remove featuring clauses
    - Lowercase + strip
    - Collapse whitespace

    Safe for CJK, Arabic, Devanagari, Thai and all Unicode text.
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
    not for scholarly romanization. Also handles Persian/Urdu letters.
    """
    result = []
    for ch in text:
        result.append(_ARABIC_LAT.get(ch, ch))
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
    """
    lang = language or "ru"

    if lang == "uz":
        return uzbek_cyrillic_to_latin(text)

    try:
        from transliterate import translit
        tl_lang = _TRANSLITERATE_LANG_MAP.get(lang, "ru")
        return translit(text, tl_lang, reversed=True)
    except Exception:
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


# ── CJK (Chinese) → Pinyin ────────────────────────────────────────────────────

def cjk_to_latin(text: str) -> str:
    """
    Convert Chinese characters to Pinyin romanization.

    Uses ``pypinyin`` (accurate, handles tone-less output).
    Falls back to ``unidecode`` if pypinyin is not installed.
    """
    try:
        from pypinyin import lazy_pinyin, Style  # type: ignore[import]
        parts = lazy_pinyin(text, style=Style.NORMAL, errors="ignore")
        return " ".join(p for p in parts if p).strip()
    except ImportError:
        return universal_to_latin(text)
    except Exception:
        return universal_to_latin(text)


# ── Japanese (Kana/Kanji) → Romaji ────────────────────────────────────────────

def japanese_to_latin(text: str) -> str:
    """
    Convert Japanese text (Hiragana, Katakana, Kanji) to Hepburn Romaji.

    Uses ``pykakasi``. Falls back to ``unidecode``.
    """
    try:
        import pykakasi  # type: ignore[import]
        kks = pykakasi.kakasi()
        result = kks.convert(text)
        parts = [item.get("hepburn", item.get("orig", "")) for item in result]
        return " ".join(p for p in parts if p).strip()
    except ImportError:
        return universal_to_latin(text)
    except Exception:
        return universal_to_latin(text)


# ── Korean (Hangul) → Latin ───────────────────────────────────────────────────

def korean_to_latin(text: str) -> str:
    """
    Convert Korean Hangul to Revised Romanization (approximate).

    Uses ``unidecode`` which produces a serviceable romanization for
    search/matching purposes.
    """
    return universal_to_latin(text)


# ── Universal Unicode → ASCII (unidecode fallback) ────────────────────────────

def universal_to_latin(text: str) -> str:
    """
    Convert any Unicode text to an ASCII approximation via ``unidecode``.

    Handles: Korean, Thai, Devanagari, other Indic scripts, and any
    script not covered by a dedicated function above.

    Falls back to stripping non-ASCII if unidecode is not installed.
    """
    try:
        from unidecode import unidecode  # type: ignore[import]
        result = unidecode(text)
        return _WHITESPACE_RE.sub(" ", result).strip()
    except ImportError:
        # Strip non-ASCII as last resort
        return text.encode("ascii", "ignore").decode("ascii").strip()
    except Exception:
        return text.encode("ascii", "ignore").decode("ascii").strip()


# ── Main public function ──────────────────────────────────────────────────────

def transliterate_track_name(
    name: str,
    script: Optional[str],
    language: Optional[str],
) -> Optional[str]:
    """
    Transliterate a track name to Latin script.

    Dispatch by detected script:
      cyrillic / georgian / armenian  → cyrillic_to_latin (transliterate pkg)
      arabic                          → arabic_to_latin (ALA-LC table)
      cjk (Chinese)                   → cjk_to_latin (pypinyin)
      japanese (mixed Kana+Kanji)     → japanese_to_latin (pykakasi)
      hiragana / katakana             → japanese_to_latin (pykakasi)
      hangul (Korean)                 → korean_to_latin (unidecode)
      devanagari / indic / thai       → universal_to_latin (unidecode)

    Returns None if no transliteration is needed (already Latin or
    script is None/unknown/mixed).
    """
    if not name or not script:
        return None

    if script == "arabic":
        result = arabic_to_latin(name)
    elif script in ("cyrillic", "georgian", "armenian"):
        result = cyrillic_to_latin(name, language)
    elif script == "cjk":
        result = cjk_to_latin(name)
    elif script in ("japanese", "hiragana", "katakana"):
        result = japanese_to_latin(name)
    elif script == "hangul":
        result = korean_to_latin(name)
    elif script in ("devanagari", "indic", "thai"):
        result = universal_to_latin(name)
    else:
        # hebrew, mixed, unknown — no transliteration
        return None

    # If transliteration produced nothing (library missing or failed),
    # return the original string so the field is never blank.
    return result if result and result.strip() else name
