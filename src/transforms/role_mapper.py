import re
import unicodedata
import logging
from functools import partial

import pandas as pd
import torch
import yaml
from sentence_transformers import SentenceTransformer, util

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Config / Regex
# ---------------------------------------------------------------------

LEADERSHIP_RE = re.compile(
    r"\b("
    r"director|directeur|head|vp|vice president|chief|president|"
    r"manager|gestionnaire"
    r")\b",
    re.I,
)

SENIORITY_RE = re.compile(
    r"\b("
    r"senior|sr\.?|junior|jr\.?|associate|staff|principal|lead|"
    r"intermediate|entry[-\s]?level|intern|stagiaire|student"
    r")\b",
    re.I,
)

CONTEXT_NOISE_RE = re.compile(
    r"\b("
    r"remote|hybrid|on[-\s]?site|contract|permanent|temporary|full[-\s]?time|"
    r"part[-\s]?time|montreal|montr[eé]al|toronto|ottawa|vancouver|canada|usa|us"
    r")\b",
    re.I,
)

STOPWORDS_RE = re.compile(
    r"\b("
    r"and|or|for|with|in|at|of|the|on|to|et|ou|pour|avec|dans|sur|"
    r"de|la|le|les|des|du|d[eu]|&"
    r")\b",
    re.I,
)

SEGMENT_SPLIT_RE = re.compile(r"\s*(?:,|\||:|;| / | -|- |\(|\))\s*")

# Translation / normalization rules applied before segmentation logic
NORMALIZATION_RULES = [
    # French -> English
    (re.compile(r"\bd[eé]veloppeur(?:\.?euse)?\b", re.I), "developer"),
    (re.compile(r"\bing[eé]nieur(?:\.?e)?\b", re.I), "engineer"),
    (re.compile(r"\bscientifique\b", re.I), "scientist"),
    (re.compile(r"\barchitecte\b", re.I), "architect"),
    (re.compile(r"\banalyste\b", re.I), "analyst"),
    (re.compile(r"\br[eé]seau\b", re.I), "network"),
    (re.compile(r"\bs[eé]curit[eé]\b", re.I), "security"),
    # Term normalization
    (re.compile(r"\bfull[-\s]?stack\b", re.I), "full-stack"),
    (re.compile(r"\bfront[-\s]?end\b", re.I), "frontend"),
    (re.compile(r"\bback[-\s]?end\b", re.I), "backend"),
    (re.compile(r"\bnodejs\b", re.I), "node.js"),
    (re.compile(r"\bdotnet\b", re.I), ".net"),
    (re.compile(r"\bui\s*/\s*ux\b", re.I), "ux ui"),
    (re.compile(r"\bux\s*/\s*ui\b", re.I), "ux ui"),
    (re.compile(r"\bdeep learning\b", re.I), "machine learning"),
    (re.compile(r"\bcyber security\b", re.I), "cybersecurity"),
    # Morphology normalization
    (re.compile(r"\bengineering\b", re.I), "engineer"),
    (re.compile(r"\bdevelopment\b", re.I), "developer"),
    (re.compile(r"\bprogramming\b", re.I), "programmer"),
]

# Protected tokens / concepts to preserve from later title segments
PROTECTED_TERMS = {
    "ai",
    "ml",
    "ai/ml",
    "machine learning",
    "genai",
    "generative ai",
    "llm",
    "data",
    "analytics",
    "full-stack",
    "frontend",
    "backend",
    "security",
    "cybersecurity",
    "cloud",
    "platform",
    "devops",
    "site reliability",
    "sre",
    "database",
    "etl",
    "bi",
    "ux",
    "ui",
    "web",
    "firmware",
    "embedded",
    "network",
    "erp",
    "salesforce",
    "software",
    "developer"
}

PROTECTED_REGEXES = [
    (term, re.compile(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", re.I))
    for term in sorted(PROTECTED_TERMS, key=len, reverse=True)
]

# Optional synonym collapsing before semantic match
CANONICAL_TERM_RULES = [
    (re.compile(r"\b(machine learning|mlops|ml)\b", re.I), "ml"),
    (re.compile(r"\b(generative ai|genai|llm|artificial intelligence|ai)\b", re.I), "ai"),
    (re.compile(r"\b(site reliability|sre)\b", re.I), "site reliability"),
]

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(c)
    )


def _collapse_spaces(text: str) -> str:
    return " ".join(text.split()).strip()


def _normalize_text(text: str) -> str:
    if not text:
        return ""

    text = str(text).lower()
    text = _strip_accents(text)
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"[\t\r\n]+", " ", text)
    text = re.sub(r"[•·]", " ", text)

    for rx, repl in NORMALIZATION_RULES:
        text = rx.sub(repl, text)

    return _collapse_spaces(text)


def _apply_canonical_term_rules(text: str) -> str:
    for rx, repl in CANONICAL_TERM_RULES:
        text = rx.sub(repl, text)
    return _collapse_spaces(text)


def _extract_protected_terms(text: str) -> list[str]:
    if not text:
        return []

    found = []
    used_spans = []

    for term, rx in PROTECTED_REGEXES:
        for match in rx.finditer(text):
            span = match.span()
            if any(not (span[1] <= s[0] or span[0] >= s[1]) for s in used_spans):
                continue
            used_spans.append(span)
            found.append(term)

    out = []
    seen = set()
    for term in found:
        if term not in seen:
            seen.add(term)
            out.append(term)
    return out


def _preprocess_title(raw_title: str) -> tuple[str, bool]:
    """
    Returns:
        cleaned_title, is_excluded
    """
    if not raw_title:
        return "", False

    text = _normalize_text(raw_title)

    if LEADERSHIP_RE.search(text):
        return text, True

    text = SENIORITY_RE.sub(" ", text)
    text = CONTEXT_NOISE_RE.sub(" ", text)

    # keep common technical punctuation before stopword stripping
    text = re.sub(r"[{}\[\]]", " ", text)
    text = STOPWORDS_RE.sub(" ", text)

    # keep letters, numbers, slash, dot, plus, hash, hyphen, separators for segmentation
    text = re.sub(r"[^a-z0-9/+.&#,\-|:;() ]+", " ", text)
    text = _collapse_spaces(text)

    return text, False


def _reduce_title_segments(clean_title: str) -> str:
    """
    Keep first segment always.
    For later segments, keep only protected terms if present.
    """
    if not clean_title:
        return ""

    parts = [p.strip() for p in SEGMENT_SPLIT_RE.split(clean_title) if p and p.strip()]
    if not parts:
        return ""

    base = parts[0]
    extras = []

    for part in parts[1:]:
        protected = _extract_protected_terms(part)
        extras.extend(protected)

    extras = list(dict.fromkeys(extras))  # preserve order, dedupe
    reduced = base if not extras else f"{base} {' '.join(extras)}"
    reduced = _apply_canonical_term_rules(reduced)
    return _collapse_spaces(reduced)


# ---------------------------------------------------------------------
# Taxonomy loading
# ---------------------------------------------------------------------


def _load_role_taxonomy(yaml_path: str):
    """
    Expected YAML shape:

    role_mapping:
      "21211":
        "Data Scientist":
          - "Data Scientist"
          - "Applied Scientist"
        "AI/ML Developer":
          - "AI Engineer"
          - "ML Engineer"
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    role_mapping = cfg["role_mapping"]

    variant_to_role: dict[str, str] = {}
    role_to_noc: dict[str, str] = {}
    variants: list[str] = []

    for noc_code, roles in role_mapping.items():
        for role_name, role_variants in roles.items():
            role_to_noc[role_name] = noc_code

            # also allow canonical role name itself as a match target
            canonical_role_key = role_name.lower()
            variant_to_role[canonical_role_key] = role_name
            variants.append(role_name)

            for variant in role_variants:
                key = variant.lower()
                variant_to_role[key] = role_name
                variants.append(variant)

    # dedupe while preserving first occurrence
    variants = list(dict.fromkeys(variants))

    return variant_to_role, role_to_noc, variants


# ---------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------


def _exact_match(clean_title: str, variant_to_role: dict[str, str]):
    text = clean_title.lower().strip()
    
    for variant in variant_to_role.keys():
        pattern = r"\b" + re.escape(variant.lower().strip()) + r"\b"
        if re.search(pattern, text):
            role = variant_to_role[variant]
            return {
                "matched_variant": variant,
                "matched_role": role,
                "match_method": "Exact",
                "confidence_score": 100.0
            }
    return None


def _semantic_match(
    clean_title: str,
    model,
    variant_texts: list[str],
    variant_embeddings,
    variant_to_role: dict[str, str],
):
    if not clean_title:
        return {
            "matched_variant": None,
            "matched_role": None,
            "match_method": "None",
            "confidence_score": 0.0,
        }

    title_embedding = model.encode(
        clean_title,
        convert_to_tensor=True,
        show_progress_bar=False,
    )
    cosine_scores = util.cos_sim(title_embedding, variant_embeddings)[0]
    best_idx = int(torch.argmax(cosine_scores).item())
    score = float(cosine_scores[best_idx].item())
    matched_variant = variant_texts[best_idx]
    matched_role = variant_to_role.get(matched_variant.lower())

    return {
        "matched_variant": matched_variant,
        "matched_role": matched_role,
        "match_method": "Semantic",
        "confidence_score": round(score * 100, 2),
    }


def _map_row(
    row,
    model,
    variant_texts,
    variant_embeddings,
    variant_to_role,
    role_to_noc,
    min_confidence=None,
):
    original_title = str(row["title"]) if pd.notna(row["title"]) else ""

    preprocessed_title, is_excluded = _preprocess_title(original_title)
    clean_title = _reduce_title_segments(preprocessed_title)

    if is_excluded:
        return pd.Series(
            {
                "clean_title": clean_title,
                "matched_variant": None,
                "matched_role": None,
                "matched_noc": None,
                "confidence_score": 0.0,
                "match_method": "ExcludedLeadership",
            }
        )

    exact = _exact_match(clean_title, variant_to_role)
    if exact is not None:
        matched_role = exact["matched_role"]
        matched_noc = role_to_noc.get(matched_role)
        return pd.Series(
            {
                "clean_title": clean_title,
                "matched_variant": exact["matched_variant"],
                "matched_role": matched_role,
                "matched_noc": matched_noc,
                "confidence_score": exact["confidence_score"],
                "match_method": exact["match_method"],
            }
        )

    semantic = _semantic_match(
        clean_title=clean_title,
        model=model,
        variant_texts=variant_texts,
        variant_embeddings=variant_embeddings,
        variant_to_role=variant_to_role,
    )

    matched_role = semantic["matched_role"]
    matched_noc = role_to_noc.get(matched_role)

    if min_confidence is not None and semantic["confidence_score"] < min_confidence:
        matched_role = None
        matched_noc = None

    return pd.Series(
        {
            "clean_title": clean_title,
            "matched_variant": semantic["matched_variant"],
            "matched_role": matched_role,
            "matched_noc": matched_noc,
            "confidence_score": semantic["confidence_score"],
            "match_method": semantic["match_method"],
        }
    )


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


def map_roles(
    df: pd.DataFrame,
    yaml_path: str,
    full_report: bool = False,
    min_confidence: float = 70.0,
    model_name: str = "all-MiniLM-L6-v2",
) -> pd.DataFrame:
    """
    Map raw job titles to:
      - matched role variant
      - matched canonical role
      - matched NOC

    Required input columns:
      - title

    Optional input columns:
      - job_hash

    Returns:
      full_report=True:
        job_hash? | title | clean_title | matched_variant |
        matched_role | matched_noc | confidence_score | match_method

      full_report=False:
        job_hash? | title | matched_role | matched_noc | confidence_score
    """
    if "title" not in df.columns:
        raise ValueError("Input DataFrame must contain a 'title' column.")

    variant_to_role, role_to_noc, variant_texts = _load_role_taxonomy(yaml_path)

    logger.info("Initializing semantic model...")
    model = SentenceTransformer(model_name)

    logger.info("Encoding role variants...")
    variant_embeddings = model.encode(
        variant_texts,
        convert_to_tensor=True,
        show_progress_bar=False,
    )

    logger.info(f"Mapping {len(df)} job titles...")
    mapper = partial(
        _map_row,
        model=model,
        variant_texts=variant_texts,
        variant_embeddings=variant_embeddings,
        variant_to_role=variant_to_role,
        role_to_noc=role_to_noc,
        min_confidence=min_confidence,
    )

    mapped = df.apply(mapper, axis=1)

    base_cols = df.columns

    if full_report:
        return pd.concat(
            [
                df[base_cols].reset_index(drop=True),
                mapped.reset_index(drop=True),
            ],
            axis=1,
        )

    return pd.concat(
        [
            df[base_cols].reset_index(drop=True),
            mapped[["matched_role", "matched_noc", "confidence_score"]].reset_index(drop=True),
        ],
        axis=1,
    )