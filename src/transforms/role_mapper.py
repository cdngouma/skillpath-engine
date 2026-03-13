import re
import unicodedata
from functools import partial
import pandas as pd
import torch
from sentence_transformers import SentenceTransformer, util
from noc_mapping import get_noc_lookup
import logging

logger = logging.getLogger(__name__)

# ---- Utilities functions ----

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )

def _normalize_separators(s: str) -> str:
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"[|•·]", " ", s)
    s = re.sub(r"[\t\r\n]+", " ", s)
    return s

def _collapse_spaces(s: str) -> str:
    return " ".join(s.split()).strip()

# ---- Config & Regex Compilation ----

# Seniority/Level noise
SENIORITY_RE = re.compile(r"\b(staff|senior|sr\.?|jr\.?|junior|intermediate|lead|principal|internship|intern)\b", re.I)

# Geographical/Format noise
CONTEXT_NOISE_RE = re.compile(
    r"\b(hybrid|remote|on[-\s]?site|canada|montreal|montréal|toronto|ottawa|vancouver|us|usa)\b",
    re.I,
)

# Language stop words
STOPWORDS_RE = re.compile(r"\b(and|or|for|with|in|at|of|the|on|et|pour|dans|avec|sur|de|la|le|des|du)\b", re.I)

# Corporate role fluff
ROLE_FLUFF_RE = re.compile(r"\b(associate|consultant|specialist|specialiste|expert|solutions?)\b", re.I)

NORMALIZATION_RULES = [
    (re.compile(r"\b(aws|azure|gcp|google cloud|infrastructure)\b", re.I), "cloud"),
    (re.compile(r"\bplatform\s+engineer\b", re.I), "cloud engineer"),
    (re.compile(r"\bscientifique\b", re.I), "scientist"),
    (re.compile(r"\bdeveloppeur(\.se)?\s+logiciel\b", re.I), "software developer"),
    (re.compile(r"\bingenieur(\·?\.?e)?\s+logiciel\b", re.I), "software engineer"),
    (re.compile(r"\bdeveloppement\b", re.I), "developer"),
    (re.compile(r"\bfull[-\s]?stack\b", re.I), "full stack"),
    (re.compile(r"\bfront[-\s]?end\b", re.I), "frontend"),
    (re.compile(r"\bback[-\s]?end\b", re.I), "backend"),
    (re.compile(r"\bui\s*/\s*ux\b", re.I), "ui/ux"),
    (re.compile(r"\bengineer(ing)?\b", re.I), "engineer"),
    (re.compile(r"\bdevelop(ment|er)?\b", re.I), "developer"),
    (re.compile(r"\betl\b", re.I), "data"),
]

PROTECTED_PHRASES = [
    "machine learning", "genai", "ml", "ai", "data", "cloud", "devops", 
    "sre", "cybersecurity", "salesforce", "sap", "aws", "azure", "gcp", 
    "ui/ux", "frontend", "backend", "full stack", "bi"
]

PROTECTED_CANONICAL = {"machine learning": "ml"}

def _compile_protected_regexes(protected_phrases):
    protected_phrases = sorted(set(p.lower() for p in protected_phrases), key=len, reverse=True)
    compiled = []
    for p in protected_phrases:
        patt = r"(?<![a-z0-9])" + re.escape(p) + r"(?![a-z0-9])"
        compiled.append((p, re.compile(patt, re.I)))
    return compiled

PROTECTED_REGEXES = _compile_protected_regexes(PROTECTED_PHRASES)
SEGMENT_SPLIT_RE = re.compile(r"\s*(?:,| - |–|\||:|;)\s*", re.I)

def _smart_clean_title(raw_text):
    if not raw_text: return ""
    text = str(raw_text).lower()
    text = _strip_accents(text)
    text = _normalize_separators(text)

    for rx, repl in NORMALIZATION_RULES:
        text = rx.sub(repl, text)

    text = SENIORITY_RE.sub(" ", text)
    text = CONTEXT_NOISE_RE.sub(" ", text)
    text = ROLE_FLUFF_RE.sub(" ", text)
    text = STOPWORDS_RE.sub(" ", text)
    text = text.replace("(", " ").replace(")", " ")
    text = re.sub(r"[^a-z/,\-|:; ]+", " ", text)

    return _collapse_spaces(text)

def _extract_protected_terms(text):
    if not text: return []
    found, used_spans = [], []
    for phrase, rx in PROTECTED_REGEXES:
        for m in rx.finditer(text):
            span = m.span()
            if any(not (span[1] <= s[0] or span[0] >= s[1]) for s in used_spans):
                continue
            used_spans.append(span)
            found.append(PROTECTED_CANONICAL.get(phrase, phrase))
    
    seen, out = set(), []
    for t in found:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

def _find_anchor(clean_title, unique_terms):
    if not clean_title: return None
    t = clean_title.lower()
    for term in sorted(unique_terms, key=len, reverse=True):
        patt = r"\b" + re.escape(term.lower()) + r"\b"
        if re.search(patt, t):
            return term.lower()
    return None

def _build_semantic_title(clean_title, anchor):
    if not clean_title: return ""
    if anchor: return anchor  # RULE: Anchor exists? Use it alone.

    parts = [p.strip() for p in SEGMENT_SPLIT_RE.split(clean_title) if p and p.strip()]
    if not parts: return ""

    base = parts[0]
    modifiers = []
    for p in parts[1:]:
        modifiers.extend(_extract_protected_terms(p))

    return _collapse_spaces(base + " " + " ".join(modifiers))

def _get_semantic_match(row, model, unique_terms, term_embeddings, noc_lookup):
    original_title = str(row["job_title"])
    clean_title = _smart_clean_title(original_title)

    anchor = _find_anchor(clean_title, unique_terms)
    synthetic_title = _build_semantic_title(clean_title, anchor)

    if anchor and anchor in unique_terms:
        best_term = anchor
        max_score = 1.0
        method = "Anchor"
    else:
        title_embedding = model.encode(synthetic_title, convert_to_tensor=True, show_progress_bar=False)
        cosine_scores = util.cos_sim(title_embedding, term_embeddings)[0]
        best_idx = int(torch.argmax(cosine_scores).item())
        max_score = float(cosine_scores[best_idx].item())
        best_term = unique_terms[best_idx]
        method = "Semantic"

    # We return everything here, but map_roles will decide what to keep
    return pd.Series({
        "clean_title": clean_title,
        "matched_label": best_term,
        "confidence_score": round(max_score * 100, 2),
        "matched_noc": noc_lookup.get(best_term),
        "match_method": method
    })

def map_roles(df, yaml_path, full_report=False):
    """
    Main entry point for NOC classification.
    If full_report=True, returns all diagnostic columns.
    If full_report=False, returns only assigned_noc and confidence_score.
    """
    noc_lookup = get_noc_lookup(yaml_path)
    
    logger.info("Initializing Semantic Model...")
    model = SentenceTransformer("all-MiniLM-L6-v2")

    unique_terms = list(noc_lookup.keys())
    term_embeddings = model.encode(unique_terms, convert_to_tensor=True, show_progress_bar=False)

    match_func = partial(
        _get_semantic_match,
        model=model,
        unique_terms=unique_terms,
        term_embeddings=term_embeddings,
        noc_lookup=noc_lookup,
    )

    logger.info(f"Mapping {len(df)} roles...")
    results = df.apply(match_func, axis=1)

    if full_report:
        return pd.concat([df, results], axis=1)
    
    # Minimalist Silver Layer output
    return pd.concat([df, results[["matched_noc", "confidence_score"]]], axis=1)
