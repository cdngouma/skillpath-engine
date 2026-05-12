from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.config import config


# ----------------------------
# Regex patterns
# ----------------------------

SENIORITY_PATTERN = re.compile(
    r"\b(senior|sr\.?|staff|intermediate|associate|junior|jr\.?|internship|intern|new grad(uate)?|rotational program)( |,|\)|$)?\b",
    re.IGNORECASE,
)

AI_PATTERN = re.compile(r"(^|\s+|\(|/)(ai|agentic|gen(erative|\s+)ai)(\s+|$)", re.IGNORECASE)
ML_PATTERN = re.compile(
    r"(^|\s+|\(|/)(ml|machine learning|nlp|computer vision|deep learning)(\s+|$)",
    re.IGNORECASE,
)
DATA_ENG_PATTERN = re.compile(
    r"(^|\s+|\(|/)(databricks|snowflake|data architect|etl)(\s+|$)",
    re.IGNORECASE,
)


# ----------------------------
# Data containers
# ----------------------------

@dataclass(frozen=True)
class MatchResult:
    cleaned_title: str
    matched_role: str | None
    match_method: str | None


# ----------------------------
# Taxonomy compilation
# ----------------------------

def compile_taxonomy(taxonomy: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compiled = []
    for role in taxonomy:
        aliases = role.get("aliases", [])
        pattern_str = r"(?i)\b(" + "|".join(re.escape(alias) for alias in aliases) + r")\b"
        compiled.append(
            {
                "name": role["name"],
                "aliases": aliases,
                "pattern": re.compile(pattern_str),
            }
        )
    return compiled


COMPILED_TAXONOMY = compile_taxonomy(config.role_taxonomy)


# ----------------------------
# Cleaning functions
# ----------------------------

def normalize_french_terms(title: str) -> str:
    clean_title = title.lower()
    clean_title = re.sub(r"[éè]", "e", clean_title)
    clean_title = re.sub(r" +ia +", " ai ", clean_title)
    clean_title = re.sub(r"(·|\.)(euse|se|e)|\((\-?euse|se|e)\)", "", clean_title)

    clean_title = re.sub(r"(ingenieur|ingenierie) logiciel", "software engineer", clean_title)
    clean_title = re.sub(r"(ingenieur|ingenierie|architecte)( de)? donnees", "data engineer", clean_title)
    clean_title = re.sub(r"(developpeur|developpement)( de)? logiciel", "software developer", clean_title)
    clean_title = re.sub(r"science des? donnees|scientifique des? donnees", "data scientist", clean_title)
    clean_title = re.sub(r"(des? )?donnees", "data", clean_title)
    clean_title = re.sub(r"developpeur|(^|\s+)dev(\s+|$)", "developer", clean_title)
    clean_title = re.sub(r"ingenieur", "engineer", clean_title)
    clean_title = re.sub(r"(stagaire|stage)( +en)?", "internship", clean_title)

    return clean_title.strip()


def remove_seniority(title: str) -> str:
    clean_title = SENIORITY_PATTERN.sub("", title)
    clean_title = re.sub(r"^[^a-zA-Z]+", "", clean_title)
    clean_title = re.sub(r"\s+", " ", clean_title)
    return clean_title.strip()


def normalize_common_terms(title: str) -> str:
    clean_title = title

    clean_title = re.sub(r"prompt engineer", "ai developer", clean_title)
    clean_title = re.sub(r"software development|software dev ", "software developer", clean_title)
    clean_title = re.sub(r"swe ", "software engineer ", clean_title)
    clean_title = re.sub(r"ai/ml|artificial intelligence", "ai", clean_title)
    clean_title = re.sub(
        r"(python|c#(/\.net)?|net|javascript|typescript|(c/)?c\+\+|react|node\.?js?|java|c(-| )sharp) developer",
        "software developer",
        clean_title,
    )
    clean_title = re.sub(r"engineering|engineers", "engineer", clean_title)
    clean_title = re.sub(r"development|developers", "developer", clean_title)

    clean_title = re.sub(r"\s+", " ", clean_title)
    return clean_title.strip()


def clean_job_title(title: str | None) -> str:
    if not title:
        return ""

    cleaned = normalize_french_terms(title)
    cleaned = remove_seniority(cleaned)
    cleaned = normalize_common_terms(cleaned)
    return cleaned.strip()


# ----------------------------
# Matching functions
# ----------------------------

def match_aliases_exact(title: str) -> tuple[str | None, str | None]:
    if not title:
        return None, None

    for role in COMPILED_TAXONOMY:
        if role["pattern"].search(title):
            return role["name"], "exact_match"

    return None, None


def match_aliases_flexible(title: str) -> tuple[str | None, str | None]:
    if not title:
        return None, None

    title_words = set(re.findall(r"\w+", title.lower()))

    for role in COMPILED_TAXONOMY:
        for alias in role["aliases"]:
            alias_words = set(re.findall(r"\w+", alias.lower()))
            if alias_words and alias_words.issubset(title_words):
                return role["name"], "flexible_match"

    return None, None


def match_keywords(title: str) -> tuple[str | None, str | None]:
    if not title:
        return None, None

    if AI_PATTERN.search(title):
        return "ai developer", "keyword_match"
    if ML_PATTERN.search(title):
        return "ml engineer", "keyword_match"
    if DATA_ENG_PATTERN.search(title):
        return "data engineer", "keyword_match"

    return None, None


# ----------------------------
# Public mapping API
# ----------------------------

def map_title(title: str | None) -> MatchResult:
    cleaned_title = clean_job_title(title)

    for matcher in (match_aliases_exact, match_aliases_flexible, match_keywords):
        matched_role, match_method = matcher(cleaned_title)
        if matched_role is not None:
            return MatchResult(
                cleaned_title=cleaned_title,
                matched_role=matched_role,
                match_method=match_method,
            )

    return MatchResult(
        cleaned_title=cleaned_title,
        matched_role=None,
        match_method="_FAILED_",
    )


def map_titles_df(df: pd.DataFrame, title_col: str = "title") -> pd.DataFrame:
    out = df.copy()

    results = out[title_col].apply(map_title)

    out["cleaned_title"] = results.apply(lambda x: x.cleaned_title)
    out["matched_role"] = results.apply(lambda x: x.matched_role)
    out["match_method"] = results.apply(lambda x: x.match_method)

    return out


def filter_mapped_titles(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["match_method"] != "_FAILED_"].copy()