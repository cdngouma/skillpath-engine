from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SRC_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
ROLE_TAXONOMY_PATH = SRC_DIR / "role_taxonomy.json"

EXCLUDED_TERMS = (
    "director",
    "head",
    "president",
    "vice",
    "vp",
    "chief",
    "founder",
    "co-founder",
    "manager",
    "gestionnaire",
    "directeur",
)

@dataclass(frozen=True)
class Configs:
    db_path: Path
    role_taxonomy: list[dict]
    excluded_terms: str
    results_per_page: int
    max_days_old: int
    max_pages: int

def _load_role_taxonomy(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

config = Configs(
    db_path=DATA_DIR / "warehouse.duckdb",
    role_taxonomy=_load_role_taxonomy(ROLE_TAXONOMY_PATH),
    excluded_terms=" ".join(EXCLUDED_TERMS),
    results_per_page=50,
    max_days_old=30,
    max_pages=10
)