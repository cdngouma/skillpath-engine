from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "configs"
DATA_DIR = PROJECT_ROOT / "data"
SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


@dataclass(frozen=True)
class Config:
    db_path: Path = DATA_DIR / "db.duckdb"
    schema_path: Path = SCHEMA_PATH

    country: str = "ca"
    results_per_page: int = 50
    max_days_old: int = 30
    max_pages: int = 10

    search_queries: dict[str, list[str]] = field(
        default_factory=lambda: load_yaml(
            CONFIG_DIR / "search_queries.yaml"
        )
    )

    excluded_terms: tuple[str, ...] = (
        "director",
        "head",
        "president",
        "vice president",
        "vp",
        "chief",
        "founder",
        "co-founder",
        "manager",
        "gestionnaire",
        "directeur",
    )


def load_config() -> Config:
    return Config(
        db_path=Path(
            os.getenv(
                "SKILLPATH_DB_PATH",
                str(DATA_DIR / "db.duckdb"),
            )
        )
    )
