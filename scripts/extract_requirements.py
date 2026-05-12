from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from src.config import config
from src.storage.db import (
    initialize_database,
    get_job_descriptions,
    insert_job_requirements,
)
from src.processing.section_extractor import extract_requirements as extract_requirement_section
from src.processing.requirement_extractor import extract_requirements as extract_structured_requirements, get_llm_client

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def infer_seniority(job_title: str, min_years: int | None = None) -> str | None:
    title = (job_title or "").lower()

    if any(term in title for term in ["intern", "internship", "student", "co-op", "new grad"]):
        return "junior"
    if any(term in title for term in ["junior", "jr.", "associate"]):
        return "junior"
    if any(term in title for term in ["senior", "sr.", "lead", "staff", "principal"]):
        return "senior"

    if min_years is not None:
        if min_years <= 2:
            return "junior"
        if min_years >= 5:
            return "senior"
        return "mid"

    return None


def build_requirements_dataframe(
    con: duckdb.DuckDBPyConnection,
    model: str,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    Build structured requirements dataframe from curated jobs and raw descriptions.
    """
    df = get_job_descriptions(con)

    if df.empty:
        return df

    if limit is not None:
        df = df.head(limit).copy()

    rows = []

    client = get_llm_client(model)

    for idx, row in df.iterrows():
        source = row["source"]
        source_job_id = row["source_job_id"]
        job_title = row["job_title"]
        html_description = row["html_description"]

        logger.info(f"[{idx + 1}/{len(df)}] Extracting requirements for {source}:{source_job_id}")

        requirement_text = extract_requirement_section(html_description)

        if not requirement_text:
            rows.append(
                {
                    "source": source,
                    "source_job_id": source_job_id,
                    "technical_tools": [],
                    "technical_concepts": [],
                    "certifications": [],
                    "min_years": None,
                    "max_years": None,
                    "seniority": infer_seniority(job_title),
                    "extraction_status": "skipped",
                    "error": "No requirement section extracted",
                }
            )
            continue

        result = extract_structured_requirements(
            description=requirement_text,
            client=client,
        )

        parsed = result.get("parsed_requirements") or {}

        min_years = parsed.get("min_years")
        max_years = parsed.get("max_years")

        rows.append(
            {
                "source": source,
                "source_job_id": source_job_id,
                "technical_tools": parsed.get("technical_tools", []),
                "technical_concepts": parsed.get("technical_concepts", []),
                "certifications": parsed.get("certifications", []),
                "min_years": min_years,
                "max_years": max_years,
                "seniority": infer_seniority(job_title, min_years=min_years),
                "extraction_status": result.get("extraction_status"),
                "error": result.get("error"),
            }
        )

    return pd.DataFrame(rows)


def main(
    db_path: Path = config.db_path,
    model: str = "ollama/gemma3:4b",
    limit: int | None = None,
    dry_run: bool = False,
) -> None:
    con = duckdb.connect(str(db_path))

    try:
        initialize_database(con)

        requirements_df = build_requirements_dataframe(
            con=con,
            model=model,
            limit=limit,
        )

        logger.info(f"Requirement records built: {len(requirements_df)}")

        if requirements_df.empty:
            logger.warning("No requirement records to insert.")
            return

        print("\nExtraction status counts:")
        print(requirements_df["extraction_status"].value_counts(dropna=False).to_string())

        print("\nSeniority distribution:")
        print(requirements_df["seniority"].value_counts(dropna=False).to_string())

        if dry_run:
            logger.info("Dry run enabled. No rows inserted.")
            return

        insert_job_requirements(con, requirements_df)
        logger.info("Inserted structured requirements into job_requirements table.")

    finally:
        con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract structured requirements from curated job descriptions."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=config.db_path,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="ollama/gemma3:4b",
        help="Ollama model name.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional number of jobs to process for testing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build requirements dataframe without inserting rows.",
    )

    args = parser.parse_args()

    main(
        db_path=args.db_path,
        model=args.model,
        limit=args.limit,
        dry_run=args.dry_run,
    )