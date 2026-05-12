from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from src.config import config
from src.storage.db import initialize_database, get_jobs_metadata, insert_jobs
from src.processing.role_mapper import map_titles_df

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_jobs_dataframe(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Build the curated jobs dataframe from raw job metadata.

    Steps:
    - read raw job metadata from DuckDB
    - normalize/map raw titles to canonical roles
    - filter out failed/ambiguous mappings
    - prepare rows for insertion into jobs table
    """
    df = get_jobs_metadata(con)

    if df.empty:
        return df

    mapped_df = map_titles_df(df, title_col="job_title")

    # Keep only successfully mapped jobs
    mapped_df = mapped_df[mapped_df["match_method"] != "_FAILED_"].copy()

    if mapped_df.empty:
        return mapped_df

    # Rename fields to match jobs table schema
    mapped_df = mapped_df.rename(
        columns={
            "matched_role": "role_title",
            "match_method": "role_match_method",
        }
    )

    required_cols = [
        "source",
        "source_job_id",
        "posted_at",
        "job_title",
        "role_title",
        "role_match_method",
        "company",
        "location",
        "min_salary",
        "max_salary",
        "redirect_url",
    ]

    return mapped_df[required_cols].copy()


def main(db_path: Path = config.db_path, dry_run: bool = False) -> None:
    con = duckdb.connect(str(db_path))

    try:
        initialize_database(con)

        jobs_df = build_jobs_dataframe(con)

        logger.info(f"Candidate jobs after role mapping: {len(jobs_df)}")

        if jobs_df.empty:
            logger.warning("No jobs to insert.")
            return

        print("\nRole distribution:")
        print(jobs_df["role_title"].value_counts().to_string())

        print("\nRole mapping method distribution:")
        print(jobs_df["role_match_method"].value_counts().to_string())

        if dry_run:
            logger.info("Dry run enabled. No rows inserted.")
            return

        insert_jobs(con, jobs_df)
        logger.info(f"Inserted curated jobs into jobs table.")

    finally:
        con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build curated jobs table from raw job postings."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and summarize jobs dataframe without inserting rows.",
    )

    args = parser.parse_args()

    main(
        db_path=config.db_path,
        dry_run=args.dry_run,
    )