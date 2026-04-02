import os
import re
import argparse
import logging
import duckdb
from dotenv import load_dotenv
from init_warehouse import init_silver
from transforms.silver_transform import (
    transform_census_income,
    transform_census_labour,
    transform_wages_trends,
    transform_labour_trends,
    transform_graduates,
    transform_job_roles,
    transform_job_skills,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

DB_PATH = "../data/warehouse.duckdb"

MAPPING_THRESHOLD = 74

SILVER_TARGETS = {
    # ---------------------------
    # StatCan targets
    # ---------------------------
    "census_income": {
        "table_name": "sc_census_income",
        "fnc": transform_census_income,
        "columns": [
            "education_level",
            "occupation",
            "median_income",
            "noc_code",
            "ingested_at",
            "source",
        ],
    },
    "census_labour": {
        "table_name": "sc_census_labour",
        "fnc": transform_census_labour,
        "columns": [
            "education_level",
            "occupation",
            "employed",
            "unemployed",
            "labour_force",
            "noc_code",
            "ingested_at",
            "source",
        ],
    },
    "wages_trends": {
        "table_name": "sc_wages_trends",
        "fnc": transform_wages_trends,
        "columns": [
            "occupation",
            "date",
            "weekly_wages",
            "noc_code",
            "ingested_at",
            "source",
        ],
    },
    "labour_trends": {
        "table_name": "sc_labour_trends",
        "fnc": transform_labour_trends,
        "columns": [
            "occupation",
            "date",
            "labour_force",
            "unemployment_rate",
            "noc_code",
            "ingested_at",
            "source",
        ],
    },
    "graduates": {
        "table_name": "sc_graduates_trends",
        "fnc": transform_graduates,
        "columns": [
            "education_level",
            "field_of_study",
            "date",
            "graduates",
            "noc_code",
            "ingested_at",
            "source",
        ],
    },

    # ---------------------------
    # Job targets
    # ---------------------------
    "job_roles": {
        "table_name": "job_postings",
        "fnc": transform_job_roles,
        "columns": [
            "job_hash",
            "title",
            "canonical_role",
            "noc_code",
            "confidence_score",
            "source",
            "ingested_at",
        ],
    },
    "job_skills": {
        "table_name": "job_skills",
        "fnc": transform_job_skills,
        "columns": None,  # handled inside transform
    },
}

VALID_TARGETS = list(SILVER_TARGETS.keys()) + ['all', 'statcan', 'jobs']


def _write_table(con, df, table_name, columns, mode="build"):
    if mode == "build":
        if table_name == SILVER_TARGETS["job_roles"].get('table_name'):
            con.execute(f"DELETE FROM silver.{SILVER_TARGETS['job_skills'].get('table_name')}")
        con.execute(f"DELETE FROM silver.{table_name}")
    elif mode != "update":
        raise ValueError(f"Unsupported mode: {mode}")
    
    if df is None or df.empty:
        logger.warning(f"Transform for silver.{table_name} returned no rows. Skipping.")
        return 0

    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for silver.{table_name}: {missing}")

    duplicated = df.duplicated().sum()
    if duplicated > 0:
        df = df.drop_duplicates()
        logger.warning(f"Removed duplicated records. Found ({duplicated})")
    
    con.register("df", df)
    col_list = ", ".join(columns)
    con.execute(f"""
        INSERT INTO silver.{table_name} ({col_list})
        SELECT {col_list}
        FROM df
        ON CONFLICT DO NOTHING
    """)
    con.unregister("df")
    return len(df)


def _run_target(con, target, mode):
    cfg = SILVER_TARGETS[target]
    table_name = cfg["table_name"]
    transform_fn = cfg["fnc"]
    columns = cfg["columns"]
    
    logger.info(f"Building target: {target} -> silver.{table_name}")

    if target == "job_skills":
        transform_fn(mode=mode)
        return

    if target == "job_roles":
        df = transform_fn(threshold=MAPPING_THRESHOLD)
        _write_table(con, df, table_name, columns, mode=mode)
        return

    df = transform_fn()
    _write_table(con, df, table_name, columns, mode=mode)


def build_statcan(con, mode):
    for target in ["census_income", "census_labour", "wages_trends", "labour_trends", "graduates"]:
        _run_target(con, target, mode)


def build_jobs(con, mode):
    for target in ["job_roles", "job_skills"]:
        _run_target(con, target, mode)


def main(mode="build", target="all"):
    if not target in VALID_TARGETS:
        logger.error(
            "Invalid target. Use one of: all, statcan, jobs, "
            "census_income, census_labour, wage_trends, labour_trends, "
            "graduates, job_roles, job_skills"
        )
        return

    load_dotenv()
    
    init_silver(db_path=DB_PATH)

    logger.info("Building Silver layer...")

    with duckdb.connect(DB_PATH) as con:
        if target == "all":
            build_statcan(con, mode)
            build_jobs(con, mode)
        elif target == "statcan":
            build_statcan(con, mode)
        elif target == "jobs":
            build_jobs(con, mode)
        else:
            _run_target(con, target, mode)

        logger.info("Silver build complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SkillPath-Engine Silver Build Pipeline")
    parser.add_argument(
        "--target",
        type=str,
        default="all",
        help=(
            "Target to build: all, statcan, jobs, census_income, "
            "census_labour, wages_trends, labour_trends, "
            "graduates, job_roles, job_skills"
        ),
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Run in update mode (append) instead of build mode (replace).",
    )

    args = parser.parse_args()
    run_mode = "update" if args.update else "build"
    main(mode=run_mode, target=args.target)
