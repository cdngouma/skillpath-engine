from __future__ import annotations

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = PROJECT_ROOT / "sql"


def execute_sql_file(con, path: str | Path) -> None:
    path = Path(path)
    sql = path.read_text(encoding="utf-8")
    con.execute(sql)


def create_tables(con) -> None:
    execute_sql_file(con, SQL_DIR / "schema.sql")


def create_views(con) -> None:
    execute_sql_file(con, SQL_DIR / "views.sql")


def initialize_database(con) -> None:
    create_tables(con)
    create_views(con)


def insert_jobs_raw(con, df: pd.DataFrame) -> None:
    con.register("jobs_df", df)
    try:
        con.execute("""
            INSERT INTO jobs_raw
                (source_job_id, created_at, search_term, redirect_url, fetched_at, source, raw_json)
            SELECT
                source_job_id,
                CAST(created_at AS TIMESTAMP),
                search_term,
                redirect_url,
                fetched_at,
                source,
                raw_json::JSON
            FROM jobs_df
            ON CONFLICT DO NOTHING;
        """)
    finally:
        con.unregister("jobs_df")


def insert_descriptions_raw(con, df: pd.DataFrame) -> None:
    con.register("desc_df", df)
    try:
        con.execute("""
            INSERT INTO descriptions_raw
                (source_job_id, redirect_url, scraped_at, source, html_description)
            SELECT
                source_job_id,
                redirect_url,
                scraped_at,
                source,
                html_description
            FROM desc_df
            WHERE source_job_id IS NOT NULL
            ON CONFLICT DO NOTHING;
        """)
    finally:
        con.unregister("desc_df")


def insert_jobs(con, df: pd.DataFrame) -> None:
    con.register("jobs_df", df)
    try:
        con.execute("""
            INSERT INTO jobs
                (
                    source_job_id,
                    source,
                    posted_at,
                    job_title,
                    role_title,
                    company,
                    location,
                    min_salary,
                    max_salary,
                    redirect_url
                )
            SELECT
                source_job_id,
                source,
                CAST(posted_at AS TIMESTAMP),
                job_title,
                role_title,
                company,
                location,
                min_salary,
                max_salary,
                redirect_url
            FROM jobs_df
            ON CONFLICT DO NOTHING;
        """)
    finally:
        con.unregister("jobs_df")


def insert_job_requirements(con, df: pd.DataFrame) -> None:
    con.register("job_req", df)
    try:
        con.execute("""
            INSERT INTO job_requirements
                (
                    source,
                    source_job_id,
                    technical_tools,
                    technical_concepts,
                    certifications,
                    min_years,
                    max_years,
                    extraction_status,
                    error
                )
            SELECT
                source,
                source_job_id,
                technical_tools,
                technical_concepts,
                certifications,
                min_years,
                max_years
            FROM job_req
            ON CONFLICT DO NOTHING;
        """)
    finally:
        con.unregister("job_req")


def get_jobs_missing_descriptions(con) -> pd.DataFrame:
    return con.execute("""
        SELECT DISTINCT
            j.source_job_id,
            j.source,
            j.redirect_url
        FROM jobs_raw j
        LEFT JOIN descriptions_raw d
          ON j.source = d.source
         AND j.source_job_id = d.source_job_id
        WHERE d.source_job_id IS NULL
          AND j.redirect_url IS NOT NULL;
    """).df()


def get_jobs_metadata(con) -> pd.DataFrame:
    return con.execute("""
        SELECT
            p.source_job_id AS source_job_id,
            p.source AS source,
            p.raw_json->>'title' AS job_title,
            p.raw_json->'company'->>'display_name' AS company,
            p.raw_json->'location'->>'display_name' AS location,
            CAST(p.raw_json->>'salary_min' AS DOUBLE) AS min_salary,
            CAST(p.raw_json->>'salary_max' AS DOUBLE) AS max_salary,
            p.created_at AS posted_at,
            p.redirect_url AS redirect_url
        FROM jobs_raw p
        JOIN descriptions_raw d
          ON p.source = d.source
         AND p.source_job_id = d.source_job_id;
    """).df()


def get_job_descriptions(con) -> pd.DataFrame:
    return con.execute("""
        SELECT
            j.source_job_id AS source_job_id,
            j.source AS source,
            j.job_title AS job_title,
            j.role_title AS role_title,
            d.html_description AS html_description
        FROM jobs j
        JOIN descriptions_raw d
          ON j.source = d.source
         AND j.source_job_id = d.source_job_id;
    """).df()


def count_rows(con, table_name: str) -> int:
    allowed_tables = {
        "jobs_raw",
        "descriptions_raw",
        "jobs",
        "job_requirements",
        "job_requirement_items",
    }

    if table_name not in allowed_tables:
        raise ValueError(f"Unsupported table name: {table_name}")

    return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]