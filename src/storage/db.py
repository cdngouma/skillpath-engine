import pandas as pd


def create_tables(con) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS jobs_raw (
            source VARCHAR NOT NULL,
            source_job_id VARCHAR NOT NULL,
            created_at TIMESTAMP,
            search_term VARCHAR NOT NULL,
            fetched_at TIMESTAMP NOT NULL,
            redirect_url VARCHAR,
            raw_json JSON NOT NULL,
            UNIQUE(source, source_job_id)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS descriptions_raw (
            source VARCHAR NOT NULL,
            source_job_id VARCHAR NOT NULL,
            redirect_url VARCHAR,
            scraped_at TIMESTAMP NOT NULL,
            html_description VARCHAR,
            UNIQUE(source, source_job_id)
        ); """)


def insert_jobs_raw(con, df: pd.DataFrame) -> None:
    con.register("jobs_df", df)
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
        ON CONFLICT DO NOTHING; """)
    con.unregister('jobs_df')


def insert_descriptions_raw(con, df: pd.DataFrame) -> None:
    con.register("desc_df", df)
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
        ON CONFLICT DO NOTHING; """)
    con.unregister('desc_df')


def get_jobs_missing_descriptions(con) -> pd.DataFrame:
    # Get list of jobs to scrape descriptions
    return con.execute("""
        SELECT DISTINCT j.source_job_id, j.source, j.redirect_url
        FROM jobs_raw j
        LEFT JOIN descriptions_raw d
          ON j.source_job_id = d.source_job_id
         AND j.source = d.source
        WHERE d.source_job_id IS NULL 
          AND j.redirect_url IS NOT NULL; 
        """).df()


def count_items(con, table_name: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]