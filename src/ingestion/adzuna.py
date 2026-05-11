from __future__ import annotations

import json
import datetime as dt
import logging
import os
import time
from typing import Any

import duckdb
import pandas as pd
import requests
from dotenv import load_dotenv

from src.ingestion.scrape import PlaywrightScraper
from src.config import config
import src.storage.db as db

from tqdm import tqdm

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
    

def fetch_jobs(
    what: str, 
    results_per_page: int=50, 
    max_days_old: int=90, 
    max_pages: int=10
) -> list[dict[str, Any]]:
    
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    country = "ca"

    if not app_id or not app_key:
        raise ValueError("Missing ADZUNA_APP_ID or ADZUNA_APP_KEY in environment.")

    all_jobs = []

    for page in range(1, max_pages + 1):
        try:
            url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
            params = {
                "app_id": app_id,
                "app_key": app_key,
                "results_per_page": results_per_page,
                "what": what,
                "what_exclude": config.excluded_terms,
                "category": "it-jobs",
                "salary_include_unknown": 1,
                "max_days_old": max_days_old,
                "full_time": 1,
                "content-type": "application/json",
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])
            all_jobs.extend(results)

            if len(results) < results_per_page:
                break

            time.sleep(0.7)
        except:
            time.sleep(1.0)
            continue

    return all_jobs


def process_jobs(jobs: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    processed = []

    now = dt.datetime.now(dt.timezone.utc)
    
    for job in jobs:
        processed.append({
            "source_job_id": job.get('id'),
            "created_at": job.get('created'),
            "redirect_url": job.get('redirect_url'),
            "search_term": query,
            "fetched_at": dt.datetime.now(),
            "source": "Adzuna",
            "raw_json": job
        })
    
    return processed


def insert_jobs(con: duckdb.DuckDBPyConnection, jobs: list[dict[str, Any]]) -> int:
    if not jobs:
        return 0
    df = pd.DataFrame(jobs)
    df['raw_json'] = df['raw_json'].apply(json.dumps)
    db.insert_jobs_raw(con, df)


def insert_descriptions(con: duckdb.DuckDBPyConnection, jobs: list[dict[str, Any]]) -> None:
    if not jobs:
        return 0
    df = pd.DataFrame(jobs)
    db.insert_descriptions_raw(con, df)


def ingest_jobs() -> None:
    # Pre-calculate tasks for a single accurate progress bar
    tasks = [
        (role, query) 
        for role in config.role_taxonomy 
        for query in role["search_term"]
    ]
    
    with duckdb.connect(config.db_path) as con:
        db.create_tables(con)

        logger.info("🔄 Fetching job postings from Adzuna...")

        initial_count = db.count_items(con, table_name="jobs_raw")

        for (role, query) in tqdm(tasks, desc="Ingesting jobs", unit="query"):
            raw_jobs = fetch_jobs(
                what=query, 
                results_per_page=config.results_per_page,
                max_days_old=config.max_days_old,
                max_pages=config.max_pages
            )

            processed_jobs = process_jobs(raw_jobs, query)
            insert_jobs(con, processed_jobs)

        final_count = db.count_items(con, table_name="jobs_raw")
        inserted = final_count - initial_count

        logger.info(f"✅ Ingestion completed. Inserted ({inserted}) raw job rows.")


def ingest_job_descriptions() -> None:
    with duckdb.connect(config.db_path) as con:
        db.create_tables(con)

        # Get list of jobs to scrape descriptions
        jobs_to_scrape = db.get_jobs_missing_descriptions(con)

        failed = 0

        rows = []
        CHUNK_SIZE = 50

        logger.info("🔄 Scraping job descriptions...")

        initial_count = db.count_items(con, table_name="descriptions_raw")

        with PlaywrightScraper() as scraper:
            for row in tqdm(jobs_to_scrape.itertuples(index=False), total=len(jobs_to_scrape), desc="Processing Jobs"):
                try:
                    description_html = scraper.fetch(row.redirect_url) if row.redirect_url else None
                    if not description_html:
                        failed += 1
                        continue
                        
                    rows.append({
                        "source_job_id": row.source_job_id,
                        "html_description": description_html,
                        "scraped_at": dt.datetime.now(),
                        "source": row.source,
                        "redirect_url": row.redirect_url,
                    })

                    if len(rows) >= CHUNK_SIZE:
                        insert_descriptions(con, rows)
                        rows = []
                        
                    time.sleep(0.5)
                
                except Exception:
                    logger.warning("Failed to scrape %s", row.redirect_url)
                    failed += 1

            if rows:
                insert_descriptions(con, rows)

            final_count = db.count_items(con, table_name="descriptions_raw")
            inserted = final_count - initial_count

            logger.info(
                f"Job description scrapping completed.\n"
                f"✅ Success: {inserted} descriptions.\n"
                f"❌ Missed: {failed}"
            )
                        