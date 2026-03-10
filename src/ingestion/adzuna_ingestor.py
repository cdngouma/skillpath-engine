import os
import requests
import yaml
import json
import duckdb
import datetime
import time
import pandas as pd
import hashlib
import re
import logging
from dotenv import load_dotenv
from ingestion.job_scraper import PlaywrightScraper

logger = logging.getLogger(__name__)

load_dotenv()

PAGES_PER_JOB = 1
RESULTS_PER_PAGE = 2
DB_PATH = "data/warehouse.duckdb"

def _load_config(cfg_path="config/role_mapping.yaml"):
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)

def _normalize_text(s):
    s = (s or "").lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _generate_job_hash(job_dict):
    title = _normalize_text(job_dict.get("title", ""))
    company = _normalize_text(job_dict.get("company", {}).get("display_name", ""))
    loc = _normalize_text(job_dict.get("location", {}).get("display_name", ""))
    desc = _normalize_text(job_dict.get("description", ""))
    desc = desc[:500]
    combined = f"{title}|{company}|{loc}|{desc}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def fetch_adzuna_jobs(what, pages=5, results_per_page=50):
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    country = "ca"

    all_jobs = []
    for page in range(1, pages + 1):
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": RESULTS_PER_PAGE,
            "what": what,
            "category": "it-jobs",        # Filters out non-tech noise
            "salary_include_unknown": 1,  # Ensures we get volume/demand metrics
            "max_days_old": 30,
            "full_time": 1,
            "content-type": "application/json"
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            all_jobs.extend(data.get("results", []))
        else:
            logger.error(f"Error: {response.status_code}: {response.text}")
            break
    return all_jobs

def _truncate_bronze_if_build(con, mode):
    if mode == "build":
        logger.warning("Truncating bronze.job_postings_raw...")
        con.execute("TRUNCATE TABLE bronze.job_postings_raw;")

def _get_existing_hashes(con, term):
    rows = con.execute(
        "SELECT job_hash FROM bronze.job_postings_raw WHERE search_term = ?",
        [term]
    ).fetchall()
    return {r[0] for r in rows}

def _should_scrape(api_desc, min_len=500):
    return len(api_desc or "") <= min_len

def _get_best_description(job, scraper, min_len=500):
    """
    Prefer scraped description when API description is short and redirect_url exists.
    """
    api_desc = job.get("description") or ""
    if not _should_scrape(api_desc, min_len=min_len):
        return api_desc

    redirect_url = job.get("redirect_url")
    if not redirect_url:
        return api_desc

    scraped = scraper.fetch(redirect_url)
    return scraped if scraped else api_desc

def _insert_rows(con, rows):
    if not rows:
        return 0

    df = pd.DataFrame(rows)
    query = """
        INSERT INTO bronze.job_postings_raw
        (job_hash, description, search_term, ingested_at, source, raw_payload)
        SELECT job_hash, description, search_term, ingested_at, source, raw_payload
        FROM df
    """
    con.execute(query)
    return len(rows)

def _build_bronze_row(job_hash: str, term: str, job_desc: str, job: dict) -> dict:
    return {
        "job_hash": job_hash,
        "description": job_desc,
        "search_term": term,
        "ingested_at": datetime.datetime.now(),
        "source": "Adzuna",
        "raw_payload": json.dumps(job),
    }

def _process_term(con, term, jobs_raw, scraper, existing_hashes):
    """
    Returns number of inserted rows for this term.
    """
    new_rows: list[dict] = []

    for job in jobs_raw:
        if not job:
            logger.warning("Empty response")
            continue

        job_hash = _generate_job_hash(job)
        if job_hash in existing_hashes:
            continue

        # logging kept, but moved up so loop stays readable
        logger.info(f"Scraping: '{job.get('title')}' @ '{job.get('company', {}).get('display_name')}'")

        job_desc = _get_best_description(job, scraper, min_len=500)
        new_rows.append(_build_bronze_row(job_hash, term, job_desc, job))

        time.sleep(1.0)  # rate limit per job

    inserted = _insert_rows(con, new_rows)
    if inserted:
        logger.info(f"Successfully inserted {inserted} new jobs for {term}.")
    return inserted

def ingest(mode="update"):
    config = _load_config()
    role_mapping = config["role_mapping"]

    with duckdb.connect(DB_PATH) as con:
        _truncate_bronze_if_build(con, mode)

        with PlaywrightScraper() as scraper:
            for noc_code, search_terms in role_mapping.items():
                for term in search_terms:
                    logger.info(f"--- Processing {term} (NOC {noc_code}) ---")

                    existing_hashes = _get_existing_hashes(con, term)
                    jobs_raw = fetch_adzuna_jobs(what=term, pages=PAGES_PER_JOB)

                    _process_term(
                        con=con,
                        term=term,
                        jobs_raw=jobs_raw,
                        scraper=scraper,
                        existing_hashes=existing_hashes,
                    )

                    time.sleep(2.0)  # cooldown between search terms