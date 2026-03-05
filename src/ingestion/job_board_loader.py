import os
import requests
import yaml
import json
import duckdb
import datetime
import time
import pandas as pd
import hashlib
import logging
from dotenv import load_dotenv
from src.ingestion.scraper import (
    get_stealth_session, 
    fetch_job_description, 
    fetch_job_description_playwright
)

logger = logging.getLogger(__name__)

load_dotenv()

PAGES_PER_JOB = 5
RESULTS_PER_PAGE = 50
DB_PATH = "data/warehouse.duckdb"

def load_config(cfg_path="config/role_mapping.yaml"):
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)

def generate_job_hash(job_dict):
    """Generate hash using stable fields only to ensure idempotency."""
    # Truncate '2026-02-25T12:00:00Z' to '2026-02'
    created_date = job_dict.get('created', '')[:7]
    
    combined_str = (
        f"{job_dict.get('title', '')}"
        f"{job_dict.get('company', {}).get('display_name', '')}"
        f"{job_dict.get('location', {}).get('display_name', '')}"
        f"{created_date}"
    ).lower().strip()
    return hashlib.sha256(combined_str.encode('utf-8')).hexdigest()

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

def ingest_jobs_to_bronze(mode="update"):
    config = load_config()
    role_mapping = config['role_mapping']
    
    # Establish connection once
    with duckdb.connect(DB_PATH) as con:
        if mode == "build":
            logger.warning("Truncating bronze.job_postings_raw...")
            con.execute("TRUNCATE TABLE bronze.job_postings_raw;")

        for noc_code, search_terms in role_mapping.items():
            for term in search_terms:
                logger.info(f"--- Processing {term} (NOC {noc_code}) ---")
                
                # 1. Fetch existing hashes for this term to avoid N+1 queries
                existing_hashes = set(
                    r[0] for r in con.execute(
                        "SELECT job_hash FROM bronze.job_postings_raw WHERE search_term = ?", 
                        [term]
                    ).fetchall()
                )
                
                jobs_raw = fetch_adzuna_jobs(what=term, pages=PAGES_PER_JOB)
                new_jobs_list = []
                
                for job in jobs_raw:
                    if not job:
                        logger.warning("Empty response")
                        continue
                    
                    job_hash = generate_job_hash(job)
                    
                    # 2. Fast in-memory check
                    if job_hash in existing_hashes:
                        continue

                    logger.info(f"Scraping: '{job.get('title')}' @ '{job.get('company', {}).get('display_name')}'")
                    
                    # Scrape full description
                    redirect_url = job.get('redirect_url')
                    full_desc = fetch_job_description_playwright(redirect_url)
                    final_desc = full_desc if full_desc else job.get('description')
                    
                    # 3. Prepare data for the DataFrame
                    new_jobs_list.append({
                        "job_hash": job_hash,
                        "description": final_desc,
                        "search_term": term,
                        "ingested_at": datetime.datetime.now(),
                        "source": "Adzuna",
                        # Convert dict to JSON string for DuckDB JSON type
                        "raw_payload": json.dumps(job) 
                    })
                    
                    # Rate limiting for Playwright
                    time.sleep(1.0)

                # 4. Bulk Insert
                if new_jobs_list:
                    df = pd.DataFrame(new_jobs_list)
                    
                    # DuckDB can query the Pandas DataFrame 'df' directly in the same scope
                    query = """
                        INSERT INTO bronze.job_postings_raw 
                        (job_hash, description, search_term, ingested_at, source, raw_payload)
                        SELECT job_hash, description, search_term, ingested_at, source, raw_payload
                        FROM df
                    """
                    con.execute(query)
                    logger.info(f"Successfully inserted {len(new_jobs_list)} new jobs for {term}.")
                
                # Cooldown between search terms
                time.sleep(2.0)