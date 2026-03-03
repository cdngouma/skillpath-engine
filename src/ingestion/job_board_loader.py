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
            "results_per_page": 50,
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
        
def process_single_job(job, search_term, full_description, redirect_url):
    """Parses a single job into the standard schema format matching DuckDB exactly."""
    return {
        "job_hash": generate_job_hash(job),
        "adzuna_id": str(job.get('id')),
        "title": job.get('title'),
        "company": job.get('company', {}).get('display_name'),
        "created_at": job.get('created'),
        "category": job.get('category', {}).get('label'),
        "location": job.get('location', {}).get('display_name', 'Canada'),
        "description": full_description,
        "search_term": search_term,
        "ingested_at": datetime.datetime.now(),
        "source": "Adzuna",
        "redirect_url": redirect_url
    }

def ingest_jobs_to_bronze(mode="update"):
    config = load_config()
    role_mapping = config['role_mapping']
    #session = get_stealth_session()

    with duckdb.connect("data/warehouse.duckdb") as con:
        if mode == "build":
            con.execute("TRUNCATE TABLE bronze.job_postings_raw;")
        
        for noc_code, search_terms in role_mapping.items():
            for term in search_terms:
                logger.info(f"--- Processing {term} (NOC {noc_code}) ---")
                jobs_raw = fetch_adzuna_jobs(what=term, pages=2)
                new_jobs_list = []
                
                for job in jobs_raw:
                    j_hash = generate_job_hash(job)
                    exists = con.execute(
                        "SELECT 1 FROM bronze.job_postings_raw WHERE job_hash = ?", [j_hash]
                    ).fetchone()
                    
                    if not exists:
                        logger.info(f"Scrapping job description for: '{job.get('title')}' @ '{job.get('company', {}).get('display_name')}'...")
                        redirect_url = job.get('redirect_url')
                        full_desc = fetch_job_description_playwright(redirect_url)
                        final_desc = full_desc if full_desc else job.get('description')
                        
                        job_data = process_single_job(job, term, final_desc, redirect_url)
                        new_jobs_list.append(job_data)
                        time.sleep(1.0)
                    else:
                        logger.info(f"--Skipped: '{job.get('title')}' @ '{job.get('company', {}).get('display_name')}'...")

                if new_jobs_list:
                    df = pd.DataFrame(new_jobs_list)

                    # Defining the column order explicitly
                    cols = [
                        "job_hash", "adzuna_id", "title", "company", "created_at", 
                        "category", "location", "description", "search_term", 
                        "ingested_at", "source", "redirect_url"
                    ]
                    col_string = ", ".join(cols)
                    con.execute(f"INSERT OR IGNORE INTO bronze.job_postings_raw ({col_string}) SELECT * FROM df")
                    logger.info(f"Successfully inserted {len(new_jobs_list)} new jobs.")
                
                time.sleep(2.0)