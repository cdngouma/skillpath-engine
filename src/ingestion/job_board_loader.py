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

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

def load_config(cfg_path="config/role_mapping.yaml"):
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)

def generate_job_hash(job_dict):
    # Concatenate core fields into a single string for hashing
    # We use .get() and lower() to ensure consistency
    combined_str = (
        f"{job_dict.get('title', '')}"
        f"{job_dict.get('company', {}).get('display_name', '')}"
        f"{job_dict.get('location', {}).get('display_name', '')}"
        f"{job_dict.get('description', '')}"
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

def process_adzuna_response(api_results, noc_code=None, search_term=None):
    processed_jobs = []

    for job in api_results:
        # Extract location: handle list of area names or display_name
        location_name = job.get('location', {}).get('display_name', 'Canada')
        # Build the lean record
        clean_job = {
            "adzuna_id": job.get('id'),
            "job_hash": generate_job_hash(job),  # Our unique identifier
            "title": job.get('title'),
            "company": job.get('company', {}).get('display_name'),
            "created_at": job.get('created'),
            "category": job.get('category', {}).get('label'),
            "location": location_name,
            "description": job.get('description'),
            "noc_code": noc_code,
            "search_term": search_term
        }
        
        processed_jobs.append(clean_job)
        
    return processed_jobs

def ingest_jobs_to_bronze(mode="update"):
    config = load_config()
    role_mapping = config['role_mapping']
    
    with duckdb.connect("data/warehouse.duckdb") as db:
        for noc_code, search_terms in role_mapping.items():
            for term in search_terms:
                logger.info(f"Fetching {term} for NOC {noc_code}...")
                jobs_raw = fetch_adzuna_jobs(what=term, pages=2)
                jobs_processed = process_adzuna_response(
                    jobs_raw, 
                    noc_code=noc_code, 
                    search_term=term
                )
                
                if not jobs_processed:
                    continue
                df = pd.DataFrame(jobs_processed)
                # Metadata for the dataframe
                df['ingested_at'] = datetime.datetime.now()
                df['source'] = "Adzuna"

                db.execute("INSERT OR IGNORE INTO bronze.job_postings_raw SELECT * FROM df")
                
                # Sleep to stay safely under rate limits
                time.sleep(2.5)
