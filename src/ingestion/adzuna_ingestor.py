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

DB_PATH = "../data/warehouse.duckdb"

SEARCH_TERMS_CFG = "../config/search_terms.yaml"
WHAT_EXCLUDE = "director head president vice vp chief founder co-founder manager gestionnaire directeur"

SOURCE_NAME = "Adzuna"

RESULTS_PER_PAGE = 50
MAX_PAGES = 5
MAX_DAYS_OLD = 90
REQUEST_SLEEP_SECONDS = 0.7


def _load_config(cfg_path=SEARCH_TERMS_CFG):
    with open(cfg_path, "r", encoding="utf-8") as f:
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


def _parse_created_ts(value):
    if not value:
        return None
    try:
        return pd.to_datetime(value, utc=False).to_pydatetime()
    except Exception:
        logger.warning(f"Could not parse created timestamp: {value}")
        return None


def fetch_adzuna_jobs(
    what,
    results_per_page=RESULTS_PER_PAGE,
    max_days_old=MAX_DAYS_OLD,
    max_pages=MAX_PAGES,
):
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    country = "ca"

    if not app_id or not app_key:
        raise ValueError("Missing ADZUNA_APP_ID or ADZUNA_APP_KEY in environment.")

    all_jobs = []

    for page in range(1, max_pages + 1):
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": results_per_page,
            "what": what,
            "what_exclude": WHAT_EXCLUDE,
            "category": "it-jobs",
            "salary_include_unknown": 1,
            "max_days_old": max_days_old,
            "full_time": 1,
            "content-type": "application/json",
        }

        response = requests.get(url, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            all_jobs.extend(results)

            if len(results) < results_per_page:
                break
        else:
            logger.error(
                f"Error fetching Adzuna jobs for '{what}': "
                f"{response.status_code}: {response.text}"
            )
            break

        time.sleep(REQUEST_SLEEP_SECONDS)

    return all_jobs


def _truncate_postings_if_build(con, mode):
    if mode == "build":
        logger.warning("Truncating bronze.job_postings_raw...")
        con.execute("TRUNCATE TABLE bronze.job_postings_raw;")


def _truncate_descriptions_if_build(con, mode):
    if mode == "build":
        logger.warning("Truncating bronze.job_descriptions_raw...")
        con.execute("TRUNCATE TABLE bronze.job_descriptions_raw;")


def _get_existing_posting_hashes(con):
    rows = con.execute("SELECT job_hash FROM bronze.job_postings_raw").fetchall()
    return {r[0] for r in rows}


def _build_posting_row(job_hash, search_query, job):
    return {
        "job_hash": job_hash,
        "title": job.get("title"),
        "created": _parse_created_ts(job.get("created")),
        "redirect_url": job.get("redirect_url"),
        "search_query": search_query,
        "ingested_at": datetime.datetime.now(),
        "source": SOURCE_NAME,
        "raw_payload": json.dumps(job),
    }


def _insert_posting_rows(con, rows):
    if not rows:
        return 0

    df = pd.DataFrame(rows)

    con.register("postings_df", df)
    con.execute("""
        INSERT INTO bronze.job_postings_raw
        (job_hash, title, created, redirect_url, search_query, ingested_at, source, raw_payload)
        SELECT
            job_hash,
            title,
            created,
            redirect_url,
            search_query,
            ingested_at,
            source,
            raw_payload::JSON
        FROM postings_df
    """)
    con.unregister("postings_df")
    return len(rows)


def _process_search_term_for_postings(con, search_query, jobs_raw, existing_hashes):
    new_rows = []

    for job in jobs_raw:
        if not job:
            logger.warning("Empty response row from Adzuna.")
            continue

        job_hash = _generate_job_hash(job)
        if job_hash in existing_hashes:
            continue

        new_rows.append(_build_posting_row(job_hash, search_query, job))

    inserted = _insert_posting_rows(con, new_rows)
    if inserted:
        logger.info(f"Inserted {inserted} new postings for query: {search_query}")
    return inserted


def ingest_jobs(mode="update", max_days_old=MAX_DAYS_OLD):
    """
    Lightweight ingestion for demand tracking.
    Writes only bronze.job_postings_raw.
    No Playwright scraping here.
    """
    config = _load_config()
    search_terms = config["search_terms"]

    with duckdb.connect(DB_PATH) as con:
        _truncate_postings_if_build(con, mode)
        existing_hashes = _get_existing_posting_hashes(con)

        # Flatten {noc: [query1, query2, ...]} into one list of (noc, query)
        query_plan = []
        for noc_code, queries in search_terms.items():
            for query in queries:
                query_plan.append((noc_code, query))

        n_iters = len(query_plan)

        for counter, (noc_code, query) in enumerate(query_plan, start=1):
            logger.info(f"Fetching postings for query: '{query}'...")

            jobs_raw = fetch_adzuna_jobs(
                what=query,
                results_per_page=RESULTS_PER_PAGE,
                max_days_old=max_days_old,
                max_pages=MAX_PAGES,
            )

            inserted = _process_search_term_for_postings(
                con=con,
                search_query=query,
                jobs_raw=jobs_raw,
                existing_hashes=existing_hashes,
            )

            if inserted:
                existing_hashes = _get_existing_posting_hashes(con)

            logger.info(f"Ingestion {counter}/{n_iters} completed.")
            time.sleep(REQUEST_SLEEP_SECONDS)


def _get_jobs_to_scrape(con, days_back=90, limit=None):
    query = f"""
        SELECT
            p.job_hash,
            p.created,
            p.source,
            p.redirect_url
        FROM bronze.job_postings_raw p
        LEFT JOIN bronze.job_descriptions_raw d
            ON p.job_hash = d.job_hash
        WHERE d.job_hash IS NULL
          AND p.created IS NOT NULL
          AND p.created >= CURRENT_TIMESTAMP - INTERVAL '{days_back} days'
          AND p.redirect_url IS NOT NULL
    """
    if limit:
        query += f" LIMIT {limit}"

    return con.execute(query).df()


def _get_raw_description(scraper, redirect_url):
    if not redirect_url:
        return None
    return scraper.fetch(redirect_url)


def _insert_description_rows(con, rows):
    if not rows:
        return 0

    df = pd.DataFrame(rows)

    con.register("descriptions_df", df)
    con.execute("""
        INSERT INTO bronze.job_descriptions_raw
        (job_hash, description_html, created, scraped_at, source, redirect_url)
        SELECT job_hash, description_html, created, scraped_at, source, redirect_url
        FROM descriptions_df
    """)
    con.unregister("descriptions_df")
    return len(rows)


def ingest_descriptions(mode="update", days_back=90, limit=None):
    """
    Scrape descriptions only for recent postings that do not already
    exist in bronze.job_descriptions_raw.
    """
    with duckdb.connect(DB_PATH) as con:
        _truncate_descriptions_if_build(con, mode)

        jobs_to_scrape = _get_jobs_to_scrape(con, days_back=days_back, limit=limit)

        if jobs_to_scrape.empty:
            logger.info("No job descriptions to scrape.")
            return

        rows = []
        chunk_size = 50

        with PlaywrightScraper() as scraper:
            for row in jobs_to_scrape.itertuples(index=False):
                job_hash = row.job_hash
                created = row.created
                source = row.source
                redirect_url = row.redirect_url

                try:
                    description_html = _get_raw_description(scraper, redirect_url)
                    if not description_html:
                        continue

                    rows.append({
                        "job_hash": job_hash,
                        "description_html": description_html,
                        "created": created,
                        "scraped_at": datetime.datetime.now(),
                        "source": source,
                        "redirect_url": redirect_url,
                    })

                    if len(rows) >= chunk_size:
                        inserted = _insert_description_rows(con, rows)
                        logger.info(f"Inserted {inserted} scraped descriptions.")
                        rows = []

                except Exception as e:
                    logger.warning(f"Failed scraping job_hash={job_hash}: {e}")
                    continue

                time.sleep(REQUEST_SLEEP_SECONDS)

        if rows:
            inserted = _insert_description_rows(con, rows)
            logger.info(f"Inserted {inserted} scraped descriptions.")

        logger.info(f"Scraping complete for {len(jobs_to_scrape)} candidate postings.")