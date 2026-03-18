import os
import re
import argparse
import logging
from dotenv import load_dotenv
from init_warehouse import init_bronze
from ingestion import statcan_ingestor
from ingestion import adzuna_ingestor

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Config mapping for StatCan Table IDs to local names and config files
STATCAN_CONFIG = {
    "98100412": ("sc_census_income_raw", "statcan_census_income.yaml"),
    "98100449": ("sc_census_labour_raw", "statcan_census_labour.yaml"),
    "14100417": ("sc_wages_trends_raw", "statcan_wages_trends.yaml"),
    "14100416": ("sc_labour_trends_raw", "statcan_labour_trends.yaml"),
    "37100276": ("sc_graduates_trends_raw", "statcan_graduates_trends.yaml")
}

DB_PATH = "../data/warehouse.duckdb"

def ingest_statcan(mode="build", table_id="all"):
    if table_id == "all":
        for tid, (t_name, source_cfg) in STATCAN_CONFIG.items():
            statcan_ingestor.ingest(
                cfg_path=f"../config/data_sources/{source_cfg}",
                table_name=t_name,
                db_path=DB_PATH,
                mode=mode
            )
    else:
        config_entry = STATCAN_CONFIG.get(table_id)
        if config_entry:
            t_name, source_cfg = config_entry
            statcan_ingestor.ingest(
                cfg_path=f"../config/data_sources/{source_cfg}",
                table_name=t_name,
                db_path=DB_PATH,
                mode=mode
            )
        else:
            logger.error(f"Could not find configuration file for Table ID: {table_id}")

def main(mode="build", source="all", days_back=90):
    # 1. Validation
    if not re.fullmatch(r"(all|statcan|adzuna|adzuna_desc|\d{8})", source):
        logger.error(
            f"Invalid value for source: {source}. "
            "Use 'all', 'adzuna', 'adzuna_desc', or an 8-digit StatCan Table ID."
        )
        return

    # 2. Environment & Database Prep
    load_dotenv()
    
    init_bronze(db_path=DB_PATH)

    # 3. StatCan Routing
    if source == "all" or source == "statcan" or source.isdigit():
        table_param = "all" if source in ["all", "statcan"] else source
        ingest_statcan(mode=mode, table_id=table_param)

    # 4. Adzuna postings routing
    if source == "all" or source == "adzuna":
        logger.info("Starting ingestion for Adzuna Job Postings...")
        adzuna_ingestor.ingest_jobs(mode=mode)

    # 5. Adzuna descriptions routing
    if source == "all" or source == "adzuna_desc":
        logger.info("Starting ingestion for Adzuna Job Descriptions...")
        adzuna_ingestor.ingest_descriptions(mode=mode, days_back=days_back)

    logger.info("Pipeline execution complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SkillPath-Engine Bronze Ingestion Pipeline")
    parser.add_argument(
        "--source",
        type=str,
        default="all",
        help="Source to ingest: 'all', 'statcan', 'adzuna', 'adzuna_desc', or specific 8-digit StatCan Table ID."
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Run in update mode (append data) instead of build mode (overwrite)."
    )
    parser.add_argument(
        "--days_back",
        type=int,
        default=90,
        help="How many days of postings to scrape descriptions for (default: 90)."
    )
    args = parser.parse_args()
    run_mode = "update" if args.update else "build"
    main(mode=run_mode, source=args.source, days_back=args.days_back)