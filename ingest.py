import os
import re
import argparse
import logging
from dotenv import load_dotenv
from src.init_warehouse import init_warehouse
from src.ingestion.statcan_loader import ingest_statcan_to_bronze
from src.ingestion.job_board_loader import ingest_jobs_to_bronze

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Config mapping for StatCan Table IDs to local names and config files
STATCAN_CONFIG = {
    "98100412": ("census_income_2021_raw", "statcan_census_income.yaml"),
    "98100449": ("census_labour_force_2021_raw", "statcan_census_lfs.yaml"),
    "14100417": ("annual_wages_raw", "statcan_annual_wages.yaml"),
    "14100416": ("annual_labour_force_raw", "statcan_annual_lfs.yaml"),
    "37100276": ("annual_graduates_raw", "statcan_annual_graduates.yaml")
}

def ingest_statcan(mode="build", table_id="all"):
    if table_id == "all":
        for tid, (t_name, source_cfg) in STATCAN_CONFIG.items():
            ingest_statcan_to_bronze(
                cfg_path=f"config/data_sources/{source_cfg}", 
                table_name=t_name, 
                mode=mode
            )
    else:
        config_entry = STATCAN_CONFIG.get(table_id)
        if config_entry:
            t_name, source_cfg = config_entry
            ingest_statcan_to_bronze(
                cfg_path=f"config/data_sources/{source_cfg}", 
                table_name=t_name, 
                mode=mode
            )
        else:
            logger.error(f"Could not find configuration file for Table ID: {table_id}")       

def main(mode="build", source="all"):
    # 1. Validation
    if not re.match(r"all|adzuna|\d{8}", source):
        logger.error(f"Invalid value for source: {source}. Use 'all', 'adzuna', or an 8-digit Table ID.")
        return

    # 2. Environment & Database Prep
    load_dotenv()
    if not os.path.exists("data/warehouse.duckdb"):
        logger.info("Database not found. Initializing warehouse...")
        init_warehouse(db_path="data/warehouse.duckdb")

    # 3. StatCan Routing
    # If source is 'all' or an 8-digit number, we handle StatCan
    if source == "all" or source.isdigit():
        table_param = "all" if source == "all" else source
        ingest_statcan(mode=mode, table_id=table_param)

    # 4. Adzuna Routing
    if source == "all" or source == "adzuna":
        logger.info("Starting ingestion for Adzuna Job Postings...")
        ingest_jobs_to_bronze(mode=mode)

    logger.info("Pipeline execution complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SkillPath-Engine Bronze Ingestion Pipeline") 
    # --source flag (default: all)
    parser.add_argument(
        "--source", 
        type=str, 
        default="all", 
        help="Source to ingest: 'all', 'adzuna', or specific 8-digit StatCan Table ID."
    )
    # --update flag (toggle mode to update)
    parser.add_argument(
        "--update", 
        action="store_true", 
        help="Run in update mode (append data) instead of build mode (overwrite)."
    )
    args = parser.parse_args()
    # Determine mode based on flag
    run_mode = "update" if args.update else "build"
    main(mode=run_mode, source=args.source)
