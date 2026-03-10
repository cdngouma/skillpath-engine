import datetime
import duckdb
import json
import yaml
from statcan_wds import get_table_data

def fetch_data(cfg_path, fmt="yaml"):
    if fmt == "yaml" or fmt == "yml":
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    elif fmt == "json":
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

    df = get_table_data(
        pid=cfg["pid"],
        query_spec=cfg.get("query", None),
        ref_start=cfg.get("ref_start", None),
        ref_end=cfg.get("ref_end", None)
    )

    # Add table ID
    df["pid"] = cfg["pid"]

    return df
    
def ingest(cfg_path, table_name, db_path="data/warehouse.duckdb", mode="build"):
    with duckdb.connect(db_path) as db:
        # Fetch data as DataFrame
        df = fetch_data(cfg_path)

        # Add Metadata
        df["ingested_at"] = datetime.datetime.now()
        df["source"] = "StatCan"

        # Write to Bronze Schema
        if mode == "build":
            db.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM df")
        elif mode == "update":
            db.execute(f"INSERT OR IGNORE INTO bronze.{table_name} SELECT * FROM df")
