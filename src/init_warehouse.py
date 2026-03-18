import os
import duckdb
import logging

logger = logging.getLogger(__name__)

DB_PATH = "../data/warehouse.duckdb"

def run_sql_file(conn, file_path):
    if not os.path.exists(file_path):
        logger.warning(f"Warning: SQL file {file_path} not found.")
        return
    with open(file_path, 'r') as f:
        sql_query = f.read()    
    # Execute the entire script
    conn.execute(sql_query)

def init_bronze(db_path=DB_PATH):
    with duckdb.connect(db_path) as conn:
        run_sql_file(conn, "../sql/init_bronze.sql")
        logger.info(f"Bronze layer initialized at {db_path}")

def init_silver(db_path=DB_PATH):
    with duckdb.connect(db_path) as conn:
        run_sql_file(conn, "../sql/init_silver.sql")
        logger.info(f"Silver layer initialized at {db_path}")

if __name__ == "__main__":
    init_bronze()
    init_silver()