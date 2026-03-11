import os
import duckdb
import logging

logger = logging.getLogger(__name__)

def run_sql_file(conn, file_path):
    if not os.path.exists(file_path):
        logger.warning(f"Warning: SQL file {file_path} not found.")
        return
    with open(file_path, 'r') as f:
        sql_query = f.read()    
    # Execute the entire script
    conn.execute(sql_query)

def init_warehouse(db_path="../data/warehouse.duckdb"):
    with duckdb.connect(db_path) as conn:
        # Initialize database
        run_sql_file(conn, "sql/init_database.sql")
        logger.info(f"Warehouse initialized at {db_path} with Medallion schemas")

if __name__ == "__main__":
    init_warehouse()