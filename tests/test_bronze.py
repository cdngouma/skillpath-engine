import duckdb
import os
import pytest

def get_connection():
    db_path = "data/warehouse.duckdb"
    if not os.path.exists(db_path):
        pytest.fail(f"Database not found at {db_path}. Run ingestion first.")
    return duckdb.connect(db_path)

def test_table_existence():
    with get_connection() as conn:
        expected_tables = [
            "job_postings_raw",
            "sc_graduates_trends_raw",
            "sc_census_income_raw",
            "sc_labour_trends_raw",
            "sc_census_labour_raw",
            "sc_wages_trends_raw"
        ]
    
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").fetchall()
        existing_tables = [t[0] for t in tables]
    
        for table in expected_tables:
            assert table in existing_tables, f"Table {table} is missing from Bronze schema."

def test_critical_columns():
    with get_connection() as conn:
        # 1. Test Job Postings specific columns
        job_cols = [c[1] for c in conn.execute("PRAGMA table_info('bronze.job_postings_raw')").fetchall()]
        assert "job_hash" in job_cols
        assert "noc_code" in job_cols
        assert "ingested_at" in job_cols
        assert "source" in job_cols
    
        # 2. Test StatCan tables (Sampling one for standard columns)
        statcan_tables = ["census_income_2021_raw", "annual_wages_raw"]
        for table in statcan_tables:
            cols = [c[1] for c in conn.execute(f"PRAGMA table_info('bronze.{table}')").fetchall()]
            assert "VALUE" in cols
            assert "REF_DATE" in cols
            assert "ingested_at" in cols
            assert "source" in cols

if __name__ == "__main__":
    # Allow running directly without pytest if needed
    test_table_existence()
    test_critical_columns()
    print("✅ All Bronze Integrity Tests Passed!")