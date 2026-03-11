-- 1. Setup Medallion Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 2 Run Table Definitions
CREATE TABLE IF NOT EXISTS bronze.job_postings_raw (
    job_hash VARCHAR PRIMARY KEY,
    description_html VARCHAR,
    search_term VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR,
    raw_payload JSON
)