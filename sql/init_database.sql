-- 1. Setup Medallion Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 2 Run Table Definitions
CREATE TABLE IF NOT EXISTS bronze.job_postings_raw (
    job_hash VARCHAR PRIMARY KEY,
    adzuna_id VARCHAR,
    title VARCHAR,
    company VARCHAR,
    created_at TIMESTAMP,
    category VARCHAR,
    location VARCHAR,
    description VARCHAR,
    search_term VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR,
    redirect_url VARCHAR
)