-- 1. Setup Medallion Schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- 2 Run Table Definitions
CREATE TABLE IF NOT EXISTS bronze.job_postings_raw (
    job_hash VARCHAR PRIMARY KEY,
    title VARCHAR,
    created TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR,
    raw_payload JSON
);

CREATE TABLE IF NOT EXISTS bronze.job_descriptions_raw (
    job_hash VARCHAR PRIMARY KEY,
    description_html VARCHAR,
    created TIMESTAMP,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR,
    redirect_url VARCHAR
);