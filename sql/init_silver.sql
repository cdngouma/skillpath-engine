-- 1. Setup Medallion Schema
CREATE SCHEMA IF NOT EXISTS silver;

-- 2. StatCan Census Tables
CREATE TABLE IF NOT EXISTS silver.sc_census_income (
    education_level VARCHAR,
    occupation VARCHAR,
    median_income DOUBLE,
    noc_code VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR
);

CREATE TABLE IF NOT EXISTS silver.sc_census_labour (
    education_level VARCHAR,
    occupation VARCHAR,
    employed DOUBLE,
    unemployed DOUBLE,
    labour_force DOUBLE,
    noc_code VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR
);

-- 3. StatCan Trend Tables (Time Series)
CREATE TABLE IF NOT EXISTS silver.sc_wages_trends (
    occupation VARCHAR,
    date DATE,
    weekly_wages DOUBLE,
    noc_code VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR
);

CREATE TABLE IF NOT EXISTS silver.sc_labour_trends (
    occupation VARCHAR,
    date DATE,
    labour_force DOUBLE,
    unemployment_rate DOUBLE,
    noc_code VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR
);

CREATE TABLE IF NOT EXISTS silver.sc_graduates_trends (
    education_level VARCHAR,
    field_of_study VARCHAR,
    date DATE,
    graduates DOUBLE,
    noc_code VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR
);

-- 4. Processed Job Market Tables
CREATE TABLE IF NOT EXISTS silver.job_postings (
    job_hash VARCHAR PRIMARY KEY,
    title VARCHAR,
    canonical_role VARCHAR,
    noc_code VARCHAR,
    confidence_score DOUBLE,
    source VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.job_skills (
    job_hash VARCHAR,
    skill_name VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Establishing a link to the main posting
    FOREIGN KEY (job_hash) REFERENCES silver.job_postings (job_hash)
);