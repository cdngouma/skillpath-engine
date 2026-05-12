CREATE TABLE IF NOT EXISTS jobs_raw (
    source VARCHAR NOT NULL,
    source_job_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    search_term VARCHAR NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    redirect_url VARCHAR,
    raw_json JSON NOT NULL,
    UNIQUE(source, source_job_id)
);

CREATE TABLE IF NOT EXISTS descriptions_raw (
    source VARCHAR NOT NULL,
    source_job_id VARCHAR NOT NULL,
    redirect_url VARCHAR,
    scraped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    html_description VARCHAR,
    UNIQUE(source, source_job_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    source VARCHAR NOT NULL,
    source_job_id VARCHAR NOT NULL,
    posted_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    job_title VARCHAR NOT NULL,
    role_title VARCHAR NOT NULL,
    company VARCHAR NOT NULL,
    location VARCHAR,
    min_salary DOUBLE,
    max_salary DOUBLE,
    redirect_url VARCHAR,
    UNIQUE(source, source_job_id)
);

CREATE TABLE IF NOT EXISTS job_requirements (
    source VARCHAR NOT NULL,
    source_job_id VARCHAR NOT NULL,
    technical_tools VARCHAR[],
    technical_concepts VARCHAR[],
    certifications VARCHAR[],
    min_years INT,
    max_years INT,
    extracted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, source_job_id)
);