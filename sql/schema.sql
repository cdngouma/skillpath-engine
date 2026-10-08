
CREATE SCHEMA IF NOT EXISTS skillpath;

CREATE TABLE IF NOT EXISTS skillpath.jobs_raw (
    source VARCHAR NOT NULL,
    source_job_id VARCHAR NOT NULL,

    title VARCHAR NOT NULL,
    company VARCHAR,
    location VARCHAR,
    posted_at TIMESTAMPTZ,
    description VARCHAR,
    url VARCHAR,

    salary_min DOUBLE,
    salary_max DOUBLE,

    raw_payload JSON NOT NULL,

    first_seen_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),

    PRIMARY KEY (source, source_job_id)
);

CREATE TABLE IF NOT EXISTS skillpath.job_search_matches (
    source VARCHAR NOT NULL,
    source_job_id VARCHAR NOT NULL,
    canonical_role VARCHAR NOT NULL,
    search_term VARCHAR NOT NULL,

    first_matched_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),
    last_matched_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),

    PRIMARY KEY (
        source,
        source_job_id,
        canonical_role,
        search_term
    ),

    FOREIGN KEY (source, source_job_id)
        REFERENCES skillpath.jobs_raw(source, source_job_id)
);

CREATE TABLE IF NOT EXISTS skillpath.ingestion_runs (
    run_id VARCHAR PRIMARY KEY,
    source VARCHAR NOT NULL,

    started_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),
    finished_at TIMESTAMPTZ,

    status VARCHAR NOT NULL
        CHECK (status IN ('running', 'completed', 'failed')),

    records_processed INTEGER NOT NULL DEFAULT 0,
    error_message VARCHAR
);
