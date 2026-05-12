# SkillPath Pipeline

A data pipeline that ingests job postings, extracts structured requirements from job descriptions, normalizes job titles into canonical roles, and stores the results in DuckDB for labor-market analytics.

## Overview

This project turns raw job postings into a structured analytics dataset for AI, data, and analytics roles.

The pipeline collects job metadata and descriptions, processes raw HTML/text, maps noisy job titles to canonical roles, extracts technical requirements with an LLM, and stores both raw and curated outputs in a local DuckDB warehouse.

The goal is to support analysis such as:

- top tools by role
- common technical concepts by role
- experience requirements by role
- certification demand
- role and seniority distributions
- career path recommendation

## Pipeline

```text
Job API + scraped descriptions
        ↓
Raw storage
        ↓
Role mapping and metadata normalization
        ↓
LLM-based requirements extraction
        ↓
DuckDB analytics tables and views
```

## Data Layers
- `jobs_raw`: Raw job metadata from the job API.
- `descriptions_raw`: Scraped job descriptions and HTML content.
- `jobs`: Cleaned job metadata with normalized canonical roles.
- `job_requirements`: Structured requirements extracted from job descriptions.
- `job_requirement_items`: View that flattens tools, concepts, and certifications into one row per requirement item.

### Extracted Fields

The pipeline extracts and stores:
- canonical role title
- company
- location
- salary range when available
- technical tools
- technical concepts
- certifications
- minimum and maximum years of experience
- inferred seniority

## Project Structure

```text
skillpath-pipeline/
├── data/
│   └── warehouse.duckdb
├── notebooks/
│   ├── 01_data_audit_and_role_mapping.ipynb
│   └── 02_extraction_prototyping.ipynb
├── scripts/
│   ├── ingest.py
│   ├── build_jobs_table.py
│   └── extract_requirements.py
├── sql/
│   ├── schema.sql
│   └── views.sql
└── src/
    ├── config.py
    ├── role_taxonomy.json
    ├── ingestion/
    │   ├── adzuna.py
    │   └── scrape.py
    ├── processing/
    │   ├── role_mapper.py
    │   ├── section_extractor.py
    │   └── requirement_extractor.py
    └── storage/
        └── db.py
```

## Setup

Install dependencies:
```bash
pip install -r requirements.txt
python -m playwright install
```

Create a `.env` file for API credentials:

```text
ADZUNA_APP_ID=your_app_id
ADZUNA_APP_KEY=your_app_key
```

The project uses Ollama for local LLM extraction. Make sure Ollama is running and the model is available:

```bash
ollama pull gemma3:4b
```

## Usage

```bash
# Run raw ingestion:
python -m scripts.ingest_raw

# Build the curated jobs table:
python -m scripts.build_jobs_table

# Extract structured requirements:
python -m scripts.extract_requirements

# Run a small extraction test:
python -m scripts.extract_requirements --limit 10 --dry-run
```

## Example Analytics

Top tools by role:

```sql
SELECT
    j.role_title,
    r.item_value_norm AS tool,
    COUNT(*) AS n_jobs
FROM job_requirement_items r
JOIN jobs j
  ON r.source = j.source
 AND r.source_job_id = j.source_job_id
WHERE r.item_type = 'technical_tool'
GROUP BY j.role_title, r.item_value_norm
ORDER BY j.role_title, n_jobs DESC;
```

Jobs requiring Python:

```sql
SELECT
    j.role_title,
    COUNT(DISTINCT j.source || ':' || j.source_job_id) AS n_jobs
FROM jobs j
JOIN job_requirement_items r
  ON j.source = r.source
 AND j.source_job_id = r.source_job_id
WHERE r.item_value_norm = 'python'
GROUP BY j.role_title
ORDER BY n_jobs DESC;
```

