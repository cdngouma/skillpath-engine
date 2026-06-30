# SkillPath Pipeline

An LLM-assisted data pipeline that transforms unstructured job postings into a structured labor-market intelligence database for analytics and career insights.

The pipeline ingests job postings, normalizes inconsistent job titles, extracts structured requirements from free-text job descriptions, and stores curated outputs in DuckDB for downstream analysis.

Unlike traditional ETL pipelines that preserve raw text, SkillPath converts noisy job advertisements into structured datasets suitable for labor-market analytics, recommendation systems, and AI-assisted career planning.

---

## Motivation

Job postings contain valuable information about technical skills, certifications, experience requirements, and hiring trends.

However, most of this information exists as unstructured text, making large-scale analysis difficult.

SkillPath automates this process by combining deterministic data processing with LLM-based information extraction to produce analytics-ready datasets.

The resulting warehouse supports questions such as:

- Which tools are most frequently requested for AI Engineers?
- Which certifications are associated with senior roles?
- How do skill requirements differ between Data Scientists and ML Engineers?
- Which technologies are emerging across different occupations?

---

## Features

### Data Ingestion

- Job API integration
- HTML scraping
- Raw data preservation

### Data Processing

- Canonical role normalization
- Metadata cleaning
- Seniority inference
- Structured schema generation

### Information Extraction

- LLM-based requirement extraction
- Technical tools
- Technical concepts
- Certifications
- Experience requirements

### Analytics

- DuckDB warehouse
- SQL views
- Labor-market analytics
- Career-path exploration

---

## Pipeline Architecture

```text
           Job API + Web Scraping
                     │
                     ▼
               Raw Job Storage
                     │
                     ▼
        Metadata Cleaning & Normalization
                     │
                     ▼
          Canonical Role Mapping
                     │
                     ▼
       LLM Requirement Extraction
                     │
                     ▼
        Structured Analytics Tables
                     │
                     ▼
      SQL Analytics & Career Insights
```

---

## Data Model

### Raw Layer

- `jobs_raw`
- `descriptions_raw`

### Curated Layer

- `jobs`
- `job_requirements`

### Analytics Layer

- `job_requirement_items`

---

## Extracted Information

Each posting is transformed into structured attributes including:

- Canonical job role
- Company
- Location
- Salary range
- Technical tools
- Technical concepts
- Certifications
- Experience requirements
- Seniority level

---

## Project Structure

```text
skillpath-pipeline/
│
├── data/
├── notebooks/
├── scripts/
├── sql/
└── src/
    ├── ingestion/
    ├── processing/
    └── storage/
```

---

## Tech Stack

- Python
- DuckDB
- Playwright
- Ollama
- Pandas
- SQL

---

## Example Analytics

Top technical tools by role

```sql
SELECT
    j.role_title,
    r.item_value_norm AS tool,
    COUNT(DISTINCT j.source || ':' || j.source_job_id) AS job_count
FROM job_requirement_items AS r
JOIN jobs AS j
    ON r.source = j.source
   AND r.source_job_id = j.source_job_id
WHERE r.item_type = 'technical_tool'
GROUP BY
    j.role_title,
    r.item_value_norm
ORDER BY
    j.role_title,
    job_count DESC;
```

---

## Future Work

- Skill taxonomy learning
- Occupation clustering
- Salary normalization
- Skill demand trend analysis
- Career recommendation engine
