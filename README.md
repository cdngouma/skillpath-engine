# SkillPath Engine — Career Transition Intelligence System
## Project Outcome

SkillPath Engine is a labor market intelligence system designed to recommend **career transitions in the tech sector** based on a user's current skill set.

The system identifies roles that:
- Require **minimal reskilling effort**
- Exhibit **strong market demand**
- Offer **higher income potential**

Rather than treating careers as isolated endpoints, the project models the job market as a connected system of roles and skills, enabling structured reasoning about feasible and optimal transitions.

## Why this exists
This tool serves to answer to question *"Given what I already know, what is the most efficient path to a better role?"*.

It addresses this by combining:
- real job market data
- structured skill extraction
- transition scoring

to produce data-driven career pathways.

## System Overview
### 1. Labor Market Representation
The system builds a structured view of the job market using:
- Job postings (demand signal)
- Public labor statistics (income, employment trends)
- Extracted technical skills from job descriptions

Roles are normalized into canonical occupations, enabling aggregation and comparison across sources.

### 2. Skill Extraction Pipeline
Job descriptions are converted into structured skill signals using an LLM-based extraction pipeline.

Key properties:
- Focus on technical skills only
- Section-aware parsing (skills vs responsibilities)
- Structured outputs for downstream aggregation

This transforms unstructured text into a machine-readable skill layer.

### 3. Role Profiling
Each role is represented by:
- Skill distribution (frequency-weighted)
- Market demand (posting frequency + trends)
- Income proxies (StatCan wage data)

This produces a comparable representation of occupations.

### 4. Transition Modeling
Career transitions are modeled between roles based on:
- Skill overlap (what you already know)
- Skill gap (what you need to learn)
- Market demand differential
- Income differential

Each transition is scored using a composite function:

```plaintext
Transition Score =
    + Demand Score
    + Salary Score
    - Reskilling Cost
```

This allows ranking transitions from a given starting role.

### 5. Recommendation System
Given a user profile (skills or current role), the system:
1. Maps input to a canonical role
2. Computes feasible transitions
3. Ranks target roles based on:
   - minimal reskilling
   - strong demand
   - higher income

Outputs include:
- Recommended roles
- Missing skills
- Market justification (demand + salary signals)

## Data Sources
- Job postings (Adzuna API)
- Job descriptions (Playwright scraping)
- Statistics Canada (income, labor, wage trends)

## Project Structure
```plaintext
SkillPath-Engine/
├── data/
├── config/
├── src/
│   ├── ingest.py
│   ├── build_silver.py
│   ├── ingestion/
│   ├── transforms/
│   ├── recommender/        # transition modeling + scoring
│   └── prompts/
├── app/
│   └── streamlit_app.py    # interactive exploration
├── sql/
├── notebooks/
└── README.md
```

## How to Use
### 1. Build the data layer
```bash
python src/ingest.py --source all
python src/build_silver.py --target all
```

Update incrementally:
```bash
python src/ingest.py --source all --update
python src/build_silver.py --target all --update
```

### 2. Generate role profiles and transitions
```bash
python src/recommender/build_profiles.py
python src/recommender/build_transitions.py
```

## Notes
- Skill extraction expects a local Ollama endpoint and the configured model in `src/transforms/skills_extractor.py`
- Role mapping uses the taxonomy defined in `config/role_mapping.yaml`

## Future work
- Build recommendation layer for role transitions
- Add gold tables for market signals and transition scoring
- Expose recommendations through FastAPI
- Add a Streamlit dashboard
- Add tests, data quality checks, and extraction evaluation