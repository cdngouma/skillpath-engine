import pandas as pd
import duckdb
import re
from src.transforms.role_mapper import map_roles
from src.noc_mapping import occupation_to_noc, cip_to_noc
from src.transforms.skills_extractor import extract_tech_skills
from src.transforms.skills_section_extractor import extract_skills_section

ROLE_MAPPING_PATH = "../config/role_mapping.yaml"

# Standardizing categorical labels across all StatCan sources
EDUCATION_MAP = {
    "Bachelor's or equivalent": "Bachelor's degree",
    "Master's or equivalent": "Master's degree",
    "Doctoral or equivalent": "Doctoral degree",
    "Earned doctorate": "Doctoral degree",
    "College, CEGEP or other non-university certificate or diploma": "CEGEP/College",
    "Postsecondary certificate or diploma below bachelor level": "CEGEP/College",
    "Computer and information sciences and support services": "Computer science",
    "Mathematics and statistics": "Mathematics and statistics"
}

TABLE_NAMES = {
    # StatCan table names
    "census_income": "sc_census_income_raw",
    "census_labour": "sc_census_labour_raw",
    "wage_trends": "sc_wages_trends_raw",
    "labour_trends": "sc_labour_trends_raw",
    "graduates": "sc_graduates_trends_raw",
    # Job postings
    "job_postings": "job_postings_raw"
}

DB_PATH = "../data/warehouse.duckdb"

def _pivot_and_clean_bronze(table_name, pivot_cols=None, val_col="VALUE", keeps=None, limit=None):
    with duckdb.connect(DB_PATH) as con:
        keeps = keeps or []
        query = f"SELECT * FROM bronze.{table_name}" + (f" LIMIT {limit}" if limit else "")
        df = con.execute(query).df()

        keeps = keeps + ["ingested_at", "source"]
        
        # Case-insensitive protection
        df.columns = [c.lower() for c in df.columns]
        val_col = val_col.lower()
        protected = [k.lower() for k in keeps] + ([p.lower() for p in pivot_cols] if pivot_cols else [])

        # Remove columns that provide zero information (constant values)
        constant_cols = [col for col in df.columns if df[col].nunique() <= 1 and col not in protected]
        df = df.drop(columns=constant_cols)

        if pivot_cols:
            pivot_cols = [p.lower() for p in pivot_cols]
            pivot_idx = [c for c in df.columns if c not in pivot_cols and c != val_col]
            df_wide = df.pivot(index=pivot_idx, columns=pivot_cols, values=val_col).reset_index()
            df_wide.columns.name = None
            return df_wide
        return df

def _apply_standard_noc_schema(df, rename_map=None):
    if rename_map:
        # Standardize column names based on the specific table's map
        df = df.rename(columns={k.lower(): v for k, v in rename_map.items()})
    
    df = df.replace(EDUCATION_MAP)

    if "occupation" in df.columns:
        # Clean the title string
        df["occupation"] = df["occupation"].str.replace(r"^[0-9\s\-\[\]]+", "", regex=True).str.strip()
        df["noc_code"] = df["occupation"].apply(occupation_to_noc)
    elif "field_of_study" in df.columns:
        df["noc_code"] = df["field_of_study"].apply(cip_to_noc)

    if "noc_code" in df.columns:
        # Generate granular NOC columns
        # Ensure noc_code is a string for slicing
        df["noc_code"] = df["noc_code"].astype(str)
        
        # Create noc-5: only if the code is at least 5 digits long
        df["noc_5"] = df["noc_code"].apply(lambda x: x if len(x) >= 5 else None)
        
        # Create noc-3: the first 3 digits of the code
        df["noc_3"] = df["noc_code"].apply(lambda x: x[:3] if len(x) >= 3 else None)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        
    df.columns = [c.lower() for c in df.columns]
    return df

# --- Public Factory Functions ---

def transform_graduates(limit=None):
    return _apply_standard_noc_schema(
        _pivot_and_clean_bronze(TABLE_NAMES["graduates"], limit=limit),
        rename_map={
            "International Standard Classification of Education (ISCED)": "education_level",
            "Field of study": "field_of_study",
            "REF_DATE": "date",
            "VALUE": "graduates"
        }
    )

def transform_income_trends(limit=None):
    df = _apply_standard_noc_schema(
        _pivot_and_clean_bronze(TABLE_NAMES["wage_trends"], keeps=["REF_DATE", "National Occupational Classification (NOC)"], limit=limit),
        rename_map={
            "National Occupational Classification (NOC)": "occupation",
            "REF_DATE": "date",
            "VALUE": "weekly_wages"
        }
    )
    # 52 weeks alignment for Gold-ready annualization
    df["median_income"] = df["weekly_wages"] * 52
    return df

def transform_census_income(limit=None):
    return _apply_standard_noc_schema(
        _pivot_and_clean_bronze(TABLE_NAMES["census_income"], limit=limit),
        rename_map={
            "Occupation - Unit group - National Occupational Classification (NOC) 2021 (821A)": "occupation",
            "Highest certificate, diploma or degree (16)": "education_level",
            "REF_DATE": "date",
            "VALUE": "median_income"
        }
    )

def transform_census_labour(limit=None):
    pivot_col = "labour force status (3)"
    df = _apply_standard_noc_schema(
        _pivot_and_clean_bronze(TABLE_NAMES["census_labour"], pivot_cols=[pivot_col], limit=limit),
        rename_map={
            "Occupation - Unit group - National Occupational Classification (NOC) 2021 (821A)": "occupation",
            "Highest certificate, diploma or degree (16)": "education_level",
            "REF_DATE": "date",
            "Employed": "employed",
            "Unemployed": "unemployed"
        }
    )
    df = df.drop(columns="total - labour force status",errors="ignore")
    df["labour_force"] = df["employed"] + df["unemployed"]
    df["unemployment_rate"] = (df["unemployed"] / df["labour_force"]).mul(100).round(2)
    return df

def transform_labour_trends(limit=None):
    pivot_col = "labour force characteristics"
    return _apply_standard_noc_schema(
        _pivot_and_clean_bronze(
            TABLE_NAMES["labour_trends"],
            keeps=["REF_DATE", "National Occupational Classification (NOC)"],
            pivot_cols=[pivot_col], 
            limit=limit
        ),
        rename_map={
            "National Occupational Classification (NOC)": "occupation",
            "Labour force": "labour_force",
            "Unemployment rate": "unemployment_rate",
            "Employment": "employed",
            "REF_DATE": "date",
        }
    )

def transform_job_roles(threshold=71, limit=None):
    # Load raw job postings
    with duckdb.connect(DB_PATH) as con:
        query = f"""
            SELECT job_hash,
                   search_term,
                   REPLACE(CAST(raw_payload->'title' AS VARCHAR), '"', '') AS job_title,
                   REPLACE(CAST(raw_payload->'company'->'display_name' AS VARCHAR), '\"', '') as company,
                   description,
                   ingested_at,
                   source
            FROM bronze.{TABLE_NAMES['job_postings']}
            WHERE COALESCE(REPLACE(CAST(raw_payload->'company'->'display_name' AS VARCHAR), '"', ''), '') <> ''
        """ + (f" LIMIT {limit}" if limit else "")
        df = con.execute(query).df()
        df = map_roles(df, yaml_path=ROLE_MAPPING_PATH, full_report=False)
        df = df.rename(columns={"matched_noc": "noc_code"})

        if threshold is not None:
            # Filter out low confidence jobs
            df = df[df["confidence_score"] >= threshold].copy()
            
        return _apply_standard_noc_schema(df)

def transform_job_skills(limit=None):
    # Load raw job description
    with duckdb.connect(DB_PATH) as con:
        query = f"""
            SELECT job_hash, description
            FROM bronze.{TABLE_NAMES['job_postings']}
            WHERE description IS NOT NULL
        """ + (f" LIMIT {limit}" if limit else "")

        jobs = con.execute(query).df()

        rows = []

        for _, row in jobs.iterrows():
            job_hash = row['job_hash']
            description = row['description']

            if not description:
                continue

            skills_section = extract_skills_section(description)
            data = extract_tech_skills(skills_section)

            for skill in data.technical_skills:
                rows.append({
                    "job_hash": job_hash,
                    "skill_name": skill.skill_name,
                    "skill_category": skill.skill_category
                })
        
        return pd.DataFrame(rows)
