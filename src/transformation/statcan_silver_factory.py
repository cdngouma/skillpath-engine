import pandas as pd
import duckdb
import re
from src.utils import occupation_to_noc
from src.utils import cip_to_noc

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

DB_PATH = "../data/warehouse.duckdb"

def _pivot_and_clean_bronze(table_name, pivot_cols=None, val_col="VALUE", keeps=[], limit=None):
    """Utility: Pivots StatCan 'Long' format to 'Wide' and standardizes casing."""
    with duckdb.connect(DB_PATH) as con:
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

def _apply_standard_noc_schema(df, rename_map):
    """Internal helper to extract NOC codes (3 and 5 digits) and normalize strings."""
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
        df["noc-5"] = df["noc_code"].apply(lambda x: x if len(x) >= 5 else None)
        
        # Create noc-3: the first 3 digits of the code
        df["noc-3"] = df["noc_code"].apply(lambda x: x[:3] if len(x) >= 3 else None)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        
    df.columns = [c.lower() for c in df.columns]
    return df

# --- Public Factory Functions ---

def build_silver_graduates(limit=None):
    return _apply_standard_noc_schema(
        _pivot_and_clean_bronze("sc_graduates_trends_raw", limit=limit),
        rename_map={
            "International Standard Classification of Education (ISCED)": "education_level",
            "Field of study": "field_of_study",
            "REF_DATE": "date",
            "VALUE": "graduates"
        }
    )

def build_silver_wage_trends(limit=None):
    df = _apply_standard_noc_schema(
        _pivot_and_clean_bronze("sc_wages_trends_raw", keeps=["REF_DATE", "National Occupational Classification (NOC)"], limit=limit),
        rename_map={
            "National Occupational Classification (NOC)": "occupation",
            "REF_DATE": "date",
            "VALUE": "weekly_wages"
        }
    )
    # 52 weeks alignment for Gold-ready annualization
    df["median_income"] = df["weekly_wages"] * 52
    return df

def build_silver_census_income(limit=None):
    """Produces high-resolution income snapshot (Reference Year 2020)."""
    return _apply_standard_noc_schema(
        _pivot_and_clean_bronze("sc_census_income_raw", limit=limit),
        rename_map={
            "Occupation - Unit group - National Occupational Classification (NOC) 2021 (821A)": "occupation",
            "Highest certificate, diploma or degree (16)": "education_level",
            "REF_DATE": "date",
            "VALUE": "median_income"
        }
    )

def build_silver_census_employment(limit=None):
    pivot_col = "labour force status (3)"
    df = _apply_standard_noc_schema(
        _pivot_and_clean_bronze("sc_census_labour_raw", pivot_cols=[pivot_col], limit=limit),
        rename_map={
            "Occupation - Unit group - National Occupational Classification (NOC) 2021 (821A)": "occupation",
            "Highest certificate, diploma or degree (16)": "education_level",
            "REF_DATE": "date",
            "Employed": "employed",
            "Unemployed": "unemployed"
        }
    )
    df = df.drop(columns="total - labour force status")
    df["labour_force"] = df["employed"] + df["unemployed"]
    df["unemployment_rate"] = (df["unemployed"] / df["labour_force"]).mul(100).round(2)
    return df

def build_silver_labour_force_trends(limit=None):
    pivot_col = "labour force characteristics"
    return _apply_standard_noc_schema(
        _pivot_and_clean_bronze(
            "sc_labour_trends_raw",
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
