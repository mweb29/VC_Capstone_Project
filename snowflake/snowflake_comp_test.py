"""
Snowflake table creation and append utilities for CSV/DataFrame ingestion.

This module provides:
- get_session(): create a Snowpark session from .env variables
- create_table_from_df(session, df, table_name, replace=False): create table using df schema
- append_df_to_table(session, df, table_name): append rows; fill missing cols with NULL; drop extras

Conventions:
- Column names are normalized to UPPERCASE when writing to Snowflake
- VARCHAR is used for pandas 'object'/'string' dtypes
- TIMESTAMP_NTZ is used for datetime64[ns]

Environment Variables required:
SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, (optional) SNOWFLAKE_ROLE
"""

import os
from typing import List, Dict
import pandas as pd
from dotenv import load_dotenv
from snowflake.snowpark import Session

# ---------------- Connection ----------------
def get_session() -> Session:
    load_dotenv()
    cfg = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }
    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        cfg["role"] = role
    missing = [k for k, v in cfg.items() if v in (None, "")]
    if missing:
        raise ValueError(f"Missing Snowflake env vars: {', '.join(missing)}")
    return Session.builder.configs(cfg).create()

# --------------- DDL helpers ----------------
_PANDAS_TO_SF = {
    "int64": "NUMBER",
    "Int64": "NUMBER",        # pandas nullable int
    "float64": "FLOAT",
    "boolean": "BOOLEAN",     # pandas nullable bool
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP_NTZ",
    "object": "VARCHAR",
    "string": "VARCHAR",
}

def _infer_sf_type(dtype) -> str:
    return _PANDAS_TO_SF.get(str(dtype), "VARCHAR")

def _build_create_table_sql(df: pd.DataFrame, table_name: str, replace: bool) -> str:
    if df.empty:
        raise ValueError("DataFrame is empty; cannot infer schema.")
    cols_sql = []
    for col, dt in df.dtypes.items():
        sf_type = _infer_sf_type(dt)
        cols_sql.append(f'"{col.upper()}" {sf_type}')
    action = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    sep = ",\n  "
    cols_block = sep.join(cols_sql)
    return f"{action} {table_name} (\n  {cols_block}\n);"

def _get_table_columns(session: Session, table_name: str) -> List[str]:
    # Robust access for Snowpark Row
    rows = session.sql(f'DESCRIBE TABLE {table_name}').collect()
    cols = []
    for r in rows:
        # Try dict-style access first, then tuple fallback
        kind = getattr(r, "kind", None)
        name = getattr(r, "name", None)
        if kind is None and hasattr(r, "__getitem__"):
            # typical order: name, type, kind, null?, default, ...
            name = r[0]
            # 'kind' usually at index 4, but depends on Snowflake version
            kind = r[4] if len(r) > 4 else None
        if str(kind).upper() == "COLUMN":
            cols.append(str(name).upper())
    if not cols:
        raise ValueError(f"Could not read columns for table {table_name}.")
    return cols

# --------------- Public API -----------------
def create_table_from_df(session: Session, df: pd.DataFrame, table_name: str, replace: bool = False) -> None:
    ddl = _build_create_table_sql(df, table_name, replace)
    session.sql(ddl).collect()
    print(f"Table ready: {table_name} (replace={replace})")

def append_df_to_table(session: Session, df: pd.DataFrame, table_name: str) -> None:
    """
    Append df to table. If table does not exist, create it.
    - Missing columns in df -> filled with NULL
    - Extra df columns -> dropped
    - Column order -> aligned to table
    """
    if df is None or df.empty:
        print("Nothing to append: DataFrame is empty.")
        return

    # Try to read target schema; if not present, create it first
    try:
        table_cols = _get_table_columns(session, table_name)
        created = False
    except Exception:
        # Auto-create using df's schema
        create_table_from_df(session, df, table_name, replace=False)
        table_cols = _get_table_columns(session, table_name)
        created = True

    # Map current df columns to uppercase
    df_cols_upper: Dict[str, str] = {c.upper(): c for c in df.columns}

    # Add any missing table columns as None
    for col in table_cols:
        if col not in df_cols_upper:
            df[col] = None
            df_cols_upper[col] = col  # now present

    # Keep only table columns and in correct order
    ordered = [df_cols_upper[c] for c in table_cols]
    df_aligned = df[ordered].copy()
    df_aligned.columns = [c.upper() for c in df_aligned.columns]

    # Append
    session.create_dataframe(df_aligned).write.mode("append").save_as_table(table_name)
    print(f"{'Created and ' if created else ''}appended {len(df_aligned)} rows to {table_name}.")


if __name__ == "__main__":
    session = get_session()
    df = pd.read_csv("CSVs/df_benchmark_account_association.csv")

    # Create new table if needed
    create_table_from_df(session, df, "BENCHMARK_ACCOUNT_ASSOCIATION")

    # Append new data (missing columns will be NULL)
    # append_df_to_table(session, df, "PORTFOLIO_GENERAL_INFO")
    session.close()