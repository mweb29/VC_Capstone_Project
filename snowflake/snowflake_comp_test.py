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
import pandas as pd
from typing import List, Dict
from dotenv import load_dotenv
from snowflake.snowpark import Session

# -------------------------- Connection --------------------------
def get_session() -> Session:
    """Return an authenticated Snowflake Snowpark session using .env config."""
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

# -------------------------- DDL Helpers --------------------------
_PANDAS_TO_SF = {
    "int64": "NUMBER",
    "Int64": "NUMBER",        # pandas nullable integer
    "float64": "FLOAT",
    "boolean": "BOOLEAN",     # pandas nullable boolean
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP_NTZ",
    "object": "VARCHAR",
    "string": "VARCHAR",
}

def _infer_sf_type(dtype: pd.api.extensions.ExtensionDtype | str) -> str:
    """Map pandas dtype to a Snowflake SQL type."""
    return _PANDAS_TO_SF.get(str(dtype), "VARCHAR")

def _build_create_table_sql(df: pd.DataFrame, table_name: str, replace: bool) -> str:
    """Build CREATE TABLE DDL from a pandas DataFrame."""
    if df.empty:
        raise ValueError("DataFrame is empty. Cannot infer schema for table creation.")

    cols_sql = []
    for col, dt in df.dtypes.items():
        sf_type = _infer_sf_type(dt)
        if sf_type == "VARCHAR":
            cols_sql.append(f'"{col.upper()}" VARCHAR')
        else:
            cols_sql.append(f'"{col.upper()}" {sf_type}')

    action = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    return f"{action} {table_name} (\n  {',\n  '.join(cols_sql)}\n);"

def _get_table_columns(session: Session, table_name: str) -> List[str]:
    """Return existing table column names (UPPERCASE, in order)."""
    rows = session.sql(f'DESCRIBE TABLE {table_name}').collect()
    return [r[0] if hasattr(r, '__getitem__') else r.name for r in rows if (getattr(r, 'kind', None) or r[4]) == 'COLUMN']

# -------------------------- Public API --------------------------
def create_table_from_df(session: Session, df: pd.DataFrame, table_name: str, replace: bool = False) -> None:
    """
    Create a Snowflake table using df's schema.

    Parameters
    ----------
    session : Session
        Active Snowpark session.
    df : pd.DataFrame
        DataFrame whose columns/types define the table schema.
    table_name : str
        Fully qualified or current-schema table name.
    replace : bool
        If True, CREATE OR REPLACE TABLE. If False, CREATE TABLE IF NOT EXISTS.
    """
    ddl = _build_create_table_sql(df, table_name, replace)
    session.sql(ddl).collect()

def append_df_to_table(session: Session, df: pd.DataFrame, table_name: str) -> None:
    """
    Append DataFrame rows to an existing Snowflake table.

    Behavior:
    - Looks up target table columns and order
    - Adds missing columns to df with None (becomes NULL in Snowflake)
    - Drops extra df columns not present in table
    - Reorders df columns to match the table prior to write
    """
    try:
        table_cols = _get_table_columns(session, table_name)
    except Exception as e:
        raise ValueError(f"Table {table_name} not found or not accessible: {e}")

    if len(df) == 0:
        return

    df_cols_upper_map: Dict[str, str] = {c.upper(): c for c in df.columns}

    for col in table_cols:
        if col not in df_cols_upper_map:
            df[col] = None
            df_cols_upper_map[col] = col

    ordered = [df_cols_upper_map[c] for c in table_cols]
    df_aligned = df[ordered].copy()
    df_aligned.columns = [c.upper() for c in df_aligned.columns]

    session.create_dataframe(df_aligned).write.mode("append").save_as_table(table_name)


if __name__ == "__main__":
    session = get_session()
    df = pd.read_csv("CSVs/portfolio_general_info.csv")

    # Create new table if needed
    create_table_from_df(session, df, "PORTFOLIO_GENERAL_INFO", replace=False)

    # Append new data (missing columns will be NULL)
    append_df_to_table(session, df, "PORTFOLIO_GENERAL_INFO")
    session.close()