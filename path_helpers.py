"""
path_helpers.py

Provides helper functions for resolving common project paths,
such as the CSVs directory, regardless of where the script is run from.

Usage:
    from path_helpers import get_csv_path, ensure_csvs_dir

    # Ensure CSVs folder exists
    csv_dir = ensure_csvs_dir()

    # Get path for reading
    holdings_path = get_csv_path('holdings.csv')

    # Get path for writing
    output_path = get_csv_path('product_master.csv')
"""

import os

def ensure_csvs_dir() -> str:
    """
    Ensures the CSVs directory exists in the same folder as this file
    (the project root) and returns its absolute path.
    """
    csvs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CSVs'))
    os.makedirs(csvs_dir, exist_ok=True)
    return csvs_dir

def get_csv_path(filename: str) -> str:
    """
    Returns the absolute path to a file inside the project's CSVs directory.
    """
    return os.path.join(ensure_csvs_dir(), filename)

