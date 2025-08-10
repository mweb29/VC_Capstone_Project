"""
Description:
    This script retrieves the full Global Industry Classification Standard 
    (GICS) hierarchy from a public GitHub Gist and separates it into two structures:
      1. A simplified list of unique top-level GICS sectors
      2. A full GICS hierarchy including sector, industry group, industry, and 
      sub-industry

    Both outputs are saved as JSON files (`sectors.json`, `gics.json`) for 
    reuse in downstream portfolio allocation modeling and sector-level analytics.

    The script uses shared utility functions from `countries_api.py` for JSON 
    export and Snowflake integration scaffolding, ensuring consistency across 
    the capstone pipeline.

Functions:
    - get_gics_sectors(): Downloads and parses the GICS CSV into two structured 
    DataFrames

Output:
    - `sectors.json`: List of unique GICS sector codes and names
    - `gics.json`: Full GICS hierarchy including Sector → Sub-Industry mappings

Intended Use:
    These outputs support sector allocation generation, investor reporting, and 
    metadata enrichment in synthetic VC and financial datasets. The JSON 
    outputs can be later uploaded to Snowflake's dimensional tables if desired.

Example:
    Run this script to generate GICS sector JSON artifacts:
        $ python gics_sector_extractor.py
"""

import pandas as pd
# Use the functions from countries_api.py so we do not have to rewrite functions
from APIs.countries_api import json_output

def get_gics_sectors():
# Load full GICS classification from GitHub Gist
    url = "https://gist.githubusercontent.com/uknj/c9bcf66ab379a35fcc8758f9a6c86ceb/raw"
    df = pd.read_csv(url)

    # Extract unique top-level sectors
    sectors_df = df[['Sector Code', 'Sector']].drop_duplicates().sort_values('Sector Code').reset_index(drop=True)

    #print("GICS Sector List:")
    # Clean table without index
    sectors = sectors_df.reset_index(drop=True)

    # Preview full GICS classification hierarchy
    #print("\n GICS Classification:")
    gics = df

    return sectors, gics


# Main execution
if __name__ == "__main__":
    """Main function to fetch and display country data.
    We will not use this at the end of the project. This is solely to make 
    sure that our output is correct and we are getting the results that we 
    expect. It allows us to validate our work before integrating it into the
    main project.
    """
    sectors, gics = get_gics_sectors()
    json_output(sectors, "sectors.json")
    json_output(gics, "gics.json")
