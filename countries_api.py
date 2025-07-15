"""
Description:
    This script fetches global country metadata from the open-source 
    `mledoze/countries` GitHub repository and parses relevant attributes for
    financial and investor modeling. Extracted fields include country name,
    ISO 2-character code, region, subregion, and primary currency code. The 
    processed data is displayed as a Pandas DataFrame and optionally saved to
    a local JSON file for later use in synthetic data generation or Snowflake
    integration.

    This utility is intended for early-stage data validation and inspection; 
    it is not designed for use in production pipelines. Placeholder functions
    are included for eventual Snowflake ingestion.

Functions:
    - fetch_countries_data(): Retrieves and parses country data from the
    external JSON source.
    - json_output(df, filename): Saves a DataFrame to a local JSON file for
    inspection or reuse.
    - connect_to_snowflake(): Placeholder for establishing a connection to
    Snowflake.
    - upload_to_snowflake_country(df): Placeholder for uploading the country
    data into Snowflake.

Output:
    - Displays a DataFrame of global country metadata.
    - Optionally exports the metadata to 'synthetic_countries.json'.

Example:
    Run this script once to generate the currency cache file:
        $ python countries_api.py
"""

import requests
import json
import pandas as pd

# Fetch data
def fetch_countries_data():
    """Fetches country data from the mledoze/countries GitHub repository."""
    url = "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Parse relevant fields
    countries = []
    for item in data:
        countries.append({
            "Country Name": item["name"]["common"],
            "ISO2": item["cca2"],
            "Region": item["region"],
            "Subregion": item["subregion"],
            "Currency": list(item["currencies"].keys())[0] if item.get("currencies") else None
        })

    # Display as table using Pandas
    df = pd.DataFrame(countries)
    pd.set_option('display.max_rows', 250)
    return df

# (Optional) Save to JSON file
def json_output(df, filename=None):
    """
    Saves a pandas DataFrame to a JSON file.
    Parameters:
    - df: pandas DataFrame to save
    - filename: Optional name of the output JSON file. 
                If None, uses 'output.json'.
    """
    if filename is None:
        filename = "output.json"  # fallback default name

    with open(filename, "w") as f:
        json.dump(df.to_dict(orient="records"), f, indent=2)

    print(f"JSON file saved as {filename}")


# -------------------------------
# Snowflake Integration Placeholders (Country)
# -------------------------------

def connect_to_snowflake():
    """
    Placeholder function for establishing connection to Snowflake.
    To be completed by a data engineer or used in final deployment phase.
    """
    # Example (not implemented):
    # import snowflake.connector
    # conn = snowflake.connector.connect(
    #     user='your_user',
    #     password='your_password',
    #     account='your_account_id',
    #     ...
    # )
    # return conn
    pass

def upload_to_snowflake_country(df):
    """
    Placeholder function for uploading country data to Snowflake.
    Parameters:
        df (pandas.DataFrame): Country metadata (name, ISO2, region, etc.)
    Expected target: DIM_COUNTRY table in Snowflake.
    """
    print(f"[Placeholder] Uploading {len(df)} country records to Snowflake (DIM_COUNTRY)...")
    pass


# Main execution
if __name__ == "__main__":
    """Main function to fetch and display country data.
    We will not use this at the end of the project. This is solely to make 
    sure that our output is correct and we are getting the results that we 
    expect. It allows us to validate our work before integrating it into the
    main project.
    """
    countries_df = fetch_countries_data()
    json_output(countries_df, "synthetic_countries.json")