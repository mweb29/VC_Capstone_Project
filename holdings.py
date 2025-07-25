"""
By: Michael Webber
holdings.py

This script generates synthetic private equity or venture capital portfolio 
holdings data, suitable for use in a Snowflake-backed data pipeline. Each 
record represents a portfolio company held by a fund, including monetary 
details (cost basis, market value, book value), classification (sector, 
region), and identifiers (issuer names, currency codes).

This is intended to simulate the data that would feed the HOLDINGSDETAILS
table. This table will connect via the 

Assumptions:
- Funds are labeled as FUND_001 to FUND_005
- Companies are randomly generated using the Faker package
- Valuations, positions, and geographic exposures are randomized within realistic ranges
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from faker import Faker
import json
import uuid

# Initialize random number generators for reproducibility
fake = Faker()
np.random.seed(42)
random.seed(42)

def generate_holdings_data(n=100):
    """
    Generate n synthetic PE/VC holdings for simulated portfolio companies.

    Parameters:
        n (int): Number of synthetic holdings to generate.

    Returns:
        pd.DataFrame: A DataFrame representing the synthetic holdings.
    """
    # Define sample pools for random selection
    portfolio_codes = [f"FUND_{i:03d}" for i in range(1, 6)] # FUND_001 to FUND_005

    # Read in the JSON File for country metadata
    with open('JSON/synthetic_countries.json', "r") as f:
        countries_json = json.load(f)

    # This accesses the JSON --> Need to limit it to a subset of countries
    countries_regions = [
        (entry["ISO2"], entry["Country Name"], entry["Region"])
        for entry in countries_json
    ]

    # Read in the JSON File for sectors metadata
    with open('JSON/sectors.json', "r") as f:
        sectors_json = json.load(f)

    sectors = [entry["Sector"] for entry in sectors_json]

    # Read in the JSON File for currency metadata
    with open('JSON/currency_lookup.json', "r") as f:
        currency_json = json.load(f)

    # Generate synthetic holdings data
    records = []
    for _ in range(n):
        # Random fund assignment
        portfolio_code = random.choice(portfolio_codes)

        # Generate a fake company name
        issuername = fake.company()

        # Generate the company identifier
        company_code = uuid.uuid4()
        company_code = str(company_code).upper()

        # Geographic and sector metadata
        risk_country_code, risk_country, region = random.choice(countries_regions)
        sector = random.choice(sectors)
        
        # Look up currency details
        if risk_country in currency_json:
            currency_info = currency_json[risk_country]
            currency_code = currency_info["currency_code"]
            currency_name = currency_info["currency_name"]
        else:
            currency_code = "USD"  # Default to USD if not found
            currency_name = "United States Dollar"

        # Ticker column for Company_ID
        # GUID for the company generated as a unique identifier
        records.append({
            "PORTFOLIOCODE": portfolio_code,
            "CURRENCYCODE": currency_code,
            "CURRENCY": currency_name,
            "TICKER": company_code,
            "ISSUENAME": issuername,
            "ISSUEDISPLAYNAME": issuername,
            "ASSETCLASSNAME": "Private Equity",
            "PRIMARYSECTORNAME": sector,
            "PRIMARYINDUSTRYNAME": f"{sector} Services",
            "RISKCOUNTRYCODE": risk_country_code,
            "RISKCOUNTRY": risk_country,
            "REGIONNAME": region
            })

    return pd.DataFrame(records)

# Main block to allow standalone script execution
if __name__ == "__main__":
    df_holdings = generate_holdings_data(n=100)
    
    # Preview the first few rows for sanity check
    print("Preview of synthetic VC/PE holdings data:")
    print(df_holdings.head())

    # Optional: save to CSV or integrate into Snowflake pipeline
    # df_holdings.to_csv("synthetic_holdings.csv", index=False)
