"""
holdings.py

This script generates synthetic private equity or venture capital portfolio holdings
data, suitable for use in a Snowflake-backed data pipeline. Each record represents a
portfolio company held by a fund, including monetary details (cost basis, market value,
book value), classification (sector, region), and identifiers (issuer names, currency codes).

This is intended to simulate the data that would feed the HOLDINGSDETAILS table and 
can be used to benchmark or analyze fund performance at the portfolio company level.

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
    portfolio_codes = [f"FUND_{i:03d}" for i in range(1, 6)]  # Five example funds
    currencies = [("USD", "US Dollar"), ("EUR", "Euro"), ("CAD", "Canadian Dollar")]
    countries = [("US", "United States"), ("DE", "Germany"), ("IN", "India"),
                 ("CN", "China"), ("CA", "Canada")]
    regions = ["North America", "Europe", "Asia-Pacific"]
    sectors = ["Tech", "Healthcare", "CleanTech", "AI", "Fintech"]

    records = []

    for _ in range(n):
        # Random fund assignment
        portfolio_code = random.choice(portfolio_codes)

        # Currency assignment (code + name)
        currency_code, currency_name = random.choice(currencies)

        # Generate a fake company name
        issuername = fake.company()

        # Generate monetary metrics
        costbasis = round(np.random.uniform(1e6, 1e7), 2)  # Initial investment
        quantity = round(np.random.uniform(1e4, 1e6), 2)   # Number of shares/units
        price = round((costbasis / quantity) * np.random.uniform(0.8, 1.2), 2)  # Simulated mark
        market_value = round(quantity * price, 2)          # Current value
        book_value = round(costbasis * np.random.uniform(0.9, 1.1), 2)  # Book-adjusted value
        unrealized = round(market_value - book_value, 2)   # Paper gain/loss

        # Geographic and sector metadata
        risk_country_code, risk_country = random.choice(countries)
        region = random.choice(regions)
        sector = random.choice(sectors)

        # Snapshot date (randomized within past year)
        history_date = datetime.today().date() - timedelta(days=random.randint(0, 365))

        records.append({
            "PORTFOLIOCODE": portfolio_code,
            "CURRENCYCODE": currency_code,
            "CURRENCY": currency_name,
            "ISSUERNAME": issuername,
            "ISSUENAME": issuername,
            "ISSUEDISPLAYNAME": issuername,
            "COSTBASIS": costbasis,
            "QUANTITY": quantity,
            "MARKETVALUE": market_value,
            "BOOKVALUE": book_value,
            "UNREALIZEDGAINSLOSSES": unrealized,
            "PRICE": price,
            "ASSETCLASSNAME": "Private Equity",
            "PRIMARYSECTORNAME": sector,
            "PRIMARYINDUSTRYNAME": f"{sector} Services",
            "RISKCOUNTRYCODE": risk_country_code,
            "RISKCOUNTRY": risk_country,
            "REGIONNAME": region,
            "HISTORYDATE": history_date
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
