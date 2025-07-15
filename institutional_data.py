"""
This script generates synthetic account-level investor data for use in a venture 
capital or financial database. It includes two investor categories—Institutional 
and Individual—with metadata such as country, currency, investor type,
committed capital, and NAV. Exchange rates are obtained from the 'restcountries'
API. 

The output is a single pandas DataFrame combining all investor accounts with 
consistent formatting.
"""

import pandas as pd
import random
import json
from faker import Faker

fake = Faker()

# 1. LP Firm Data (Institutional)
# The following LP firm data has been hardcoded based on a screenshot generated via ChatGPT,
# as a proxy for actual PitchBook export due to access limitations.

lp_data = [
    ("European Investment Fund", "Fund of Funds", "Luxembourg"),
    ("California Public Employees’ Retirement System", "Public Pension Fund", "United States"),
    ("HarbourVest Partners", "Fund of Funds", "United States"),
    ("Employees’ Retirement System of the State of Hawaii", "Corporate Pension", "United States"),
    ("University of Michigan", "Endowment", "United States"),
    ("Adams Street Partners", "Fund of Funds", "United States"),
    ("MacArthur Foundation", "Foundation", "United States"),
    ("University of Texas IMC", "Endowment", "United States"),
    ("Regents of UC", "Endowment", "United States"),
    ("SBC Master Pension", "Corporate Pension", "United States"),
    ("NYS Common Retirement Fund", "Public Pension Fund", "United States"),
    ("Lucent Pension Plan", "Corporate Pension", "United States"),
    ("Rockefeller Foundation", "Foundation", "United States"),
    ("Michigan Treasury", "Government Agency", "United States"),
    ("Engineers Local 3 Fund", "Union Pension Fund", "United States"),
    ("Knightsbridge Advisers", "Fund of Funds", "United States"),
    ("MassMutual", "Corporate Pension", "United States"),
    ("CDPQ", "Public Pension Fund", "Canada"),
    ("Mass PRIM Board", "Public Pension Fund", "United States"),
    ("SF Employees’ Retirement", "Public Pension Fund", "United States"),
    ("Michigan Retirement", "Public Pension Fund", "United States"),
    ("Sherman Fairchild Foundation", "Foundation", "United States"),
    ("HP Retirement Plan", "Corporate Pension", "United States"),
    ("Lexington Partners", "Secondary LP", "United States"),
    ("Illinois Municipal Fund", "Public Pension Fund", "United States"),
]

def get_currency_info(country, filename="currency_lookup.json"):
    """
    Retrieve currency code, name, and FX rate from cached JSON.
    """
    with open(filename, "r") as f:
        currency_cache = json.load(f)

    info = currency_cache.get(country)
    if info:
        return info["currency_code"], info["currency_name"], info["fx_to_usd"]
    else:
        return "USD", "US Dollar", 1.0

def generate_institutional_accounts():
    """
    Generate a list of dictionaries representing institutional investor accounts
    with varying types, fund sizes, and currencies.
    """
    institutional_accounts = []
    for i, (name, lp_type, country) in enumerate(lp_data):
        currency_code, currency_name, fx = get_currency_info(country)
        num_funds = random.randint(1, 100)
        
        # Assign capital size by investor type
        if "Pension" in lp_type:
            base_amt = random.uniform(120, 300)
        elif "Endowment" in lp_type or "Government" in lp_type:
            base_amt = random.uniform(70, 200)
        elif "Foundation" in lp_type or "Fund of Funds" in lp_type:
            base_amt = random.uniform(40, 120)
        else:
            base_amt = random.uniform(60, 150)

        # Add variation by number of funds
        local_amt = round(base_amt * (1 + num_funds / 100), 2)

        institutional_accounts.append({
            "Account ID": f"ACC{i+1:04}",
            "Investor Type": "Institutional",
            "Account Name": name,
            "Type": lp_type,
            "Country": country,
            "Account Currency": currency_code,
            "Currency Name": currency_name,
            "FX to USD": fx,
            "Committed Capital (Local)": local_amt,
            "Committed Capital (USD)": round(local_amt * fx, 2),
            "Number of Funds": num_funds,
            "NAV (USD)": round(random.uniform(40, 140), 2),
            "Start Date": random.choice(["2012-11-23", "2013-09-08", "2017-11-02"])
        })
    return institutional_accounts

def generate_individual_accounts(start_index=0):
    """
    Generate a list of synthetic individual investor accounts with
    names, countries, currencies, and capital contributions.
    """
    countries = ["United States", "United Kingdom", "Germany", "France", "Canada",
                 "Australia", "Netherlands", "Japan", "India", "Brazil"]
    individual_accounts = []

    for i in range(25):
        name = fake.name()
        country = random.choice(countries)
        currency_code, currency_name, fx = get_currency_info(country)
        num_funds = random.randint(1, 10)

        # Use higher capital amounts for Western countries
        if country in ["United States", "United Kingdom", "Germany", "Canada"]:
            base_amt = random.uniform(60, 150)
        else:
            base_amt = random.uniform(20, 80)

        local_amt = round(base_amt * (1 + num_funds / 10), 2)

        individual_accounts.append({
            "Account ID": f"ACC{i+start_index+1:04}",
            "Investor Type": "Individual",
            "Account Name": name,
            "Type": "Private Individual",
            "Country": country,
            "Account Currency": currency_code,
            "Currency Name": currency_name,
            "FX to USD": fx,
            "Committed Capital (Local)": local_amt,
            "Committed Capital (USD)": round(local_amt * fx, 2),
            "Number of Funds": num_funds,
            "NAV (USD)": round(random.uniform(40, 120), 2),
            "Start Date": random.choice(["2012-11-23", "2013-09-08", "2017-11-02"])
        })
    return individual_accounts

def main():
    """
    Main function to generate both institutional and individual account data
    and combine them into a single DataFrame.
    """
    institutional_accounts = generate_institutional_accounts()
    individual_accounts = generate_individual_accounts(start_index=len(institutional_accounts))
    accounts_df = pd.DataFrame(institutional_accounts + individual_accounts)
    
    # Display the combined dataframe
    print(accounts_df)

if __name__ == "__main__":
    main()