# # Account
# ### LP Account Generation & Enrichment Logic
#
# This section simulates a diverse set of Limited Partner (LP) accounts, combining **institutional investors** (e.g., pensions, endowments) and **individual investors**. The output table `accounts_df` supports capital analysis, account-level reporting, and fund allocation logic.
#
# ### Account Types
# - **Institutional LPs**: 25 named entities (e.g., CalPERS, CDPQ), each committing to 1–2 funds
# - **Individual LPs**: 25 synthetic accounts using Faker, each committing to exactly 1 fund
#
# ---
#
# ### Processing Steps
#
# | Step | Description |
# |------|-------------|
# | 1    | Define institutional LPs with LP type and country |
# | 2    | Retrieve currency code, name, and FX rate using API |
# | 3    | Generate committed capital based on LP type and number of fund commitments |
# | 4    | Assign NAV using a random multiplier of committed capital |
# | 5    | Set account start date using number of funds as proxy for tenure |
# | 6    | Generate 25 additional individual LPs with fake names and country-based logic |
# | 7    | Final combined output: `accounts_df` with 50 total LP accounts |
#
# ---
#
# ### Output Schema (`accounts_df`)
#
# | Column | Description |
# |--------|-------------|
# | Account ID | Unique account key (e.g., ACC0001) |
# | Investor Type | Institutional or Individual |
# | Account Name | Name of LP (e.g., "CalPERS", "John Smith") |
# | Type | Subcategory (e.g., Pension Fund, Endowment) |
# | Country | Country of registration |
# | Account Currency | 3-letter currency code (USD, GBP, etc.) |
# | FX to USD | Exchange rate to USD |
# | Committed Capital (Local / USD) | Fund commitment values |
# | NAV (USD) | Current net asset value |
# | Number of Funds | Number of VC funds this LP is invested in |
# | Start Date | Inferred participation start date |

!pip install faker

import pandas as pd
import random
import requests
from faker import Faker
from datetime import datetime

fake = Faker()

# 1. LP Firm Data (Institutional LPs)
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

def get_currency_info(country):
    try:
        if country == "Unknown":
            return "USD", "US Dollar", 1.0

        # Get currency code from restcountries
        r = requests.get(f"https://restcountries.com/v3.1/name/{country}?fullText=true")
        r.raise_for_status()
        data = r.json()[0]
        currency_code = list(data["currencies"].keys())[0]
        currency_name = data["currencies"][currency_code]["name"]

        # Check cache
        if currency_code in currency_cache:
            return currency_code, currency_name, currency_cache[currency_code]

        # Use Frankfurter API
        fx_url = f"https://api.frankfurter.app/latest?from={currency_code}&to=USD"
        fx_res = requests.get(fx_url)
        fx_res.raise_for_status()
        fx_data = fx_res.json()
        fx_to_usd = round(fx_data["rates"]["USD"], 4)

        currency_cache[currency_code] = fx_to_usd
        return currency_code, currency_name, fx_to_usd

    except Exception as e:
        print(f"[WARNING] Failed to get FX for {country}: {e}")
        return "USD", "US Dollar", 1.0



# 3. Generate Institutional Accounts
institutional_accounts = []
for i, (name, lp_type, country) in enumerate(lp_data):
    currency_code, currency_name, fx = get_currency_info(country)
    num_funds = random.randint(1, 2)

    if "Pension" in lp_type:
        base_amt = random.uniform(120, 300)
    elif "Endowment" in lp_type or "Government" in lp_type:
        base_amt = random.uniform(70, 200)
    elif "Foundation" in lp_type or "Fund of Funds" in lp_type:
        base_amt = random.uniform(40, 120)
    else:
        base_amt = random.uniform(60, 150)

    local_amt = round(base_amt * (1 + num_funds / 100), 2)
    committed_usd = round(local_amt * fx, 2)
    nav_usd = round(random.uniform(0.5, 1.1) * committed_usd, 2)

    year = random.randint(2010, 2022)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    start_date = f"{year}-{month:02d}-{day:02d}"

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
        "Committed Capital (USD)": committed_usd,
        "Number of Funds": num_funds,
        "NAV (USD)": nav_usd,
        "Start Date": start_date
    })

# 4. Generate Individual Accounts
countries = ["United States", "United Kingdom", "Germany", "France", "Canada", "Australia", "Netherlands", "Japan", "India", "Brazil"]
individual_accounts = []

for i in range(25):
    name = fake.name()
    country = random.choice(countries)
    currency_code, currency_name, fx = get_currency_info(country)
    num_funds = 1

    if country in ["United States", "United Kingdom", "Germany", "Canada"]:
        base_amt = random.uniform(60, 150)
    else:
        base_amt = random.uniform(20, 80)

    local_amt = round(base_amt * (1 + num_funds / 10), 2)
    committed_usd = round(local_amt * fx, 2)
    nav_usd = round(random.uniform(0.5, 1.1) * committed_usd, 2)

    year = random.randint(2010, 2022)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    start_date = f"{year}-{month:02d}-{day:02d}"

    individual_accounts.append({
        "Account ID": f"ACC{i+len(institutional_accounts)+1:04}",
        "Investor Type": "Individual",
        "Account Name": name,
        "Type": "Private Individual",
        "Country": country,
        "Account Currency": currency_code,
        "Currency Name": currency_name,
        "FX to USD": fx,
        "Committed Capital (Local)": local_amt,
        "Committed Capital (USD)": committed_usd,
        "Number of Funds": num_funds,
        "NAV (USD)": nav_usd,
        "Start Date": start_date
    })

# 5. Combine both into one DataFrame
accounts_df = pd.DataFrame(institutional_accounts + individual_accounts)
accounts_df


if __name__ == "__main__":
    # Generate LP account data and export to CSV
    accounts_df = pd.DataFrame(institutional_accounts + individual_accounts)
    accounts_df.to_csv("accounts.csv", index=False)
