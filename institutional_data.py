import pandas as pd
import random
import requests
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

# 2. Currency info
def get_currency_info(country):
    try:
        if country == "Unknown":
            return "USD", "US Dollar", 1.0
        r = requests.get(f"https://restcountries.com/v3.1/name/{country}")
        r.raise_for_status()
        data = r.json()[0]
        currency_code = list(data["currencies"].keys())[0]
        currency_name = data["currencies"][currency_code]["name"]
        fx_dict = {"USD": 1.0, "EUR": 1.1, "GBP": 1.3, "CAD": 0.74, "JPY": 0.0067}
        fx_to_usd = fx_dict.get(currency_code, 1.0)
        return currency_code, currency_name, fx_to_usd
    except:
        return "USD", "US Dollar", 1.0

# 3. Institutional Accounts
institutional_accounts = []
for i, (name, lp_type, country) in enumerate(lp_data):
    currency_code, currency_name, fx = get_currency_info(country)
    num_funds = random.randint(1, 100)
    if "Pension" in lp_type:
        base_amt = random.uniform(120, 300)
    elif "Endowment" in lp_type or "Government" in lp_type:
        base_amt = random.uniform(70, 200)
    elif "Foundation" in lp_type or "Fund of Funds" in lp_type:
        base_amt = random.uniform(40, 120)
    else:
        base_amt = random.uniform(60, 150)
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

# 4. Individual Accounts
countries = ["United States", "United Kingdom", "Germany", "France", "Canada", "Australia", "Netherlands", "Japan", "India", "Brazil"]
individual_accounts = []

for i in range(25):
    name = fake.name()
    country = random.choice(countries)
    currency_code, currency_name, fx = get_currency_info(country)
    num_funds = random.randint(1, 10)
    if country in ["United States", "United Kingdom", "Germany", "Canada"]:
        base_amt = random.uniform(60, 150)
    else:
        base_amt = random.uniform(20, 80)
    local_amt = round(base_amt * (1 + num_funds / 10), 2)

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
        "Committed Capital (USD)": round(local_amt * fx, 2),
        "Number of Funds": num_funds,
        "NAV (USD)": round(random.uniform(40, 120), 2),
        "Start Date": random.choice(["2012-11-23", "2013-09-08", "2017-11-02"])
    })

# 5. Combine
accounts_df = pd.DataFrame(institutional_accounts + individual_accounts)
accounts_df