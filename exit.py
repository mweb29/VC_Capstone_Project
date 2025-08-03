# #Exit
#
# ### Exit Generation Logic
#
# The exit event simulator creates `vc_exit_df` by **linking directly to actual holdings data** from `holdings_df` (e.g., generated via `holdings.py`).  
# It samples real companies held by each fund to simulate exits.
#
# ---
#
# ### Processing Steps
#
# | Step | Description |
# |------|-------------|
# | 1    | For each fund in `portfolio_general_info_df`, filter corresponding holdings from `holdings_df` |
# | 2    | Skip funds with no holdings (only active portfolios are eligible) |
# | 3    | Randomly select ~20% of funds to simulate exits |
# | 4    | Sample 1–5 existing companies from the fund's holdings to simulate exit events |
# | 5    | For each exited company: Assign exit type (IPO, Acquisition, Write-off), Generate exit date 3–9 years after fund close  
# | 6    | Construct one row per exit and append to `vc_exit_df` |
#
# ---
#
# ### Output Schema: `vc_exit_df`
#
# | Column | Description |
# |--------|-------------|
# | PORTFOLIOCODE | VC fund identifier (from portfolio) |
# | TICKER | Company ID (from holdings) |
# | COMPANY | Company name (from holdings) |
# | EXITTYPE | Type of exit (IPO, Acquisition, Write-off) |
# | ACQUIRERTYPE | Strategic or Financial Sponsor |
# | MOIC | Multiple on Invested Capital |
# | EXITVALUE_MILLION_USD | Exit proceeds in millions of USD |
# | EXITDATE | Exit completion date |
#
# ---

# Exit-Holdings Integration
# This updated exit event generator links directly to the holdings_df

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Exit settings
exit_types = ['IPO', 'Acquisition', 'Write-off']
acquirer_types = ['Strategic', 'Financial Sponsor']
today = datetime.today()

vc_exit_events = []

# Load or generate holdings_df (should already exist in pipeline)
# Example: holdings_df = pd.read_csv("synthetic_holdings.csv")
# Must contain: PORTFOLIOCODE, TICKER, ISSUENAME

for _, row in portfolio_general_info_df.iterrows():
    fund_id = row["PORTFOLIOCODE"]
    close_date = datetime.strptime(str(row["CLOSE_DATE"]), "%Y-%m-%d")

    # Filter holdings belonging to this fund
    fund_holdings = holdings_df[holdings_df["PORTFOLIOCODE"] == fund_id]

    # Skip if no holdings available
    if fund_holdings.empty:
        continue

    # ~20% of funds have exits
    if random.random() > 0.8:  # Keep ~20%
        num_exits = random.randint(1, 5)

        # Prevent sampling more exits than companies
        num_exits = min(num_exits, len(fund_holdings))

        # Randomly select companies to exit
        exited_companies = fund_holdings.sample(n=num_exits)

        for _, company_row in exited_companies.iterrows():
            company_id = company_row["TICKER"]
            company_name = company_row["ISSUENAME"]

            # Generate realistic exit date
            exit_years = int(np.random.choice(
                [3, 4, 5, 6, 7, 8, 9],
                p=[0.05, 0.1, 0.2, 0.25, 0.25, 0.1, 0.05]
            ))
            exit_date = close_date + timedelta(days=exit_years * 365)

            # Cap to today if in future
            if exit_date > today:
                exit_date = today - timedelta(days=random.randint(0, 365))

            exit_type = random.choice(exit_types)

            if exit_type == "Write-off":
                moic = 0.0
                exit_value = 0.0
            else:
                moic = round(np.random.uniform(0.5, 5.0), 2)
                exit_value = round(np.random.uniform(10, 500), 2)

            exit_event = {
                "PORTFOLIOCODE": fund_id,
                "TICKER": company_id,
                "COMPANY": company_name,
                "EXITTYPE": exit_type,
                "ACQUIRERTYPE": random.choice(acquirer_types),
                "MOIC": moic,
                "EXITVALUE_MILLION_USD": exit_value,
                "EXITDATE": exit_date.strftime("%Y-%m-%d")
            }

            vc_exit_events.append(exit_event)

# Convert to DataFrame
vc_exit_df = pd.DataFrame(vc_exit_events)

if __name__ == "__main__":
    # Simulate exit events and export to CSV
    vc_exit_df = pd.DataFrame(vc_exit_events)
    vc_exit_df.to_csv("vc_exit.csv", index=False)
