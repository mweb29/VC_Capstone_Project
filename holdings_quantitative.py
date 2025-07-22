"""
This module generates synthetic financial performance metrics for a set of 
portfolio companies. It is designed to simulate realistic investment outcomes 
for venture capital or private equity portfolios and will serve as the 
underlying data used for fund-level performance rollups.

Each portfolio company is simulated with:
- An investment date and investment amount
- A series of (optional) cash distributions to the investor (realizations)
- A final valuation representing unrealized value (NAV)

Using these synthetic cash flows and valuations, we compute key VC/PE 
performance metrics:
- IRR (Internal Rate of Return)
- MOIC (Multiple on Invested Capital)
- DPI (Distributions to Paid-In)
- TVPI (Total Value to Paid-In)
- NAV (Net Asset Value)

This module is intended to connect with a broader data generation pipeline 
where general information (e.g., sector, region, currency) is handled 
separately. Here, we focus solely on the financials.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random


# ----------------------------
# Helper Functions
# ----------------------------
def generate_investment_date(start_year=2015, end_year=2022):
    """Randomly generate an investment date between start_year and end_year."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_distributions(investment_date, total_investment, max_years=10):
    """
    Generate synthetic distributions after investment_date.
    Total distributions should not exceed 2x investment (to keep MOIC reasonable).
    """
    num_distributions = random.randint(0, 4)
    distributions = []
    amounts = []
    for _ in range(num_distributions):
        days_offset = random.randint(180, max_years * 365)
        dist_date = investment_date + timedelta(days=days_offset)
        dist_amt = round(random.uniform(0.1, 0.6) * total_investment, 2)
        distributions.append(dist_date)
        amounts.append(dist_amt)
    return distributions, amounts


def generate_nav(investment_date, distributions, amounts, total_investment):
    """Estimate NAV as residual unrealized value, keeping MOIC realistic."""
    max_nav = total_investment * 2 - sum(amounts)
    nav = round(random.uniform(0, max_nav), 2)
    if distributions:
     latest_date = max(distributions)
    else:
        latest_date = investment_date + timedelta(days=5 * 365)
    return nav, latest_date


def compute_irr(cash_flows):
    """Compute IRR given a list of (amount, date) cash flows. Returns None if invalid."""
    try:
        if not cash_flows or len(cash_flows) < 2:
            return None
        amounts, dates = zip(*cash_flows)
        days = np.array([(d - dates[0]).days for d in dates])
        years = days / 365.25
        return round(np.irr(amounts), 4)
    except:
        return None


def build_company_record(company_id):
    """
    Generate full financial record for a single portfolio company.
    Returns a dictionary containing all performance metrics and synthetic inputs.
    """
    investment_date = generate_investment_date()
    investment_amount = round(random.uniform(1_000_000, 10_000_000), 2)
    dist_dates, dist_amounts = generate_distributions(investment_date, investment_amount)
    nav, val_date = generate_nav(investment_date, dist_dates, dist_amounts, investment_amount)

    # Create cash flow list
    cash_flows = [( -investment_amount, investment_date )]
    cash_flows += list(zip(dist_amounts, dist_dates))
    if nav > 0:
        cash_flows.append((nav, val_date))

    irr = compute_irr(cash_flows)
    total_dist = sum(dist_amounts)
    moic = round((total_dist + nav) / investment_amount, 4)
    dpi = round(total_dist / investment_amount, 4)
    tvpi = round((total_dist + nav) / investment_amount, 4)

    return {
        "company_id": company_id,
        "investment_date": investment_date.date(),
        "investment_amount": investment_amount,
        "distribution_dates": dist_dates,
        "distribution_amounts": dist_amounts,
        "valuation_date": val_date.date(),
        "current_nav": nav,
        "irr": irr,
        "moic": moic,
        "dpi": dpi,
        "tvpi": tvpi
    }


def generate_portfolio_company_financials(n=100):
    """
    Generate synthetic financials for a batch of portfolio companies.

    Parameters:
    n (int): Number of companies to generate

    Returns:
    pd.DataFrame: Tabular output with one row per company
    """

    # This is where we need to make the connection to the holdings.py file
    # so that the company names are comning over correctly

    records = [build_company_record(f"PC{i:04d}") for i in range(1, n + 1)]
    return pd.DataFrame(records)


# Example usage
if __name__ == "__main__":
    df = generate_portfolio_company_financials(100)
    print(df.head())