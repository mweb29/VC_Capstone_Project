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
import numpy_financial as npf
import pandas as pd
from datetime import datetime, timedelta
import random

def generate_distributions(investment_date, total_investment, max_years=7):
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
        return round(npf.irr(amounts), 4)
    except:
        return None


def build_company_record(company_id):
    """
    Generate full financial record for a single portfolio company.
    Returns a dictionary containing all performance metrics and synthetic inputs.
    """
    # Generate an investment date from the last 7 years like holdings.pys
    investment_date = datetime.today().date() - timedelta(days=random.randint(0, 365*7))
    
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
        "TICKER": company_id,
        "INVESTMENT_DATE": investment_date,
        "INVESTMENT_AMOUNT": investment_amount,
        "DISTRIBUTION_DATES": dist_dates,
        "DISTRIBUTION_AMOUNTS": dist_amounts,
        "VALUATION_DATE": val_date,
        "CURRENT_NAV": nav,
        "IRR": irr,
        "MOIC": moic,
        "DPI": dpi,
        "TVPI": tvpi
        #"history_date": # history
    }

def validate_performance(df):
    """
        Checks that MOIC, DPI, and TVPI values in the DataFrame are consistent with
        the investment amount, distribution amounts, and current NAV.

        For each row, it recalculates the metrics and compares them to what's stored.
        Raises an error if any values are significantly off.

        Parameters:
        df : pandas.DataFrame
            Must include columns: 'investment_amount', 'distribution_amounts',
            'current_nav', 'moic', 'dpi', and 'tvpi'.
    """
    for i, row in df.iterrows():
        investment = row['INVESTMENT_AMOUNT']
        nav = row['CURRENT_NAV']
        distributions = sum(row['DISTRIBUTION_AMOUNTS'])

        # Recalculate metrics
        expected_moic = round((distributions + nav) / investment, 4)
        expected_dpi = round(distributions / investment, 4)
        expected_tvpi = expected_moic  # TVPI and MOIC are equivalent

        # Pull from row
        moic, dpi, tvpi = round(row['MOIC'], 4), round(row['DPI'], 4), round(row['TVPI'], 4)

        # Validate
        assert np.isclose(moic, expected_moic, atol=0.01), f"MOIC mismatch at index {i}"
        assert np.isclose(dpi, expected_dpi, atol=0.01), f"DPI mismatch at index {i}"
        assert np.isclose(tvpi, expected_tvpi, atol=0.01), f"TVPI mismatch at index {i}"
        assert np.isclose(tvpi, moic, atol=0.001), f"TVPI and MOIC should match at index {i}"

    print("All performance metrics are internally consistent!")

def generate_portfolio_company_financials(n=100):
    """
    Generate synthetic financials for a batch of portfolio companies.

    Parameters:
    n (int): Number of companies to generate

    Returns:
    pd.DataFrame: Tabular output with one row per company
    """
    # Gets the tickers from the holdings DataFrame
    tickers = holdings_df["TICKER"].unique()
    records = [build_company_record(ticker) for ticker in tickers]
    return pd.DataFrame(records)   

if __name__ == "__main__":
    # 100 has to be entered so that the company names are coming over correctly
    # from the holdings module
    holdings_df = pd.read_csv("CSVs/holdings.csv")
    metrics_df = generate_portfolio_company_financials(holdings_df)

    # Run the validation
    validate_performance(metrics_df)

    # Save to CSV for further use
    metrics_df.to_csv('CSVs/holdings_metrics.csv', index=False)
    