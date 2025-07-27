# price last month, price this month --> performance metrics
# do this for each company in the portfolio and aggregate the results

# could just say that the majority of the portfolio companies increased in value
# so we select some random value to show that increase

"""
This script computes aggregated portfolio-level performance metrics by combining:
1. holdings.csv: Synthetic holdings for each company including portfolio affiliation
2. holdings_metrics.csv: Performance metrics (IRR, MOIC, DPI, etc.) at company level

Output:
A DataFrame containing portfolio-level IRR, MOIC, TVPI, DPI, RVPI, NAV, and total cash flows.

Assumptions:
- Each COMPANYID exists in both files
- Holdings include a 'PORTFOLIOCODE' field
- All metrics are numeric and clean
"""

import pandas as pd
import numpy as np

def compute_portfolio_performance(holdings_path='holdings.csv', metrics_path='holdings_metrics.csv'):
    # Load data
    holdings_df = pd.read_csv(holdings_path)
    metrics_df = pd.read_csv(metrics_path)

    # Merge to align company performance with portfolio mapping
    df = pd.merge(metrics_df, holdings_df[['COMPANYID', 'PORTFOLIOCODE']], on='COMPANYID', how='left')

    # Ensure numeric columns
    numeric_cols = ['MOIC', 'IRR', 'TVPI', 'DPI', 'RVPI', 'CASHINVESTED', 'CASHDISTRIBUTED', 'NAV']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # Aggregate core values
    portfolio_perf = df.groupby('PORTFOLIOCODE').agg({
        'CASHINVESTED': 'sum',
        'CASHDISTRIBUTED': 'sum',
        'NAV': 'sum'
    }).reset_index()

    # Derived performance ratios
    portfolio_perf['TVPI'] = (portfolio_perf['CASHDISTRIBUTED'] + portfolio_perf['NAV']) / portfolio_perf['CASHINVESTED']
    portfolio_perf['DPI'] = portfolio_perf['CASHDISTRIBUTED'] / portfolio_perf['CASHINVESTED']
    portfolio_perf['RVPI'] = portfolio_perf['NAV'] / portfolio_perf['CASHINVESTED']

    # Weighted IRR and MOIC by invested capital
    df['WEIGHT'] = df['CASHINVESTED'] / df.groupby('PORTFOLIOCODE')['CASHINVESTED'].transform('sum')
    weighted_perf = df.groupby('PORTFOLIOCODE').apply(
        lambda x: pd.Series({
            'IRR': np.average(x['IRR'], weights=x['WEIGHT']),
            'MOIC': np.average(x['MOIC'], weights=x['WEIGHT'])
        })
    ).reset_index()

    # Merge final result
    final_perf = pd.merge(portfolio_perf, weighted_perf, on='PORTFOLIOCODE', how='left')
    return final_perf

# Example usage:
if __name__ == '__main__':
    summary = compute_portfolio_performance()
    print(summary.head())
