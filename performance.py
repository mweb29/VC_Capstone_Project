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
import importlib
importlib.import_module("ace_tools")

# Load the newly uploaded corrected files
holdings_df = pd.read_csv("holdings.csv")
metrics_df = pd.read_csv("holdings_metrics.csv")

# Merge using TICKER (holdings) <-> company_id (metrics)
df = pd.merge(metrics_df, holdings_df[['TICKER', 'PORTFOLIOCODE']], left_on='company_id', right_on='TICKER', how='left')

# Rename and convert fields as needed
df.rename(columns={
    'irr': 'IRR',
    'moic': 'MOIC',
    'tvpi': 'TVPI',
    'dpi': 'DPI',
    'current_nav': 'NAV',
    'investment_amount': 'CASHINVESTED',
    'distribution_amounts': 'CASHDISTRIBUTED'
}, inplace=True)

# Ensure numeric columns are float
numeric_cols = ['MOIC', 'IRR', 'TVPI', 'DPI', 'CASHINVESTED', 'CASHDISTRIBUTED', 'NAV']
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

# Aggregate to portfolio level
portfolio_perf = df.groupby('PORTFOLIOCODE').agg({
    'CASHINVESTED': 'sum',
    'CASHDISTRIBUTED': 'sum',
    'NAV': 'sum'
}).reset_index()

# Derived performance metrics
portfolio_perf['TVPI'] = (portfolio_perf['CASHDISTRIBUTED'] + portfolio_perf['NAV']) / portfolio_perf['CASHINVESTED']
portfolio_perf['DPI'] = portfolio_perf['CASHDISTRIBUTED'] / portfolio_perf['CASHINVESTED']
portfolio_perf['RVPI'] = portfolio_perf['NAV'] / portfolio_perf['CASHINVESTED']

# Weighted IRR and MOIC
df['WEIGHT'] = df['CASHINVESTED'] / df.groupby('PORTFOLIOCODE')['CASHINVESTED'].transform('sum')
weighted_perf = df.groupby('PORTFOLIOCODE').apply(
    lambda x: pd.Series({
        'IRR': np.average(x['IRR'], weights=x['WEIGHT']),
        'MOIC': np.average(x['MOIC'], weights=x['WEIGHT'])
    })
).reset_index()

# Final merge
final_perf = pd.merge(portfolio_perf, weighted_perf, on='PORTFOLIOCODE', how='left')

import ace_tools as tools; tools.display_dataframe_to_user(name="Final Portfolio Performance (Updated Inputs)", dataframe=final_perf)

