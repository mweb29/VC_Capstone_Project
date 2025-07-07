# data_generation.py

"""
Script: vc_synthetic_data_export.py

Description:
    Generates synthetic data for venture capital analytics, including:
    - Portfolio metadata
    - Portfolio attributes
    - VC exit events
    - Regional and sector allocations
    - Product master information
    - VC fund-level benchmark and return data

    Exports the structured data to an Excel workbook for validation before Snowflake integration.

Author:
    MSBA Capstone Venture Capital Team – 2025

Usage:
    Run this script to generate synthetic VC data and export to Excel.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import openpyxl

# Set seed for reproducibility
np.random.seed(42)

# --- 1. PORTFOLIO_GENERAL_INFO ---
portfolio_codes = [f'ACC{i:04d}' for i in range(1, 6)]
currencies = [('USD', 'US Dollar'), ('EUR', 'Euro'), ('GBP', 'British Pound')]

portfolio_general_info = pd.DataFrame({
    'PORTFOLIOCODE': portfolio_codes,
    'NAME': portfolio_codes,
    'BASECURRENCYCODE': [currencies[i % 3][0] for i in range(5)],
    'BASECURRENCYNAME': [currencies[i % 3][1] for i in range(5)],
    'INVESTMENTSTYLE': ['Venture Capital'] * 5,
    'ISBEGINOFDAYPERFORMANCE': [False] * 5,
    'OPENDATE': pd.date_range(end=datetime.today(), periods=5).strftime('%Y-%m-%d'),
    'PERFORMANCEINCEPTIONDATE': pd.date_range(end=datetime.today(), periods=5).strftime('%Y-%m-%d'),
    'PORTFOLIOCATEGORY': ['Individual'] * 5,
    'PRODUCTCODE': ['VCF_SIM'] * 5
})

# --- 2. PORTFOLIO_ATTRIBUTES ---
attribute_types = [
    ('Strategy', 'Early-StageVC', 'Early-Stage VC'),
    ('Region', 'NorthAmerica', 'North America'),
    ('Vehicle', 'Commingled Fund', 'Commingled Fund')
]

portfolio_attributes = pd.DataFrame([
    {
        'PORTFOLIOCODE': code,
        'ATTRIBUTETYPE': atype,
        'ATTRIBUTETYPECODE': code_val,
        'ATTRIBUTETYPEVALUE': val
    }
    for code in portfolio_codes
    for atype, code_val, val in attribute_types
])

# --- 3. VC_EXIT_EVENTS ---
fund_ids = [f'VCF{i:03d}' for i in range(1, 4)]
exit_types = ['IPO', 'Acquisition']
acquirer_types = ['Strategic', 'Financial Sponsor']

vc_exit_events = pd.DataFrame([
    {
        'FUNDID': np.random.choice(fund_ids),
        'COMPANY': f'Co_{np.random.randint(1000, 9999)}',
        'EXITTYPE': np.random.choice(exit_types),
        'ACQUIRERTYPE': np.random.choice(acquirer_types),
        'MOIC': round(np.random.uniform(1.5, 5.0), 2),
        'EXITVALUE_MILLION_USD': round(np.random.uniform(20, 150), 2),
        'EXITDATE_ISO': (datetime.today() - timedelta(days=np.random.randint(100, 3000))).strftime('%Y-%m-%d')
    }
    for _ in range(15)
])

# --- 4. REGIONAL_ALLOCATION ---
regions = ['North America', 'Europe', 'Asia']
regional_allocation = []

for code in portfolio_codes:
    weights = np.random.dirichlet(np.ones(len(regions)), size=1).flatten()
    for region, weight in zip(regions, weights):
        regional_allocation.append({
            'PORTFOLIOCODE': code,
            'HISTORYDATE': datetime.today().strftime('%Y-%m-%d'),
            'CURRENCYCODE': 'USD',
            'CURRENCY': 'US Dollar',
            'LANGUAGECODE': 'en-US',
            'MARKETVALUEWITHOUTACCRUEDINCOME': 100,
            'MARKETVALUE': 100,
            'REGIONSCHEME': 'Synthetic Scheme',
            'REGION': region,
            'PORTFOLIOWEIGHT': round(weight * 100, 2)
        })

regional_allocation = pd.DataFrame(regional_allocation)

# --- 5. SECTOR_ALLOCATION ---
sectors = ['Health Care', 'Financials', 'Information Technology', 'Industrials']
sector_allocation = []

for code in portfolio_codes:
    weights = np.random.dirichlet(np.ones(len(sectors)), size=1).flatten()
    for sector, weight in zip(sectors, weights):
        sector_allocation.append({
            'PORTFOLIOCODE': code,
            'HISTORYDATE': datetime.today().strftime('%Y-%m-%d'),
            'CURRENCYCODE': 'USD',
            'CURRENCY': 'US Dollar',
            'LANGUAGECODE': 'en-US',
            'MARKETVALUE': 100,
            'SECTORSCHEME': 'GICS Sector',
            'CATEGORY': 'Sector',
            'CATEGORYNAME': sector,
            'PORTFOLIOWEIGHT': round(weight * 100, 2),
            'INDEX1WEIGHT': round(np.random.uniform(5, 15), 2),
            'INDEX2WEIGHT': np.nan,
            'INDEX3WEIGHT': np.nan
        })

sector_allocation = pd.DataFrame(sector_allocation)

# --- 6. PRODUCT_MASTER ---
product_master = pd.DataFrame({
    'PRODUCTCODE': ['VCF001', 'VCF002', 'VCF003'],
    'PRODUCTNAME': ['Assette VC Fund I', 'Assette VC Fund II', 'Assette VC Fund III'],
    'STRATEGY': ['VC'] * 3,
    'VEHICLECATEGORY': ['Pooled'] * 3,
    'VEHICLETYPE': ['Limited Partnership'] * 3,
    'ASSETCLASS': ['Private Equity'] * 3,
    'SHARECLASS': ['A', 'B', 'C'],
    'PERFORMANCEACCOUNT': ['VCACCT1', 'VCACCT2', 'VCACCT3'],
    'REPRESENTATIVEACCOUNT': ['VCREP1', 'VCREP2', 'VCREP3'],
    'ISMARKETED': [True, True, False],
    'PARENTPRODUCTCODE': [np.nan, 'VCF001', 'VCF002']
})

# --- 7. VC_FUNDS (Fund-Level Benchmark Data) ---
fund_names = [
    "Accel V", "Benchmark Capital", "Sequoia Capital U.S.", "Northzone IX", "Mayfield IX",
    "Flexcap Fund I", "Tamarack Global", "Tribe Digital Fund", "Columbia Capital Equity",
    "Factorial Funds"
]

firm_names = [
    "Accel", "Benchmark", "Sequoia Capital", "Northzone Ventures", "Mayfield Fund",
    "Flexcap Ventures", "Tamarack Global", "Tribe Capital", "Columbia Capital", "Factorial Funds"
]

locations = [
    "Palo Alto, CA", "San Francisco, CA", "Menlo Park, CA", "London, United Kingdom",
    "Menlo Park, CA", "New York, NY", "Darien, CT", "Menlo Park, CA", "Alexandria, VA", "Menlo Park, CA"
]

vintages = [1996, 1999, 2003, 2021, 1998, 2019, 2024, 2023, 1989, 2021]

irr = np.round(np.random.uniform(-10, 60, size=10), 2)
quartile = ["1 (Top Quartile)"] * 10
dpi = np.round(np.random.uniform(0.5, 20, size=10), 2)
rvpi = np.round(np.random.uniform(0.0, 8.0, size=10), 2)
tvpi = np.round(dpi + rvpi, 2)
fund_nav = np.round(np.random.uniform(10, 500, size=10), 2)
dry_powder = np.round(np.random.uniform(0, 30, size=10), 2)
contributed = np.round(np.random.uniform(50, 300, size=10), 2)
distributed = np.round(dpi * contributed, 2)
as_of_year = [2024] * 10
fund_types = ["Venture - General"] * 10

vc_funds = pd.DataFrame({
    "FUND_NAME": fund_names,
    "FIRM_NAME": firm_names,
    "FUND_TYPE": fund_types,
    "FUND_LOCATION": locations,
    "VINTAGE_YEAR": vintages,
    "IRR_BENCHMARK": irr,
    "FUND_QUARTILE": quartile,
    "DPI": dpi,
    "RVPI": rvpi,
    "TVPI_BENCHMARK": tvpi,
    "FUND_NAV": fund_nav,
    "DRY_POWDER": dry_powder,
    "CONTRIBUTED": contributed,
    "DISTRIBUTED": distributed,
    "AS_OF_YEAR": as_of_year
})

# --- EXPORT to Excel ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
output_path = f"vc_synthetic_data_{timestamp}.xlsx"

with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    portfolio_general_info.to_excel(writer, sheet_name='PORTFOLIO_GENERAL_INFO', index=False)
    portfolio_attributes.to_excel(writer, sheet_name='PORTFOLIO_ATTRIBUTES', index=False)
    vc_exit_events.to_excel(writer, sheet_name='VC_EXIT_EVENTS', index=False)
    regional_allocation.to_excel(writer, sheet_name='REGIONAL_ALLOCATION', index=False)
    sector_allocation.to_excel(writer, sheet_name='SECTOR_ALLOCATION', index=False)
    product_master.to_excel(writer, sheet_name='PRODUCT_MASTER', index=False)
    vc_funds.to_excel(writer, sheet_name='VC_FUNDS', index=False)

print(f"Excel file exported successfully to: {output_path}")
