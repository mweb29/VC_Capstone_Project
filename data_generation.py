import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import openpyxl

# Set random seed for reproducibility
np.random.seed(42)

# --- 1. portfolio_general_info ---
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

# --- 2. portfolio_attributes ---
attribute_types = [
    ('Strategy', 'Early-StageVC', 'Early-Stage VC'),
    ('Region', 'NorthAmerica', 'North America'),
    ('Vehicle', 'Commingled Fund', 'Commingled Fund')
]
portfolio_attributes = pd.DataFrame([
    {'PORTFOLIOCODE': code,
     'ATTRIBUTETYPE': atype,
     'ATTRIBUTETYPECODE': code_val,
     'ATTRIBUTETYPEVALUE': val}
    for code in portfolio_codes for atype, code_val, val in attribute_types
])

# --- 3. vc_exit_events ---
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

# --- 4. regional_allocation ---
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

# --- 5. sector_allocation ---
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

# --- 6. product_master ---
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

# --- Export to Excel ---
with pd.ExcelWriter('vc_synthetic_data.xlsx', engine='openpyxl') as writer:
    portfolio_general_info.to_excel(writer, sheet_name='PORTFOLIO_GENERAL_INFO', index=False)
    portfolio_attributes.to_excel(writer, sheet_name='PORTFOLIO_ATTRIBUTES', index=False)
    vc_exit_events.to_excel(writer, sheet_name='VC_EXIT_EVENTS', index=False)
    regional_allocation.to_excel(writer, sheet_name='REGIONAL_ALLOCATION', index=False)
    sector_allocation.to_excel(writer, sheet_name='SECTOR_ALLOCATION', index=False)
    product_master.to_excel(writer, sheet_name='PRODUCT_MASTER', index=False)

print("Excel file 'vc_synthetic_data.xlsx' created successfully.")