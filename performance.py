# price last month, price this month --> performance metrics
# do this for each company in the portfolio and aggregate the results

# could just say that the majority of the portfolio companies increased in value
# so we select some random value to show that increase

# portfolio_performance.py
"""
This script computes and visualizes portfolio-level performance metrics using synthetic holdings and performance data.
It supports CSV-based input for local testing and will be adapted to use a Snowflake connection.

Functionality:
- Aggregates investment metrics by portfolio
- Calculates TVPI, DPI, RVPI
- Computes weighted IRR and MOIC
- Visualizes results with bar charts
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class PortfolioPerformanceAnalyzer:
    def __init__(self, holdings_path: str, metrics_path: str):
        """
        Initialize the analyzer with file paths to holdings and metrics data.
        """
        self.holdings_path = holdings_path
        self.metrics_path = metrics_path
        self.df = None
        self.portfolio_perf = None
        self.final_perf = None

    def load_data(self):
        """
        Load holdings and metrics CSV files and merge on TICKER.
        """
        holdings_df = pd.read_csv(self.holdings_path)
        metrics_df = pd.read_csv(self.metrics_path)

        df = pd.merge(metrics_df, holdings_df[['TICKER', 'PORTFOLIOCODE']],
                      on='TICKER', how='left')

        df.rename(columns={
            'CURRENT_NAV': 'NAV',
            'INVESTMENT_AMOUNT': 'CASHINVESTED',
            'DISTRIBUTION_AMOUNTS': 'CASHDISTRIBUTED'
        }, inplace=True)

        numeric_cols = ['MOIC', 'IRR', 'TVPI', 'DPI', 'CASHINVESTED', 'CASHDISTRIBUTED', 'NAV']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        self.df = df

    def calculate_aggregates(self):
        """
        Aggregate and compute portfolio-level metrics.
        """
        df = self.df

        portfolio_perf = df.groupby('PORTFOLIOCODE').agg({
            'CASHINVESTED': 'sum',
            'CASHDISTRIBUTED': 'sum',
            'NAV': 'sum'
        }).reset_index()

        portfolio_perf['TVPI'] = (portfolio_perf['CASHDISTRIBUTED'] + portfolio_perf['NAV']) / portfolio_perf['CASHINVESTED']
        portfolio_perf['DPI'] = portfolio_perf['CASHDISTRIBUTED'] / portfolio_perf['CASHINVESTED']

        df['WEIGHT'] = df['CASHINVESTED'] / df.groupby('PORTFOLIOCODE')['CASHINVESTED'].transform('sum')
        weighted_perf = df.groupby('PORTFOLIOCODE').apply(
            lambda x: pd.Series({
                'IRR': np.average(x['IRR'], weights=x['WEIGHT']),
                'MOIC': np.average(x['MOIC'], weights=x['WEIGHT'])
            })
        ).reset_index()

        final_perf = pd.merge(portfolio_perf, weighted_perf, on='PORTFOLIOCODE', how='left')
        self.portfolio_perf = portfolio_perf
        self.final_perf = final_perf

    def plot_metrics(self):
        """
        Generate bar plots for IRR and MOIC by portfolio.
        """
        sorted_perf = self.final_perf.sort_values(by="IRR", ascending=False)

        plt.figure(figsize=(10, 5))
        plt.bar(sorted_perf['PORTFOLIOCODE'], sorted_perf['IRR'])
        plt.title("Portfolio IRR by Fund")
        plt.ylabel("Internal Rate of Return")
        plt.xlabel("Portfolio Code")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

        plt.figure(figsize=(10, 5))
        plt.bar(sorted_perf['PORTFOLIOCODE'], sorted_perf['MOIC'])
        plt.title("Portfolio MOIC by Fund")
        plt.ylabel("Multiple on Invested Capital")
        plt.xlabel("Portfolio Code")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

if __name__ == '__main__':
    analyzer = PortfolioPerformanceAnalyzer('holdings.csv', 'holdings_metrics.csv')
    analyzer.load_data()
    analyzer.calculate_aggregates()
    print(analyzer.final_perf.head())
    #analyzer.plot_metrics()


"""
Get the benchmark for that time period so we can see if the metrics are performing
well relative to whatever benchmark we choose.

Refactor, documentation, next steps
Snowflake 
"""