# Holdings & Exits Summary

## 1. What does the folder do?
Generates and enriches position-level holdings data for the VC simulation, calculates standardized investment metrics, and simulates portfolio company exit events. Produces Snowflake-ready CSV outputs designed to feed into performance aggregation, attribution analysis, and fact sheet generation.

**Scripts:**

**holdings.py** — Simulates current and historical portfolio company positions across funds, including identifiers, investment dates, quantities, cost basis, and valuations.

**holdings_metrics.py** — Derives standardized performance metrics at the holding level (unrealized gain/loss, MOIC, IRR, ownership %), normalizing across currencies using FX data.

**exit.py** — Generates exit event records for portfolio companies (M&A, IPO, secondary sale) with proceeds, dates, and post-exit performance impact.

## 2. Role in the Overall Project
Provides the granular building blocks for portfolio performance aggregation.  
Holdings data feeds `performance.py` for NAV, TVPI, DPI calculations and benchmarking.  
Exit events contribute realized return data for LP reporting and fact sheet narratives.

## 3. How it Works
**holdings.py**
- For each simulated fund/company:
  - Assign investment date, ticker, sector, region, currency.
  - Generate cost basis, shares owned, and current valuation.
- Ensure referential integrity to portfolio and product codes.
- Output `holdings.csv`.

**holdings_metrics.py**
- Load `holdings.csv` and join with FX/currency data.
- Calculate:
  - Unrealized Gain/Loss = Current Value − Cost Basis.
  - MOIC = Current Value ÷ Cost Basis.
  - IRR = Annualized rate from investment date to valuation date.
  - Ownership % = Shares Owned ÷ Total Shares.
- Output `holdings_metrics.csv` for performance aggregation.

**exit.py**
- Randomly assign exit type (IPO, M&A, secondary sale) to a subset of holdings.
- Assign exit date within a realistic range post-investment.
- Calculate proceeds based on valuation at exit and ownership %.
- Output `exits.csv`.

## 4. Assumptions
- Investment amounts, valuations, and exit outcomes are simulated within realistic ranges for VC portfolios.
- FX rates are static at time of metric calculation.
- Each holding belongs to exactly one portfolio company and one portfolio code.
- Exit events are a minority of total holdings, aligned with typical VC exit timelines.

## 5. Future Expansion
- Add quarterly time-series valuations for each holding to support historical NAV analysis.
- Introduce dynamic FX rates over time.
- Expand exit modeling with staged proceeds (e.g., earn-outs) and post-IPO performance tracking.
- Integrate directly with Snowflake and automate metric refreshes.
