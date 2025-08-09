Product Summary
## 1. What does the folder do?

Provides product metadata and portfolio-level performance for downstream reporting and benchmarks.

Scripts:

**product_master.py** — Builds product_master.csv by grouping funds into products (strategy × region) and enriching with vehicle type/category, share class, and readable names.

**performance.py** — Aggregates holdings/metrics to portfolio-level TVPI, DPI, IRR, MOIC, NAV (CSV inputs for now; Snowflake-ready design). Optional bar charts for IRR/MOIC.


## 2. Role in the Overall Project
**product_master.py** standardizes product identifiers (e.g., VC_EARLY_NA) consumed by fact sheets and benchmarks.

**performance.py** turns position-level data into fund-level KPIs used in dashboards and LP reporting.

## 3. How it Works
**product_master.py**
- Read `portfolio_general_info.csv`.
- Derive STRATEGY_ABBR and REGION_BLOCK; form product groups (STRATEGY_ABBR_REGION_BLOCK).
- Assign PRODUCTCODE = VC_{group} and generate readable PRODUCTNAME.
- Randomize VEHICLETYPE → set VEHICLECATEGORY; assign SHARECLASS.
- Output `product_master.csv`.

**performance.py**
- Load `holdings.csv` and `holdings_metrics.csv`; merge on TICKER, keep PORTFOLIOCODE.
- Rename to standard columns and coerce numerics.
- Aggregate per PORTFOLIOCODE: CASHINVESTED, CASHDISTRIBUTED, NAV; compute TVPI, DPI.
- Compute weighted IRR & MOIC using investment weights.
- (Optional) Plot IRR/MOIC bar charts.

## 4. Assumptions
- Strategy/region → product grouping is one-to-many (one product, many funds).
- Vehicle type/category and share class can be randomly assigned for simulation.
- Performance weights use CASHINVESTED proportions.
- Current implementation reads/writes CSV; Snowflake connector will replace local IO later.
## 5. Future Expansion
- Save aggregated performance data to Snowflake and merge with benchmarks for relative performance analysis.
- Add time-series (monthly/quarterly) performance and rolling IRR.
- Support multi-share classes/fee structures and product hierarchies.
