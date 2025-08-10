# Benchmark Summary

## 1. What does the folder do?
Builds the complete “benchmark layer” for the VC simulation — stable benchmark master data, descriptive characteristics, time-series performance, and account→benchmark mappings used for relative performance and reporting. Outputs are Snowflake-ready (normalized, PK/FK keys) and can run locally (CSV) or in Azure Functions.

**Scripts:**

**benchmark_general.py** — Creates `BENCHMARK_GENERAL_INFORMATION` with stable codes and readable names (mix of a few real indices + synthetic VC/PE families by provider × region × sector).

**benchmark_characteristics.py** — Populates `BENCHMARK_CHARACTERISTICS` (inception year, currency, category) and long-form stats rows (e.g., Median IRR, Mean MOIC, Top Quartile DPI, Fund Count, Std Dev IRR, # of Securities).

**benchmark_performance.py** — Generates `BENCHMARK_PERFORMANCE`:
- Real indices via configurable ticker proxies (daily).
- Synthetic VC/PE curves via a 3-phase quarterly path (price-level index from 100).

**benchmark_account_association.py** — Builds `BENCHMARK_ACCOUNT_ASSOCIATION` mapping accounts (`ACC0001`–`ACC0050`) to 2–3 benchmarks each with preference `RANK` 1–3.

## 2. Role in the Overall Project
Provides canonical benchmark definitions and time series consumed by `product/performance.py` and fact-sheet generation (comparative charts, “vs. benchmark” tables, narrative commentary).

Enables account-specific default benchmark sets (preferred, alternates) aligned with institutional reporting norms (quarterly VC, daily public-market proxies).

Fits the unified data model used across portfolio, account, product, holdings, exits, and manager modules; designed for Snowflake schemas and future orchestration in Azure Functions.

## 3. How it Works
**benchmark_general.py**
- Generate a small universe:
  - Real benchmarks: S&P 500 (`SP_500`), Russell 2500 (`R2500`), MSCI World (`MSCI_WD`).
  - Synthetic VC/PE benchmarks: exactly seven generated names such as “PitchBook U.S. VC Performance Index” or “Cambridge Associates Europe Tech Venture Capital Index.”
- Code & name rules:
  - Code pattern: `{PROVIDER_ABBR}_{REGION}_{SECTOR}` (collision-safe with numeric suffix).
  - Human-readable names for fact sheets.
- Output: `BENCHMARK_CODE`, `BENCHMARK_NAME`.
- Validation (inline): Uniqueness of codes; non-null names; deterministic seed option for reproducibility.

**benchmark_characteristics.py**
- Attach baseline metadata per benchmark:
  - `INCEPTION_YEAR` sampled from a realistic range (2010–2022).
  - `CURRENCY_CODE`/`CURRENCY` selected via region defaults (e.g., Europe→EUR, Canada→CAD; otherwise weighted toward USD).
  - `CATEGORY`/`CATEGORY_NAME` standardized to “VC Benchmark” / “Venture Capital”.
- Generate characteristic rows (wide-to-long):
  - `CHARACTERISTIC_NAME` / `STATISTIC_TYPE` drawn from:
    - Median IRR (Median), Mean MOIC (Mean), Top Quartile DPI (Percentile), Fund Count (Count), Std Dev IRR (StdDevIRR), # of Securities (NumSecurities).
  - `CHARACTERISTIC_VALUE`:
    - Count ≈ 40–150; NumSecurities fixed for real indices (SP_500→500, R2500→2500, MSCI_WD→~1600) otherwise 20–500; remaining stats sampled around plausible ranges (~10 with noise).
  - `UNIT` determined by `STATISTIC_TYPE` (%, #).
  - `HISTORY_DATE` stamped to run date.
- Output: `BENCHMARK_CODE`, `INCEPTION_YEAR`, `CURRENCY_CODE`, `CURRENCY`, `CATEGORY`, `CATEGORY_NAME`, `CHARACTERISTIC_NAME`, `STATISTIC_TYPE`, `UNIT`, `CHARACTERISTIC_VALUE`, `HISTORY_DATE`.
- Validation (inline): Referential integrity to GENERAL_INFORMATION; type checks; value range sanity (e.g., IRR within bounds).

**benchmark_performance.py**
- Real indices:
  - Pull daily closes via yfinance proxies (e.g., ^GSPC, IWM, URTH).
  - Start at the benchmark’s `INCEPTION_YEAR`; store as PRICE series with `PERFORMANCE_FREQUENCY` = Daily.
- Synthetic VC/PE benchmarks:
  - Build a quarterly PRICE path with a three-phase pattern (slow early growth, acceleration, then normalization), starting from 100 and appending ~48 quarters (capped by inception→today).
  - `PERFORMANCE_FREQUENCY` = Quarterly.
  - `CURRENCY` inherited from characteristics.
- Output: `BENCHMARK_CODE`, `PERFORMANCE_DATA_TYPE` ("PRICE"), `CURRENCY_CODE`, `CURRENCY`, `PERFORMANCE_FREQUENCY`, `VALUE`, `HISTORY_DATE`.
- Validation (inline): Monotonic dates per benchmark; no duplicate `(code, date)`; currency consistency; frequency checks.

**benchmark_account_association.py**
- Accounts:
  - Generate 50 account IDs (`ACC0001`…`ACC0050`).
- Assignment logic:
  - For each account, sample 2–3 distinct `BENCHMARK_CODE`s from `BENCHMARK_GENERAL_INFORMATION`.
  - Assign `RANK` = 1, 2, 3 to express preference/display order.
- Output: `ACCOUNT_ID`, `BENCHMARK_CODE`, `RANK`.
- Validation (inline): All codes exist in GENERAL_INFORMATION; `(ACCOUNT_ID, BENCHMARK_CODE)` unique; ranks contiguous per account.

## 4. Assumptions
- Small, mixed universe: a few public-market proxies + ~7 synthetic VC/PE families (compact for demos, rich enough for comparisons).
- Region-aware currency defaults (USD dominant); weights skewed toward USD for global comparability.
- Characteristic and synthetic performance values are simulated for QA/demo realism — not backtested against provider data.
- VC benchmarks represented as price-level indices to support relative charts in fact sheets; cash-flow metrics (IRR/DPI/TVPI) come from the product/performance side.
- Each account prefers multiple benchmarks; `RANK` conveys the default display order in reports.

## 5. Future Expansion
- **Returns layer**: Add total-return series, rolling % change, drawdown, and volatility for public indices.
- **PME & Beta**: Implement PME vs S&P 500 and factor/beta summaries for context in fact sheets.
- **Config-driven universes**: Externalize providers/regions/sectors/tickers via JSON/YAML; add deterministic seeds for repeatable demos.
- **Snowflake schemas**: Persist to BENCHMARK.GENERAL_INFORMATION, BENCHMARK.CHARACTERISTICS, BENCHMARK.PERFORMANCE, BENCHMARK.ACCOUNT_ASSOCIATION with PK/FK and history columns.
- **Automation**: Schedule in Azure Functions; add logging, data-quality alerts (nulls, gaps, outliers); cache API pulls for reproducibility.
- **Cross-module mapping**: Link PRODUCTCODE → preferred BENCHMARK_CODE to auto-populate “vs. benchmark” in fact sheets.
