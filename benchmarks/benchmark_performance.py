"""
The `BENCHMARK_PERFORMANCE` table records price index levels for each 
benchmark—daily for public (market) benchmarks and quarterly for 
synthetic (VC/PE) strategies. This mixed frequency enables direct, realistic 
time-series comparison between venture strategies and public benchmarks in fact 
sheets, dashboards, and J-curve analysis.

Each row represents a single (date, benchmark) value—daily for public 
benchmarks, quarterly for synthetic.

### Generation Logic

| Step                       | Description                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------------------| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Benchmark Loop          | For every benchmark in `BENCHMARK_GENERAL_INFORMATION`, determine its reporting currency and inception year using `BENCHMARK_CHARACTERISTICS`.                                                                                                                                                                                                                                                    |
| 2. Index Type Detection    | If the benchmark is a public index (S&P 500, Russell 2000/2500, MSCI World),  daily closing levels are pulled using the [yfinance Python package](https://pypi.org/project/yfinance/) , with the time series starting from the benchmark's inception year.                                                                                                                                      |
| 3. Synthetic VC Indices    | For venture capital and private benchmarks, a synthetic "price index" (base=100) is generated  quarterly  from the inception year, reflecting realistic fund value trajectories:<br> - Early Years: Modest or flat growth<br> - Growth Phase: Accelerated mark-ups<br> - Late Years: Steady, slower compounding.                                                                    |
| 4. Placeholder for Future Daily Synthetic Logic | For synthetic benchmarks, the code includes a clearly marked placeholder for future extension to daily frequency, should granular reporting ever be required. The quarterly logic can be swapped for daily by enabling a commented code block.                                                                                                                               |
| 5. Consistent Currency     | Each row records both `CURRENCY_CODE` (ISO) and `CURRENCY` (full name), consistent with benchmark-level assignment for reporting.                                                                                                                                                                                                                                                                 |
| 6. Time Window Enforcement | For both real and synthetic indices, only periods from the benchmark's inception year through the most recent available date are reported, so series are directly comparable by inception.                                                                                                                                                                                                        |
| 7. Validation              | All `VALUE` fields are guaranteed floats (never arrays or lists). Rows with missing price data (package gaps) are excluded.                                                                                                                                                                                                                                                                       |
| 8. Foreign Key             | `BENCHMARK_CODE` maintains referential integrity to `BENCHMARK_GENERAL_INFORMATION` and downstream fact sheet components.                                                                                                                                                                                                                                                                         |

### Field Definitions

| Column                 | Description                                             | Example        |
|------------------------|---------------------------------------------------------|----------------|
| `BENCHMARK_CODE`       | Unique code for each benchmark                          | VC_US_GROWTH   |
| `PERFORMANCE_DATA_TYPE`| Type of measure (always 'PRICE' in this implementation) | PRICE          |
| `CURRENCY_CODE`        | ISO code (USD, EUR, etc.)                               | USD            |
| `CURRENCY`             | Full currency name                                      | US Dollar      |
| `PERFORMANCE_FREQUENCY`| 'Daily' for public benchmarks; 'Quarterly' for synthetic| Daily / Quarterly |
| `VALUE`                | Index level (float)                                     | 125.43         |
| `HISTORY_DATE`         | Price date (YYYY-MM-DD); can be quarter-end or daily    | 2022-09-30     |

### Design Rationale and Alignment

- Hybrid Data Source: Public indices are actual market levels 
(e.g., S&P 500, MSCI World) pulled with the yfinance Python package at daily 
frequency; VC/private indices are simulated quarterly for realism.
- Flexible Frequency: Real indices support daily analysis and charting; 
synthetic benchmarks reflect true fund reporting practice (quarterly), but 
code is designed for future daily expansion if needed.
- Aligned Inceptions: Every benchmark’s time series starts at its own 
inception year for apples-to-apples analysis.
- Consistent Index Construction: All synthetic price series are generated 
using plausible VC logic—never negative in early years, no bracketed values, 
and appropriate compounding.
- Schema Compliance: Structure and keys align to Assette’s Snowflake data model 
and support downstream fact sheet automation.
- Clean Output: Output ready for reporting, charting, and IRR/J-curve 
calculations (to be performed downstream using the price series).
"""

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import random
import yfinance as yf
from datetime import datetime
from path_helpers import get_csv_path

random.seed(42)
np.random.seed(42)

# Bring in the information
df_benchmark_characteristics = pd.read_csv("CSVs/benchmark_characteristics.csv")
df_benchmark_general = pd.read_csv("CSVs/benchmark_general.csv")

# Mapping: BENCHMARK_CODE -> INCEPTION_YEAR
INCEPTION_MAP = df_benchmark_characteristics.drop_duplicates("BENCHMARK_CODE") \
    .set_index("BENCHMARK_CODE")["INCEPTION_YEAR"].to_dict()

# yfinance tickers for real indices
REAL_INDEX_MAP = {
    "SP_500":  "^GSPC",
    "R2500":   "IWM",    # IWM = Russell 2000 ETF
    "MSCI_WD": "URTH"    # URTH = MSCI World ETF
}

def get_daily_prices_yf(ticker, start, end):
    """Download daily close prices from yfinance and return as list of (price, date)."""
    df = yf.download(ticker, start=start, end=end, progress=False)
    if df.empty or "Close" not in df.columns:
        return []
    prices = df["Close"].round(2).values
    dates = [d.date() for d in df.index]
    return [(float(np.asarray(price).squeeze()), dt) for price, dt in zip(prices, dates)]

def get_quarterly_prices_yf(ticker, start, end):
    """(Retained for possible future use) Download quarterly prices if needed."""
    df = yf.download(ticker, start=start, end=end, progress=False)
    if df.empty or "Close" not in df.columns:
        return []
    df_q = df.resample('Q-DEC').last()
    if df_q.empty or "Close" not in df_q.columns:
        return []
    prices = df_q["Close"].round(2).values
    dates = [d.date() for d in df_q.index]
    return [(float(np.asarray(price).squeeze()), dt) for price, dt in zip(prices, dates)]

def simulate_vc_price_series(n, base=100):
    """Quarterly price simulation for synthetic VC/PE benchmarks."""
    prices = [base]
    for i in range(n):
        if i < 8:
            q_return = np.random.normal(0.005, 0.005)
        elif i < 20:
            q_return = np.random.normal(0.04, 0.01)
        else:
            q_return = np.random.normal(0.015, 0.007)
        prices.append(prices[-1] * (1 + q_return))
    return [round(x, 2) for x in prices[1:]]

# --- Placeholder for daily synthetic simulation (future enhancement) ---
# def simulate_vc_price_series_daily(n_days, base=100):
#     """Future enhancement: Simulate daily synthetic price series for VC/PE benchmarks."""
#     prices = [base]
#     for i in range(n_days):
#         # Insert realistic daily return logic for VC (tiny up/flat, random walk, etc.)
#         d_return = np.random.normal(0.00025, 0.001)  # placeholder for demonstration
#         prices.append(prices[-1] * (1 + d_return))
#     return [round(x, 2) for x in prices[1:]]

performance_records = []
today = datetime.today()
currency_code_default = "USD"
currency_name_default = "US Dollar"

for _, row in df_benchmark_general.iterrows():
    code = row["BENCHMARK_CODE"]
    currency_code = row.get("CURRENCY_CODE", currency_code_default)
    currency_name = row.get("CURRENCY", currency_name_default)
    performance_frequency = "Daily" if code in REAL_INDEX_MAP else "Quarterly"
    inception = INCEPTION_MAP.get(code, 2012)
    start_date = datetime(int(inception), 3, 31)  # First Q-end from inception year

    if code in REAL_INDEX_MAP:
        # Use daily prices from this inception year forward
        start = f"{inception}-01-01"
        end = today.strftime("%Y-%m-%d")
        yf_ticker = REAL_INDEX_MAP[code]
        d_data = get_daily_prices_yf(yf_ticker, start, end)
        for price, dt in d_data:
            if dt >= start_date.date():
                performance_records.append({
                    "BENCHMARK_CODE": code,
                    "PERFORMANCE_DATA_TYPE": "PRICE",
                    "CURRENCY_CODE": currency_code,
                    "CURRENCY": currency_name,
                    "PERFORMANCE_FREQUENCY": "Daily",
                    "VALUE": price,
                    "HISTORY_DATE": dt
                })
    else:
        # Quarterly simulation for synthetic benchmarks (VC/PE)
        n_quarters = min(48, (today.year - int(inception)) * 4 + today.month // 3)
        price_series = simulate_vc_price_series(n_quarters, base=100)
        for i in range(n_quarters):
            q_date = start_date + pd.DateOffset(months=3*i)
            performance_records.append({
                "BENCHMARK_CODE": code,
                "PERFORMANCE_DATA_TYPE": "PRICE",
                "CURRENCY_CODE": currency_code,
                "CURRENCY": currency_name,
                "PERFORMANCE_FREQUENCY": "Quarterly",
                "VALUE": price_series[i],
                "HISTORY_DATE": q_date.date()
            })
        # --- Placeholder for future daily synthetic logic ---
        # If daily synthetic simulation is needed, use the code below:
        # n_days = (today - start_date).days
        # price_series = simulate_vc_price_series_daily(n_days, base=100)
        # for i in range(n_days):
        #     d_date = start_date + pd.DateOffset(days=i)
        #     performance_records.append({
        #         "BENCHMARK_CODE": code,
        #         "PERFORMANCE_DATA_TYPE": "PRICE",
        #         "CURRENCY_CODE": currency_code,
        #         "CURRENCY": currency_name,
        #         "PERFORMANCE_FREQUENCY": "Daily",
        #         "VALUE": price_series[i],
        #         "HISTORY_DATE": d_date.date()
        #     })

df_benchmark_performance = pd.DataFrame(performance_records)[[
    "BENCHMARK_CODE",
    "PERFORMANCE_DATA_TYPE",
    "CURRENCY_CODE",
    "CURRENCY",
    "PERFORMANCE_FREQUENCY",
    "VALUE",
    "HISTORY_DATE"
]]

print("\nBENCHMARK_PERFORMANCE")
print(df_benchmark_performance.head())

output_file_path = get_csv_path('benchmark_performance.csv')
df_benchmark_performance.to_csv(output_file_path, index=False)

# -- Snowflake SQL table creation

# CREATE TABLE BENCHMARK_PERFORMANCE (
#     BENCHMARK_CODE            VARCHAR(50),     -- Foreign key to BENCHMARK_GENERAL_INFORMATION
#     PERFORMANCE_DATA_TYPE     VARCHAR(20),     -- Always 'PRICE' in this simulation (or could be 'IRR', 'MOIC', etc. if expanded)
#     CURRENCY_CODE             VARCHAR(10),     -- ISO code, e.g. 'USD'
#     CURRENCY                  VARCHAR(50),     -- Full currency name
#     PERFORMANCE_FREQUENCY     VARCHAR(30),     -- 'Daily' for public, 'Quarterly' for synthetic
#     VALUE                     NUMBER(18,2),    -- Index price level (never array)
#     HISTORY_DATE              DATE,            -- Date (quarter-end or daily)

#     FOREIGN KEY (BENCHMARK_CODE) REFERENCES BENCHMARK_GENERAL_INFORMATION(BENCHMARK_CODE)
# );


"""
1 Failed download:
['^GSPC']: Timeout('Failed to perform, curl: (28) Operation timed out after 10001 
milliseconds with 70715 bytes received. See https://curl.se/libcurl/c/libcurl-errors.html first for more details.')

This is the error that gets thrown if you are offline due to the connection to 
the yfinance API. Would we quickly be able to get this information as a CSV
just in case there is a problem in the future?
"""