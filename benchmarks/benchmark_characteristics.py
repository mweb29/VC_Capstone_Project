"""
The `BENCHMARK_CHARACTERISTICS` table captures key summary statistics and structural characteristics for each venture capital benchmark. It supports comparisons of performance, composition, and reporting within fact sheets. Each row represents one benchmark–statistic combination (e.g., MEDIAN_IRR for a benchmark).

### **Generation Logic**

We dynamically generate one row per benchmark and characteristic defined for each `BENCHMARK_CODE` in the `BENCHMARK_GENERAL_INFORMATION` table. Values are simulated within realistic ranges, with metadata and currency logic inferred from benchmark names and regional mapping.

| Column Name             | Type      | Example           | Notes                                                |
|-------------------------|-----------|-------------------|------------------------------------------------------|
| `BENCHMARK_CODE`        | String    | `PB_US_TE`        | Foreign key from `BENCHMARK_GENERAL_INFORMATION`     |
| `INCEPTION_YEAR`        | Integer   | `2017`            | Random year between 2010 and 2022                    |
| `CURRENCY_CODE`         | String    | `USD`             | Derived by region mapping or default-weighted random |
| `CURRENCY`              | String    | `US Dollar`       | Full currency name                                   |
| `CATEGORY`              | String    | `VC Benchmark`    | Always set as 'VC Benchmark'                         |
| `CATEGORY_NAME`         | String    | `Venture Capital` | Always set as 'Venture Capital'                      |
| `CHARACTERISTIC_NAME`   | String    | `Median IRR`      | Statistic label (e.g., Median IRR, Mean MOIC, etc.)  |
| `STATISTIC_TYPE`        | String    | `Median`          | Underlying type (Median, Count, NumSecurities, etc.) |
| `UNIT`                  | String    | `%`               | Unit for the value (%, #, etc.)                      |
| `CHARACTERISTIC_VALUE`  | Float/Int | `10.5`            | Simulated value, type depends on characteristic      |
| `HISTORY_DATE`          | Date      | `2025-07-30`      | As-of date (current date or snapshot)                |


### **Key Logic and Validation**

| Step                                  | Description                                                                                                                              |
|----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| **1. Row Generation**                  | Number of rows = #benchmarks × #characteristics (e.g., MEDIAN_IRR, FUND_COUNT, NUM_SECURITIES, etc.)                                     |
| **2. Name-Based Metadata Extraction**  | Currency, region, and sector are inferred from benchmark name if possible, otherwise assigned randomly with controlled weights.          |
| **3. Simulation Logic**                | All values (e.g., IRR, MOIC, COUNT, NUM_SECURITIES) are simulated using business logic and controlled random distributions.              |
| **4. Range Validation**                | Ranges and types enforced for realism and clean data (e.g., IRR 5–25%, FUND_COUNT 40–150, NUM_SECURITIES 20–500, S&P 500 set to 500, etc.) |
"""

import pandas as pd
import numpy as np
import random
import yfinance as yf
from datetime import datetime

# Ensure reproducibility
random.seed(42)
np.random.seed(42)

df_benchmark_general = pd.read_csv("CSVs/df_benchmark_general.csv")

BENCHMARK_CODES = df_benchmark_general["BENCHMARK_CODE"].tolist()
BENCHMARK_NAMES = df_benchmark_general["BENCHMARK_NAME"].tolist()

UNIT_MAP = {
    "Median":     "%",
    "Mean":       "%",
    "Percentile": "%",
    "Count":      "#",
    "StdDevIRR":  "%",
    "NumSecurities": "#"
}

# SHOULD THESE BE CALLS TO OUR APIS!!!!!!

CURRENCY_CODES = ["USD", "EUR", "CAD", "JPY", "GBP"]
CURRENCY_WEIGHTS = [0.80, 0.05, 0.05, 0.05, 0.05]
CURRENCY_NAME_MAP = {
    "USD": "US Dollar", "EUR": "Euro", "CAD": "Canadian Dollar",
    "JPY": "Japanese Yen", "GBP": "British Pound"
}
REGION_CURRENCY_MAP = {
    "Europe": "EUR", "Canada": "CAD", "U.S.": "USD", "North America": "USD",
    "Global": "USD", "Asia-Pacific": "USD", "Emerging Markets": "USD"
}

CHAR_DEFS = [
    {"name": "Median IRR",      "type": "Median"},
    {"name": "Mean MOIC",       "type": "Mean"},
    {"name": "Top Quartile DPI","type": "Percentile"},
    {"name": "Fund Count",      "type": "Count"},
    {"name": "Std Dev IRR",     "type": "StdDevIRR"},
    {"name": "# of Securities", "type": "NumSecurities"}
]

INCEPTION_YEARS = list(range(2010, 2023))
TODAY = datetime.today().strftime('%Y-%m-%d')

# Fixed securities for known indices
FIXED_SECURITIES = {
    "SP_500": 500,
    "R2500": 2500,
    "MSCI_WD": 1600  # Approximate, adjust if needed
}

# --- 1. Define yfinance tickers and a function for index meta ---
INDEX_META = {
    "SP_500":   {"yf": "^GSPC"},
    "R2500":    {"yf": "^R25I"},  # Note: "R25I" is not always present; "IWM" or "^RUT" are common proxies for Russell 2000
    "MSCI_WD":  {"yf": "URTH"},   # MSCI World ETF as proxy; "URTH" trades in USD; for exact, use "MXWO.L" for GBP
}
def get_index_info(bench_code):
    yf_code = INDEX_META[bench_code]["yf"]
    ticker = yf.Ticker(yf_code)
    info = ticker.info

    # Inception year: get first available data or fund inception
    if "firstTradeDateEpochUtc" in info:
        dt = datetime.utcfromtimestamp(info["firstTradeDateEpochUtc"] / 1000)
        year = dt.year
    elif "inceptionDate" in info and info["inceptionDate"]:
        dt = datetime.utcfromtimestamp(info["inceptionDate"])
        year = dt.year
    else:
        year = 2000  # Fallback

    # Currency
    currency_code = info.get("currency", "USD")
    currency_name = CURRENCY_NAME_MAP.get(currency_code, "Unknown")
    return year, currency_code, currency_name

# --- 2. Main Simulation ---
char_records = []
for bench_code, bench_name in zip(BENCHMARK_CODES, BENCHMARK_NAMES):
    if bench_code in INDEX_META:
        # Get from yfinance
        try:
            inception_year, currency_code, currency_name = get_index_info(bench_code)
        except Exception as e:
            # fallback if API fails
            inception_year, currency_code, currency_name = 2000, "USD", "US Dollar"
    else:
        inception_year = random.choice(INCEPTION_YEARS)
        # Region-based currency assignment
        currency_code = None
        for region, code in REGION_CURRENCY_MAP.items():
            if region in bench_name:
                currency_code = code
                break
        if currency_code is None:
            currency_code = random.choices(CURRENCY_CODES, weights=CURRENCY_WEIGHTS, k=1)[0]
        currency_name = CURRENCY_NAME_MAP[currency_code]

    for char in CHAR_DEFS:
        if char["type"] == "Count":
            value = np.random.randint(40, 150)
        elif char["type"] == "NumSecurities":
            # Use fixed value if known benchmark, else randomize
            if bench_code in FIXED_SECURITIES:
                value = FIXED_SECURITIES[bench_code]
            else:
                value = np.random.randint(20, 500)
        else:
            value = round(np.random.normal(loc=10, scale=2), 2)

        char_records.append({
            "BENCHMARK_CODE":         bench_code,
            "INCEPTION_YEAR":         inception_year,
            "CURRENCY_CODE":          currency_code,
            "CURRENCY":               currency_name,
            "CATEGORY":               "VC Benchmark",
            "CATEGORY_NAME":          "Venture Capital",
            "CHARACTERISTIC_NAME":    char["name"],
            "STATISTIC_TYPE":         char["type"],
            "UNIT":                   UNIT_MAP[char["type"]],
            "CHARACTERISTIC_VALUE":   value,
            "HISTORY_DATE":           TODAY
        })

df_benchmark_characteristics = pd.DataFrame(char_records)
print("\nBENCHMARK_CHARACTERISTICS")
print(df_benchmark_characteristics.head())

df_benchmark_characteristics.to_csv("CSVs/df_benchmark_characteristics.csv", index=False)


# -- Snowflake SQL table creation

# CREATE TABLE BENCHMARK_CHARACTERISTICS (
#     BENCHMARK_CODE          VARCHAR(50)    NOT NULL,   -- Foreign key to BENCHMARK_GENERAL_INFORMATION
#     INCEPTION_YEAR          INTEGER        NOT NULL,   -- Typical inception year of the funds in the benchmark
#     CURRENCY_CODE           VARCHAR(5)     NOT NULL,   -- Currency code (e.g., USD, EUR)
#     CURRENCY                VARCHAR(40)    NOT NULL,   -- Full currency name
#     CATEGORY                VARCHAR(30)    NOT NULL,   -- e.g., 'VC Benchmark'
#     CATEGORY_NAME           VARCHAR(40)    NOT NULL,   -- e.g., 'Venture Capital'
#     CHARACTERISTIC_NAME     VARCHAR(50)    NOT NULL,   -- Statistic label (e.g., 'Median IRR', '# of Securities')
#     STATISTIC_TYPE          VARCHAR(30)    NOT NULL,   -- Type of statistic (e.g., 'Median', 'Count', 'NumSecurities')
#     UNIT                    VARCHAR(5)     NOT NULL,   -- %, #, etc.
#     CHARACTERISTIC_VALUE    NUMBER(18,4)   NOT NULL,   -- Simulated value (can be float or integer)
#     HISTORY_DATE            DATE           NOT NULL,   -- As-of date

#     CONSTRAINT FK_BENCHMARK_CHARACTERISTICS_BENCHMARK_CODE
#         FOREIGN KEY (BENCHMARK_CODE) REFERENCES BENCHMARK_GENERAL_INFORMATION(BENCHMARK_CODE)
# );