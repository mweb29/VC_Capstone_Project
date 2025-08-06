"""
The `BENCHMARK_GENERAL_INFORMATION` table defines a list of 10 benchmarks, 
3 real‐world indices, and 7 synthetic benchmarks—that Venture Capital (VC) 
funds in our simulation can be evaluated against. Each row represents a 
benchmark commonly used in institutional performance reporting—either a 
well-known public index or a simulated sector/geography/combination benchmark.

### Composition

- Real benchmarks (3): 
  - `SP_500`  — S&P 500 Index  
  - `R2500`   — Russell 2500 Index  
  - `MSCI_WD` — MSCI World Index  

- Synthetic benchmarks (7):** Generated via random combinations of provider,
region, and/or sector.

### Synthetic Generation Logic

We generate 7 unique synthetic records using one of three patterns:

| Pattern Type | Name Example                                                 | Code Example    |
|--------------|--------------------------------------------------------------|-----------------|
| `geo`        | PitchBook Europe VC Benchmark                                | `PB_EU`         |
| `sector`     | Preqin CleanTech VC Performance Index                        | `PR_CT`         |
| `combo`      | Cambridge Associates North America Healthcare Growth Index   | `CA_NA_HC`      |

### Key Logic and Validation

| Step                     | Description                                                                                                                                |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| 1. Provider Prefix**     | Extracts a stable 2-letter code from the provider name: <br>e.g., `"PitchBook"` → `"PB"`                                                   |
| 2. Geography Mapping**   | Maps region names like `"Asia-Pacific"` to short codes like `"AP"`                                                                         |
| 3. Sector Mapping**      | Maps sectors like `"Healthcare"` to codes like `"HC"`                                                                                      |
| 4. Code Construction**   | Combines provider, region, and/or sector codes to form a machine-readable `BENCHMARKCODE`                                                  |
| 5. Real vs Synthetic**   | Lists 3 fixed real benchmarks explicitly; generates 7 synthetic ones programmatically                                                      |
| 6. Uniqueness**          | Appends numeric suffixes (`_2`, `_3`, etc.) if a generated code already exists                                                             |
"""

import pandas as pd
import numpy as np
import random
import re

# Ensure reproducibility
random.seed(42)
np.random.seed(42)

def get_provider_prefix(name: str) -> str:
    """
    Data Validation Logic 1:
    Derive a stable 2-letter prefix from a provider name by:
      - Extracting the first two uppercase letters if available (e.g., 'PitchBook' → 'PB')
      - Otherwise falling back to the first two characters uppercased.
    """
    caps = re.findall(r'[A-Z]', name)
    if len(caps) >= 2:
        return ''.join(caps[:2])
    return name[:2].upper()

# Map full geography names to 2-letter codes
REGION_MAP = {
    "U.S.":             "US",
    "North America":    "NA",
    "Global":           "GL",
    "Asia-Pacific":     "AP",
    "Europe":           "EU",
    "Emerging Markets": "EM",
    "Canada":           "CA"
}

# Pools of inputs
PROVIDERS        = ["PitchBook", "Cambridge Associates", "Preqin", "CB Insights", "CB Insights", "Carta"]
GEOGRAPHIES      = list(REGION_MAP.keys())
SECTORS          = ["Tech", "Healthcare", "CleanTech", "AI", "Growth Equity"]

# Explicit sector code map for clarity in codes
SECTOR_CODE_MAP = {
    "Tech":           "TE",
    "Healthcare":     "HC",
    "CleanTech":      "CT",
    "AI":             "AI",
    "Life Sciences":  "LS",
    "Growth Equity":  "GE"
}

# Suffix pools for different naming patterns
GEO_SUFFIXES     = ["Venture Capital Index", "VC Benchmark", "Private Equity Index", "Growth Equity Index", "Private Capital Index"]
SECTOR_SUFFIXES  = ["VC Performance Index", "Venture Capital Index", "Private Equity Index", "Growth Equity Benchmark"]
COMBO_SUFFIXES   = ["Venture Capital Index", "Growth Index", "VC Performance Index"]

BENCHMARK_NAMES = []
BENCHMARK_CODES = []
USED_CODES      = set()

# Add 3 traditional benchmarks
TRADITIONAL = [
    ("SP_500",  "S&P 500 Index"),
    ("R2500",   "Russell 2500 Index"),
    ("MSCI_WD", "MSCI World Index")
]
for code, name in TRADITIONAL:
    USED_CODES.add(code)
    BENCHMARK_CODES.append(code)
    BENCHMARK_NAMES.append(name)

# Generate exactly 7 synthetic benchmark entries
for _ in range(7):  # Change n here if more benchmarks are needed
    provider = random.choice(PROVIDERS)
    pattern  = random.choice(["geo", "sector", "combo"])

    if pattern == "geo":
        region_full = random.choice(GEOGRAPHIES)
        suffix      = random.choice(GEO_SUFFIXES)
        name        = f"{provider} {region_full} {suffix}"
        pfx = get_provider_prefix(provider)
        rfx = REGION_MAP[region_full]
        base_code = f"{pfx}_{rfx}"

    elif pattern == "sector":
        sector_full = random.choice(SECTORS)
        suffix      = random.choice(SECTOR_SUFFIXES)
        name        = f"{provider} {sector_full} {suffix}"
        pfx = get_provider_prefix(provider)
        sfx = SECTOR_CODE_MAP[sector_full]
        base_code = f"{pfx}_{sfx}"

    else:
        region_full = random.choice(GEOGRAPHIES)
        sector_full = random.choice(SECTORS)
        suffix      = random.choice(COMBO_SUFFIXES)
        name        = f"{provider} {region_full} {sector_full} {suffix}"
        pfx = get_provider_prefix(provider)
        rfx = REGION_MAP[region_full]
        sfx = SECTOR_CODE_MAP[sector_full]
        base_code = f"{pfx}_{rfx}_{sfx}"

    # Ensure uniqueness
    code = base_code
    counter = 1
    while code in USED_CODES:
        counter += 1
        code = f"{base_code}_{counter}"
    USED_CODES.add(code)

    BENCHMARK_NAMES.append(name)
    BENCHMARK_CODES.append(code)

# Assemble into a DataFrame with UPPER_SNAKE_CASE column names
df_benchmark_general = pd.DataFrame({
    "BENCHMARK_CODE":  BENCHMARK_CODES,
    "BENCHMARK_NAME":  BENCHMARK_NAMES
})


print("BENCHMARK_GENERAL_INFORMATION")
print(df_benchmark_general.head())

df_benchmark_general.to_csv("CSVs/df_benchmark_general.csv", index=False)

print("Completed")