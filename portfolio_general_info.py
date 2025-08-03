# # Portfolio General Info
#
#
# ### Portfolio Generation Logic
#
# This module generates a synthetic **venture capital fund-level dataset** for use in fact sheets, LP reporting, and simulation.  
# It mimics enriched PitchBook-style data, maintaining internal consistency across firm, strategy, region, currency, and naming conventions.
#
# ---
#
# ### Processing Steps
#
# | Step | Description |
# |------|-------------|
# | 1 | Simulates 100 funds across synthetic VC firms (e.g., Horizon Growth Partners, IronHill Ventures) |
# | 2 | Assigns fund strategy (`Early`, `General`, `Later`) randomly across firms |
# | 3 | Generates `VINTAGE_YEAR` and aligns `CLOSE_DATE` within that year |
# | 4 | Randomly selects `FUND_LOCATION` and infers `COUNTRY`, `REGION_BLOCK`, and `BASECURRENCYCODE` |
# | 5 | Computes `FUND_SIZE` based on strategy, country, and scaling rules (e.g., U.S. funds are 20% larger) |
# | 6 | Constructs a unique `FUND_NAME` using firm, strategy, and suffix (e.g., “Photon Capital Innovation Fund IV”) |
# | 7 | Assigns unique `PORTFOLIOCODE` (e.g., `FND0001`) for internal tracking |
# | 8 | Generates `PRODUCTCODE` based on strategy and region (e.g., `VC_EARLY_NA`) |
# | 9 | Returns final dataset as `portfolio_general_info_df` with full metadata for integration |
#
# ---
#
# ### Output Schema: `portfolio_general_info_df`
#
# | Column              | Description |
# |---------------------|-------------|
# | `PORTFOLIOCODE`     | Unique fund ID (e.g., `FND0001`) |
# | `FIRM_NAME`         | Synthetic VC firm name (e.g., `Photon Capital`) |
# | `FUND_NAME`         | Full fund name with suffix (e.g., `Photon Capital Innovation Fund IV`) |
# | `STRATEGY`          | Fund strategy: `Early Stage`, `General`, or `Later Stage` |
# | `VINTAGE_YEAR`      | Launch year of the fund |
# | `CLOSE_DATE`        | Date the fund closed (format: `YYYY-MM-DD`) |
# | `FUND_SIZE_MILLIONS`| Committed capital in millions of local currency |
# | `FUND_LOCATION`     | City or hub where the fund is based |
# | `COUNTRY`           | Country corresponding to the location |
# | `BASECURRENCYCODE`  | Currency code (e.g., `USD`, `EUR`) |
# | `PRODUCTCODE`       | Internal product group code (e.g., `VC_EARLY_AS`, `VC_GEN_NA`) |
# | `PORTFOLIOCATEGORY` | Always set to `"Fund"` |
# | `STRATEGY_ABBR`     | Short code for strategy (e.g., `EARLY`, `GEN`, `LATE`) |
# | `REGION_BLOCK`      | Geographic block (e.g., `NA`, `EU`, `AS`, `GL`) |
#
# ---

import random
import pandas as pd

# Utility: Ensure unique FUND_NAMEs
def generate_unique_fund_name(existing_names, firm, base_name):
    if base_name not in existing_names:
        return base_name
    i = 2
    while f"{base_name} #{i}" in existing_names:
        i += 1
    return f"{base_name} #{i}"

# Main generator function
def generate_synthetic_portfolio(n=100, seed=42):
    random.seed(seed)

    strategies = ["Early Stage", "General", "Later Stage"]
    strategy_abbr = {"Early Stage": "EARLY", "General": "GEN", "Later Stage": "LATE"}
    region_map = {
        "United States": "NA", "Canada": "NA",
        "United Kingdom": "EU", "Germany": "EU", "France": "EU",
        "Japan": "AS", "South Korea": "AS"
    }

    locations = ["San Francisco", "New York", "London", "Berlin", "Paris", "Toronto", "Tokyo", "Seoul"]
    countries = {
        "San Francisco": "United States", "New York": "United States", "London": "United Kingdom",
        "Berlin": "Germany", "Paris": "France", "Toronto": "Canada", "Tokyo": "Japan", "Seoul": "South Korea"
    }
    currency_map = {
        "United States": "USD", "United Kingdom": "GBP", "Germany": "EUR",
        "France": "EUR", "Canada": "CAD", "Japan": "JPY", "South Korea": "KRW"
    }

    firms = [
        "Summit Bridge Capital", "Redwood Partners", "NorthPoint Ventures", "Photon Capital",
        "Horizon Growth Partners", "BlueRock Ventures", "Global Gate Capital", "NextEdge Advisors",
        "Vertex Frontier Partners", "IronHill Ventures"
    ]

    strategy_name_templates = {
        "Early Stage": ["Seed Fund", "Innovation Fund", "Early Stage Fund"],
        "General": ["Opportunity Fund", "Flagship Fund", "Select Fund"],
        "Later Stage": ["Growth Fund", "Expansion Fund", "Crossover Fund"]
    }

    funds = []
    fund_names_set = set()

    for i in range(n):
        firm = random.choice(firms)
        strategy = random.choice(strategies)

        # Vintage year
        vintage = random.choice(range(2016, 2025)) if strategy == "Early Stage" else random.choice(range(2010, 2023))

        # Location logic
        loc = random.choice(locations)
        country = countries[loc]
        region = region_map.get(country, "GL")  # Default to "GL" if not found
        currency = currency_map[country]

        # Close date
        close_date = f"{vintage}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

        # Fund size
        if strategy == "Early Stage":
            fund_size = round(random.uniform(100, 500), 2)
        elif strategy == "General":
            fund_size = round(random.uniform(300, 1500), 2)
        else:
            fund_size = round(random.uniform(1000, 5000), 2)
        if country == "United States":
            fund_size *= 1.2

        # Fund name
        suffix = random.choice(["II", "III", "IV", "V", "VI", "VII", "VIII", "IX"])
        fund_prefix = random.choice(strategy_name_templates[strategy])
        base_name = f"{firm} {fund_prefix} {suffix}"
        fund_name = generate_unique_fund_name(fund_names_set, firm, base_name)
        fund_names_set.add(fund_name)

        # Final PRODUCTCODE logic (region + strategy based)
        strategy_code = strategy_abbr[strategy]
        product_code = f"VC_{strategy_code}_{region}"

        fund = {
            "PORTFOLIOCODE": f"FND{str(i+1).zfill(4)}",
            "FIRM_NAME": firm,
            "FUND_NAME": fund_name,
            "STRATEGY": strategy,
            "VINTAGE_YEAR": vintage,
            "CLOSE_DATE": close_date,
            "FUND_SIZE_MILLIONS": round(fund_size, 2),
            "FUND_LOCATION": loc,
            "COUNTRY": country,
            "BASECURRENCYCODE": currency,
            "PRODUCTCODE": product_code,
            "PORTFOLIOCATEGORY": "Fund",
            "STRATEGY_ABBR": strategy_code,
            "REGION_BLOCK": region
        }
        funds.append(fund)

    return pd.DataFrame(funds)

# Generate updated portfolio
portfolio_general_info_df = generate_synthetic_portfolio(n=100)
portfolio_general_info_df


if __name__ == "__main__":
    # Generate synthetic portfolio and export to CSV
    portfolio_general_info_df = generate_synthetic_portfolio(n=100)
    portfolio_general_info_df.to_csv("portfolio_general_info.csv", index=False)
