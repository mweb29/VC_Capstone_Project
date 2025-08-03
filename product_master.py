# # Product Master
#
# ### Product Master Metadata Logic  
# This section creates a metadata table `product_master_df` for each unique VC product derived from fund-level data.  
# Each product represents a group of funds sharing the same **strategy** and **region**, and is enriched with human-readable names, vehicle structure, and share class tags.
#
# ---
#
# ### Processing Steps
#
# | Step | Description |
# |------|-------------|
# | 1 | Derive unique `(strategy, region)` combinations from `portfolio_general_info_df` |
# | 2 | Map strategy abbreviations (e.g., `EARLY`) to full strategy names (e.g., `Early Stage`) |
# | 3 | Assign product codes in the format `VC_{STRATEGY_ABBR}_{REGION_BLOCK}` |
# | 4 | Randomly assign vehicle type and determine vehicle category |
# | 5 | Generate product name (e.g., `Assette Growth Equity Fund IV`) |
# | 6 | Assign share class as Institutional or Offshore |
# | 7 | Store output in `product_master_df` with 1 row per product group |
#
# ---
#
# ### Output Schema: `product_master_df`
#
# | Column           | Description |
# |------------------|-------------|
# | `PRODUCTCODE`    | Internal product code (e.g., `VC_EARLY_NA`) |
# | `PRODUCTNAME`    | Human-readable product name (e.g., `Assette Growth Equity Fund II`) |
# | `STRATEGY`       | Full strategy name (e.g., `Early Stage`, `Venture Capital`, `Growth Equity`) |
# | `VEHICLETYPE`    | Structure type (e.g., `Separate Account`, `Commingled Fund`) |
# | `VEHICLECATEGORY`| Segregation level (`Segregated` or `Pooled`) |
# | `ASSETCLASS`     | Always set to `Venture Capital` |
# | `SHARECLASS`     | Assigned as `Institutional` or `Offshore` |
# | `REGION_BLOCK`   | Regional classification (e.g., `NA`, `EU`, `AS`, or `GL`)

import pandas as pd
import random

# 1. Strategy & Region Mapping
strategy_abbr = {"Early Stage": "EARLY", "General": "GEN", "Later Stage": "LATE"}
strategy_name_map = {
    "EARLY": "Early Stage",
    "GEN": "Venture Capital",
    "LATE": "Growth Equity"
}
region_map = {
    "United States": "NA", "Canada": "NA",
    "United Kingdom": "EU", "Germany": "EU", "France": "EU",
    "Japan": "AS", "South Korea": "AS"
}

# 2. Add derived columns to portfolio
portfolio_general_info_df["STRATEGY_ABBR"] = portfolio_general_info_df["STRATEGY"].map(strategy_abbr)
portfolio_general_info_df["REGION_BLOCK"] = portfolio_general_info_df["COUNTRY"].map(region_map).fillna("GL")
portfolio_general_info_df["PRODUCT_GROUP"] = (
    portfolio_general_info_df["STRATEGY_ABBR"] + "_" + portfolio_general_info_df["REGION_BLOCK"]
)

# 3. Assign PRODUCTCODEs per group
unique_groups = portfolio_general_info_df["PRODUCT_GROUP"].unique()
productcode_lookup = {
    group: f"VC_{group}" for group in sorted(unique_groups)
}
portfolio_general_info_df["PRODUCTCODE"] = portfolio_general_info_df["PRODUCT_GROUP"].map(productcode_lookup)

# 4. Generate Product Master
vehicle_types = ["Separate Account", "Commingled Fund"]
vehicle_categories = {"Separate Account": "Segregated", "Commingled Fund": "Pooled"}

def assign_shareclass(vt):
    return "Institutional" if "SEPARATE" in vt.upper() else random.choice(["Institutional", "Offshore"])

def generate_product_name(strategy_abbr):
    firm = "Assette"
    label = strategy_name_map[strategy_abbr]
    suffix = random.choice(["Fund I", "Fund II", "Fund III", "Fund IV"])
    return f"{firm} {label} {suffix}"

product_rows = []
for group_key, product_code in productcode_lookup.items():
    strategy_abbr, region = group_key.split("_")
    vt = random.choice(vehicle_types)
    product_rows.append({
        "PRODUCTCODE": product_code,
        "PRODUCTNAME": generate_product_name(strategy_abbr),
        "STRATEGY": strategy_name_map[strategy_abbr],
        "VEHICLECATEGORY": vehicle_categories[vt],
        "VEHICLETYPE": vt,
        "ASSETCLASS": "Venture Capital",
        "SHARECLASS": assign_shareclass(vt),
        "REGION_BLOCK": region
    })

product_master_df = pd.DataFrame(product_rows)
product_master_df


if __name__ == "__main__":
    # Generate product master metadata and export to CSV
    product_master_df = pd.DataFrame(product_rows)
    product_master_df.to_csv("product_master.csv", index=False)
