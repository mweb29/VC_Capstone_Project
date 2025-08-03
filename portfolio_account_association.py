# # Portfolio-Account Association
#
# ### Fund–Account Mapping Logic
#
# This section creates the `portfolio_account_map_df`, which links each LP account to its committed VC funds.  
# It reflects how each investor (institutional or individual) allocates capital across multiple portfolios.
#
# ---
#
# ### Processing Steps
#
# | Step | Description |
# |------|-------------|
# | 1    | Load list of `PORTFOLIOCODE`s and `ACCOUNTID`s from previous tables |
# | 2    | Copy the fund list into `available_funds` to manage sampling without reuse |
# | 3    | For each account: Read `Number of Funds`, Randomly assign that many funds, If insufficient unique funds remain, sample with replacement from full pool |
# | 4    | Append each `(ACCOUNTID, PORTFOLIOCODE)` pair to the mapping list |
# | 5    | Construct the final `portfolio_account_map_df` DataFrame from the mapping |
#
# ---
#
# ### Output Schema: `portfolio_account_map_df`
#
# | Column | Description |
# |--------|-------------|
# | PORTFOLIOCODE | VC fund identifier (e.g., FND0032) |
# | ACCOUNTID | LP account identifier (e.g., ACC0010) |

import pandas as pd
import random

# 1. Prepare portfolio and account ID lists
portfolio_codes = portfolio_general_info_df["PORTFOLIOCODE"].tolist()
account_ids = accounts_df["Account ID"].tolist()

# Copy the full fund list to manage duplicates
available_funds = portfolio_codes.copy()

# 2. Create mapping between accounts and portfolios
mapping = []
for _, row in accounts_df.iterrows():
    account_id = row["Account ID"]
    num_funds = int(row["Number of Funds"])

    # If not enough unique funds left, sample with replacement from full pool
    if num_funds > len(available_funds):
        selected_funds = random.sample(portfolio_codes, num_funds)
    else:
        selected_funds = random.sample(available_funds, num_funds)
        # Remove selected funds to prevent reuse
        available_funds = [f for f in available_funds if f not in selected_funds]

    # Append account-fund pairs
    for fund in selected_funds:
        mapping.append({
            "PORTFOLIOCODE": fund,
            "ACCOUNTID": account_id
        })

# 3. Create final mapping DataFrame
portfolio_account_map_df = pd.DataFrame(mapping)
portfolio_account_map_df


# Count number of funds per account
funds_per_account = portfolio_account_map_df.groupby("ACCOUNTID")["PORTFOLIOCODE"].nunique().reset_index()
funds_per_account.columns = ["ACCOUNTID", "Num_Funds"]
funds_per_account


if __name__ == "__main__":
    # Map accounts to portfolios and export to CSV
    portfolio_account_map_df = pd.DataFrame(mapping)
    portfolio_account_map_df.to_csv("portfolio_account_map.csv", index=False)
