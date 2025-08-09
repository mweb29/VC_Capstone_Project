"""
The `BENCHMARK_ACCOUNT_ASSOCIATION` table defines the relationship between
accounts and the benchmarks they are evaluated against. This association
layer serves as the **join table** that connects account-level data with 
benchmark-level return and characteristic information.

Each row links an account (identified by `ACCOUNT_ID`) to one of its assigned
 benchmarks, with an ordinal `RANK` indicating priority.

---

### Generation Logic

- We use the list of account IDs from the **ACCOUNTS_MASTER** table (e.g., `ACC0001` through `ACC0050`).  
- Each account is randomly assigned **2 or 3 unique benchmarks** from the set of available benchmark codes.
- The `RANK` field indicates the priority: 1 (primary), 2 (secondary), 3 (tertiary), in assignment order.

---

| Field            | Description                                                   |
|------------------|---------------------------------------------------------------|
| `ACCOUNT_ID`     | Internal code for the account (FK to `ACCOUNTS_MASTER` table) |
| `BENCHMARK_CODE` | Reference to a benchmark (e.g., `PB_EU_HC`)                   |
| `RANK`           | Priority level: 1 (primary), 2 (secondary), 3 (tertiary)      |

---

Notes:
- This table enables flexible benchmarking: an account can be compared against 
multiple benchmarks, supporting composite benchmarking, relative performance 
reporting, and peer analysis.
- There are no duplicate (`ACCOUNT_ID`, `BENCHMARK_CODE`) pairs—each 
association is unique.
"""

import pandas as pd
import random

# Import the necessary information
df_benchmark_general = pd.read_csv("CSVs/benchmark_general.csv")

# 1. Simulate 50 account IDs (ACC0001, ACC0002, ...)
NUM_ACCOUNTS = 50
ACCOUNT_IDS = [f"ACC{i+1:04}" for i in range(NUM_ACCOUNTS)]

BENCHMARK_CODES = df_benchmark_general["BENCHMARK_CODE"].tolist()

# 2. Build the association rows
assoc_rows = []
for account in ACCOUNT_IDS:
    # each account gets between 2 and 3 distinct benchmarks
    chosen = random.sample(BENCHMARK_CODES, k=random.randint(2, 3))
    for rank, bench in enumerate(chosen, start=1):
        assoc_rows.append({
            "ACCOUNT_ID":    account,
            "BENCHMARK_CODE": bench,
            "RANK":          rank
        })

# 3. Create the DataFrame
df_benchmark_account_association = pd.DataFrame(assoc_rows)

# 4. Validation: ensure no duplicates in account–benchmark pairing
if df_benchmark_account_association.duplicated(subset=["ACCOUNT_ID", "BENCHMARK_CODE"]).any():
    raise ValueError("Duplicate ACCOUNT_ID–BENCHMARK_CODE pairs found!")

# Display the full association table
print("BENCHMARK_ACCOUNT_ASSOCIATION")
print(df_benchmark_account_association.head())

df_benchmark_account_association.to_csv("CSVs/benchmark_account_association.csv", index=False)


# -- Snowflake SQL table creation

# CREATE TABLE BENCHMARK_ACCOUNT_ASSOCIATION (
#     ACCOUNT_ID      VARCHAR(20),                -- Account code (FK to ACCOUNTS_MASTER table, e.g., ACC0001)
#     BENCHMARK_CODE  VARCHAR(50),                -- Linked benchmark (FK to BENCHMARK_GENERAL_INFORMATION)
#     RANK            NUMBER(1),                  -- Importance: 1=primary, 2=secondary, 3=tertiary

#     PRIMARY KEY (ACCOUNT_ID, BENCHMARK_CODE),
#     FOREIGN KEY (ACCOUNT_ID) REFERENCES ACCOUNTS_MASTER(ACCOUNT_ID),
#     FOREIGN KEY (BENCHMARK_CODE) REFERENCES BENCHMARK_GENERAL_INFORMATION(BENCHMARK_CODE)
# )