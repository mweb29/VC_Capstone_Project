# Portfolio Summary

## 1. What does the folder do?
Generates and maps core portfolio data for the VC simulation. Provides fund metadata, LP account generation, LP to fund mapping, and fund manager assignments.

**Scripts:**

**portfolio_general_info.py** — Creates synthetic VC fund metadata including fund name, strategy, vintage, size, base location, currency, and product code.

**account.py** — Builds a set of institutional and individual LP accounts. Adds committed capital, NAV, FX conversion, investment count, and inception date. 
**portfolio_account_association.py** — Creates the many-to-many mapping of LP accounts to funds with allocation amounts and dates.

**fund_manager.py** — Reads manager pool from JSON and assigns exactly two managers per fund with role and experience constraints.

## 2. Role in the Overall Project
Provides the foundational entities (funds, LPs, managers) that feed into performance, holdings, and benchmarking modules

## 3. How it Works
**portfolio_general_info.py**
- Simulate 100 funds with firm, strategy, vintage, target size, domicile, and currency.
- Generate product and readable names to align with product-level reporting.
- Output `portfolio_general_info.csv`.

**account.py**
- Create 25 named institutional LPs and 25 synthetic individuals.
- Enrich with FX rate lookup, committed capital, current NAV, investment count, and start date.
- Output `accounts.csv`.

**portfolio_account_association.py**
- Randomly assign each LP to 1 to 2 funds to ensure a realistic many-to-many structure.
- Record commitment amounts and effective dates.
- Output `portfolio_account_map.csv`.

**fund_manager.py**
- Load managers from `manager_data.json`.
- Enforce constraints: exactly two managers per fund and each manager on at most three funds.
- Assign roles from a predefined set and store experience attributes.
- Output `fund_managers.csv`.

## 4. Assumptions
- LPs can participate in multiple funds.
- FX rates are static and sourced from JSON lookup for this phase.
- Fund sizes, commitments, NAV, and experience are randomly generated within bounded ranges.
- Each fund has exactly two managers.
- Manager roles are randomly selected from a curated list.

## 5. Future Expansion
- Add time-series NAV and AUM with capital call and distribution schedules.
- Simulate FX rate changes over time and revalue NAV accordingly.
- Track manager performance history and prior fund records.
- Persist outputs to Snowflake and expose portfolio to benchmark joins and attribution.
