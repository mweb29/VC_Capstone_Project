# VC_Capstone_Project
## ERD.dot
- This file helps visualize the different important primary and foreign keys for the
tables within Snowflake. It is an easy way for anyone to quickly upload synthetic data
into the database. This documentation also allows for us to build out additoinal tables
easily since we can properly visualize how they will all connect back.

## institutional_data.py
 - This file works to create the institutional level data that is required. There
 is some larger organizations that contribute capital. This information was pulled
 from Pitchbook. However, there is an additional set of investors and these are
 generated randomly by using the Faker library. This is a quick and effective way
 to create a lot of fake names and other attributes.

## countries_api.py
- This file uses an API to get information regarding countries. It contains 
their name, abbreviation (ISO2), region, subregion, and currency. This 
information is then stored as a JSON file for later use within other scripts. 
The resulting JSON is named, synthetic_countries.json.

## data_generation.py
- This file was our initial data generation that we used as a framework for our
subsequent files. The jupyter notebooks that are in this repository are the
most up to date code segments that generate informaton for the VC portfolio. 
This file leads to the creation of vc_synthetic_data_20250706_2112.xlsx.

## extract_currency_api.py
- This script is designed to extract currency exchange rates from a specified 
API. It utilizes the `requests` library to fetch data and processes it to 
provide users with up-to-date currency information. Like our other API collection
files, this also stores the results as a JSON file.

## sectors.py
- Like the currency API above, this script is designed to extract industry
sectors from a specified API. It utilizes the `requests` library to fetch data 
and processes it to provide users with up-to-date sector information. Like our 
other API collection files, this also stores the results as a JSON file.

## holdings.py
- This is the framework that we are going to build out in relation to the 
holdings table that is present in snowflake. This is going to be the base layer
that our product, portfolio, and performance tables will use to have a dynamic
input. The setup will expand to upload information directly to Snowflake, once
we get the exact output that we are looking for.

## institutional_data.py
- This file was the original setup for what is present in Jooyeon's jupyter
notebook. As we continue to iterate, we will combine what is in her notebook 
and what remains in here. Please use her notebook as the most up to date version
in order to understand the progress that has been made.

## Benchmark.ipynb
- This notebook simulates benchmark performance data tailored for venture capital strategies. It aligns with Assette’s Snowflake schema and models synthetic returns using industry-informed patterns—such as J-curve behavior, vintage-year clustering, and top-quartile dispersion.It produces four structured tables:

**BENCHMARKGENERALINFORMATION**: Contains metadata for 20 synthetic benchmarks, including unique codes, sector/geography-based names, provider, and region. Acts as the anchor table for all benchmark joins.

**BENCHMARKCHARACTERISTICS**: Provides static summary statistics such as median IRR, MOIC, and fund count, along with associated units and currencies.

**BENCHMARKPERFORMANCE**: Stores time-series return data (quarterly, YTD, 1Y, 3Y, 5Y, 10Y) for each benchmark, enabling trend analysis across key VC metrics like IRR and MOIC.

**PORTFOLIOBENCHMARKASSOCIATION**: Links 100 simulated VC portfolios to appropriate benchmarks via foreign keys, supporting composite benchmarking and performance attribution.
This synthetic dataset is designed for seamless integration into a fact sheet automation pipeline.

## factsheet_V2.ipynb
- This notebook simulates fact sheet components for Venture Capital portfolios, aligning with Assette’s Snowflake schema and business logic. It produces synthetic yet institutionally consistent data across key analytical domains needed for automated reporting and client presentations.

**portfolio_general_info_df**
Stores metadata for 100 synthetic VC portfolios, based on real PitchBook fund exports and extended via simulation. Includes fund name, strategy, close date, fund size, location, base currency, and unique product codes.

**accounts_df**
Simulates both institutional and individual limited partners (LPs). Each record includes LP name, type, country, account currency, committed capital (local & USD), NAV, and number of funds committed to.

**portfolio_account_map_df**
Creates a many-to-many relationship between LP accounts and VC portfolios, using the Number of Funds field from accounts_df to allocate investments. Ensures no duplicate assignments per account unless replacement is needed.

**product_master_df**
Generates standardized product master data based on PRODUCTCODE. Adds fields such as strategy, asset class, vehicle type/category, and share class. Supports reporting and classification of portfolio products.

## synthetic_countries.json
- The resulting file from the call to countries_api.py

## currency_lookup.json
- The resulting file from the call to extract_currency_api.py

## sectors.json
- The resulting file from the call to sectors.py

## gics.json
- The resulting file from the call to sectors.py

## vc_synthetic_data_20250706_2112.xlsx
- This was one of our original synthetic data creations. It was used as a 
framework for what we wanted to do, but it is not longer the standard of 
what we are building towards. It is the result of running data_generation.py
