"""
Description:
    This script generates a local JSON file (`currency_lookup.json`) containin
    basic currency metadata for a defined list of countries relevant to the
    venture capital account-level dataset. Currency data is retrieved from the
    RESTCountries API and includes the 3-letter currency code, currency name,
    and a fixed FX rate to USD.

    The goal is to avoid repeated real-time API calls in downstream investor
    simulations by providing a cached, reusable lookup structure. This is
    particularly important for reproducibility, performance, and offline 
    compatibility in synthetic data generation.

Output Structure:
    {
        "Country Name": {
            "currency_code": "USD",
            "currency_name": "US Dollar",
            "fx_to_usd": 1.0
        },
        ...
    }

Functions:
    - get_currency_info(country): Queries the RESTCountries API and returns 
    currency metadata.
    - build_currency_json(): Builds the currency lookup dictionary and writes
    it to a JSON file.

Output:
    - Creates a single JSON file `currency_lookup.json` containing currency data for:
        Luxembourg, United States, Canada, United Kingdom, Germany, France, 
        Australia, Netherlands, Japan, India, and Brazil.

Intended Use:
    This JSON cache will be used by investor generation scripts (`institutional` and 
    `individual` account builders) to assign currency attributes without incurring 
    additional API latency or dependency.

Example:
    Run this script once to generate the currency cache file:
        $ python currency_cache_generator.py
"""

import requests
import json

# List of countries from your LP and individual account scripts
countries = list(set([
    "Luxembourg", "United States", "Canada", "United Kingdom", "Germany", 
    "France", "Australia", "Netherlands", "Japan", "India", "Brazil"
]))

def get_currency_info(country):
    """
    Query restcountries API for currency info and return tuple:
    (currency_code, currency_name, fx_to_usd)
    """
    try:
        if country == "Unknown":
            return "USD", "US Dollar", 1.0

        r = requests.get(f"https://restcountries.com/v3.1/name/{country}")
        r.raise_for_status()
        data = r.json()[0]
        currency_code = list(data["currencies"].keys())[0]
        currency_name = data["currencies"][currency_code]["name"]

        # FX rates - manually defined snapshot for consistency
        fx_dict = {"USD": 1.0, "EUR": 1.1, "GBP": 1.3, "CAD": 0.74, "JPY": 0.0067}
        fx_to_usd = fx_dict.get(currency_code, 1.0)

        return currency_code, currency_name, fx_to_usd

    except Exception as e:
        print(f"Warning: API failed for {country}: {e}")
        return "USD", "US Dollar", 1.0

def build_currency_json():
    """
    Build dictionary for all countries and save as JSON
    """
    cache = {}
    for country in countries:
        code, name, fx = get_currency_info(country)
        cache[country] = {
            "currency_code": code,
            "currency_name": name,
            "fx_to_usd": fx
        }

    with open("currency_lookup.json", "w") as f:
        json.dump(cache, f, indent=2)

    print("Currency cache saved to 'currency_lookup.json'.")

if __name__ == "__main__":
    build_currency_json()
