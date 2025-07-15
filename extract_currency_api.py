"""
Script to generate a local JSON file containing currency information
(FX rates, currency code, and currency name) from restcountries API,
to avoid repeated API calls in other scripts. This is limited to a select
number based on our LP and individual account scripts. This could be expanded
to include more countries as needed, but for now we focus on the ones we have.
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
