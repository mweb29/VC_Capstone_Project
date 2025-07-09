import requests
import json
import pandas as pd

# Fetch data
def fetch_countries_data():
    """Fetches country data from the mledoze/countries GitHub repository."""
    url = "https://raw.githubusercontent.com/mledoze/countries/master/countries.json"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Parse relevant fields
    countries = []
    for item in data:
        countries.append({
            "Country Name": item["name"]["common"],
            "ISO2": item["cca2"],
            "Region": item["region"],
            "Subregion": item["subregion"],
            "Currency": list(item["currencies"].keys())[0] if item.get("currencies") else None
        })

    # Display as table using Pandas
    df = pd.DataFrame(countries)
    pd.set_option('display.max_rows', 250)
    return df

# (Optional) Save to JSON file
def json_output(countries_df):
    """Saves the country data (as a DataFrame) to a JSON file."""
    with open("synthetic_countries.json", "w") as f:
        json.dump(countries_df.to_dict(orient="records"), f, indent=2)

if __name__ == "__main__":
    countries_df = fetch_countries_data()
    #print(countries_df)  # Display the DataFrame in console
    json_output(countries_df)  # Uncomment to download to JSON file