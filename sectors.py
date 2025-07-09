import pandas as pd
# Use the functions from countries_api.py so we do not have to rewrite functions
from countries_api import json_output, connect_to_snowflake, upload_to_snowflake_country

def get_gics_sectors():
# Load full GICS classification from GitHub Gist
    url = "https://gist.githubusercontent.com/uknj/c9bcf66ab379a35fcc8758f9a6c86ceb/raw"
    df = pd.read_csv(url)

    # Extract unique top-level sectors
    sectors_df = df[['Sector Code', 'Sector']].drop_duplicates().sort_values('Sector Code').reset_index(drop=True)

    #print("GICS Sector List:")
    # Clean table without index
    sectors = sectors_df.reset_index(drop=True)

    # Preview full GICS classification hierarchy
    #print("\n GICS Classification:")
    gics = df

    return sectors, gics


# Main execution
if __name__ == "__main__":
    """Main function to fetch and display country data.
    We will not use this at the end of the project. This is solely to make 
    sure that our output is correct and we are getting the results that we 
    expect. It allows us to validate our work before integrating it into the
    main project.
    """
    sectors, gics = get_gics_sectors()
    json_output(sectors, "sectors.json")
    json_output(gics, "gics.json")
