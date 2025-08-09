# load_to_snowflake.py
import pandas as pd
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
import glob


def create_initial_snowflake_tables():
    # Load the environment variables from the .env file
    load_dotenv()

    connection_params = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }

    # Create Snowflake session 
    session = Session.builder.configs(connection_params).create()

    for file in glob.glob("CSVs/*.csv"):
        df = pd.read_csv(file)
        table_name = os.path.splitext(os.path.basename(file))[0].upper()
        session.create_dataframe(df).write.mode("overwrite").save_as_table(table_name)
        print(f"✅ Loaded {file} into {table_name}")

    # Close the session
    session.close()

def appending_snowflake_tables(file):
    # Load the environment variables from the .env file
    load_dotenv()

    connection_params = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }

    # Create Snowflake session 
    session = Session.builder.configs(connection_params).create()
    df = pd.read_csv(file)

    session.create_dataframe(df).write.mode("append").save_as_table()

if __name__ == "__main__":
    create_initial_snowflake_tables()




