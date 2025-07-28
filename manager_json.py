"""
Fetch manager information from an API and store it as JSON.
"""

import requests
import json

def fetch_manager_json(save_path="manager_data.json"):
        """Fetch manager information from API and store raw JSON and write to file."""
        r = requests.get(f"https://randomuser.me/api/?results={2000}&nat=us")
        r.raise_for_status()
        manager_json = r.json()["results"]
        with open(save_path, "w") as f:
            json.dump(manager_json, f, indent=2)


if __name__ == "__main__":
    fetch_manager_json("manager_data.json")  