"""
OOP version of a synthetic fund manager assignment generator.
This class simulates the process of assigning experienced managers
from a randomly generated pool to a set of venture capital funds.
"""

import requests
import pandas as pd
import random

class FundManagerAssigner:
    def __init__(self, n_funds=100, n_managers=80):
        self.N_FUNDS = n_funds
        self.N_MANAGERS = n_managers
        self.MIN_EXP = 15
        self.MAX_EXP = 30
        self.POSITIONS = ["Managing Partner", "Principal", "Investment Partner"]
        self.FUND_IDS = [f"FUND{str(i).zfill(4)}" for i in range(1, self.N_FUNDS + 1)]
        self.df_managers = None
        self.df_assignments = None

    
    # Convert this to a JSON file
    def fetch_manager_pool(self):
        """Fetch manager names & assign experience. Store as DataFrame."""
        r = requests.get(f"https://randomuser.me/api/?results={self.N_MANAGERS}&nat=us")
        r.raise_for_status()
        users = r.json()["results"]
        names = [f"{u['name']['first']} {u['name']['last']}" for u in users]
        exp = [random.randint(self.MIN_EXP, self.MAX_EXP) for _ in range(self.N_MANAGERS)]
        ids = [f"MNGR{str(i+1).zfill(3)}" for i in range(self.N_MANAGERS)]
        self.df_managers = pd.DataFrame({
            "ManagerID": ids,
            "ManagerName": names,
            "YearsExperience": exp
        })

    def assign_to_funds(self):
        """Assign two managers to each fund while tracking max 3 assignments and experience."""
        if self.df_managers is None:
            raise ValueError("Must call fetch_manager_pool first.")

        assign_counts = {mid: 0 for mid in self.df_managers["ManagerID"]}
        years_left = {mid: int(exp) for mid, exp in zip(self.df_managers["ManagerID"], self.df_managers["YearsExperience"])}

        records = []
        for fund in self.FUND_IDS:
            available = [mid for mid in assign_counts if assign_counts[mid] < 3 and years_left[mid] > 0]
            if len(available) < 2:
                available = [mid for mid in assign_counts if assign_counts[mid] < 3 and years_left[mid] > 0]
            chosen = random.sample(available, 2)
            random.shuffle(chosen)

            for rank, mid in enumerate(chosen, 1):
                assign_counts[mid] += 1
                mgr = self.df_managers[self.df_managers["ManagerID"] == mid].iloc[0]
                role = random.choice(self.POSITIONS)
                remaining = years_left[mid]
                assignments_left = 3 - assign_counts[mid] + 1
                max_years = max(remaining - (assignments_left - 1), 1)
                years_on_fund = random.randint(1, max_years)
                years_left[mid] -= years_on_fund

                records.append({
                    "FundID": fund,
                    "ManagerID": mid,
                    "ManagerName": mgr["ManagerName"],
                    "Position": role,
                    "Rank": rank,
                    "YearsOnFund": years_on_fund,
                    "YearsExperience": mgr["YearsExperience"]
                })

        self.df_assignments = pd.DataFrame(records)

    def get_assignments(self):
        """Return the final DataFrame of manager-fund assignments."""
        if self.df_assignments is None:
            raise ValueError("Assignments have not been generated yet.")
        return self.df_assignments

if __name__ == '__main__':
    assigner = FundManagerAssigner()
    assigner.fetch_manager_pool()
    assigner.assign_to_funds()
    df = assigner.get_assignments()
    print(df.head(10))