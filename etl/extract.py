import time
from datetime import datetime, timedelta
import pandas as pd
import requests

def extract_complaints_data():
    states_url = "https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json"
    states_dict = requests.get(states_url).json()
    list_of_states = list(states_dict.keys())

    anchor_date = datetime(2024, 8, 31)
    max_date = anchor_date.strftime("%Y-%m-%d")
    min_date = (anchor_date - timedelta(days=30)).strftime("%Y-%m-%d")
    
    size = 500
    all_complaints = []

    for state in list_of_states:
        api_url = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?size={size}&date_received_max={max_date}&date_received_min={min_date}&state={state}"
        
        try:
            response = requests.get(api_url, timeout=30)
            if response.status_code == 200:
                hits = response.json().get("hits", {}).get("hits", [])
                for hit in hits:
                    all_complaints.append(hit.get("_source", {}))
                print(f"State {state}: {len(hits)} records")
        except Exception:
            pass

        time.sleep(1)

    df_raw = pd.DataFrame(all_complaints)
    return df_raw

if __name__ == "__main__":
    df = extract_complaints_data()
    print("Total rows fetched:", len(df))
    print(df.head())