

# import pandas as pd
# import pandas_gbq # Make sure to install this in requirements.txt
# import requests
# import os
# import json
# from datetime import datetime
# from google.oauth2 import service_account

# # --- CONFIGURATION ---
# PROJECT_ID = 'loyal-weaver-471905-p9' 
# DATASET_ID = 'crypto_raw'
# TABLE_ID = 'daily_market'
# CREDENTIALS_PATH = './gcp_key.json'

# # 1. EXTRACT
# print("Extracting data...")
# url = "https://api.coingecko.com/api/v3/coins/markets"
# params = {
#     'vs_currency': 'usd',
#     'order': 'market_cap_desc',
#     'per_page': 50,
#     'page': 1
# }

# try:
#     response = requests.get(url, params=params)
#     response.raise_for_status()
#     data = response.json()
# except Exception as e:
#     print(f"API Error: {e}")
#     exit(1)

# # 2. TRANSFORM
# df = pd.DataFrame(data)
# df = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'last_updated']]
# df['loaded_at'] = datetime.now()

# # 3. AUTHENTICATION (With Debugging)
# print(f"Loading to BigQuery Project: {PROJECT_ID}...")

# credentials = None
# if os.path.exists(CREDENTIALS_PATH):
#     try:
#         credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
#         print("✅ Credentials loaded successfully.")
#     except json.JSONDecodeError:
#         print("❌ CRITICAL ERROR: The gcp_key.json file is corrupt.")
#         print("This usually means the GitHub Secret is missing quotes.")
#         # Print the first 20 chars to show the format (safe, doesn't leak full key)
#         with open(CREDENTIALS_PATH, 'r') as f:
#             print(f"File preview: {f.read(50)}...") 
#         exit(1)
# else:
#     print("⚠️ Warning: No 'gcp_key.json' found. Using default environment...")

# # 4. LOAD (Using the modern library)
# try:
#     pandas_gbq.to_gbq(
#         df,
#         destination_table=f"{DATASET_ID}.{TABLE_ID}",
#         project_id=PROJECT_ID,
#         if_exists='append',
#         credentials=credentials,
#     )
#     print("Success! Data loaded.")
# except Exception as e:
#     print(f"BigQuery Load Error: {e}")
#     exit(1)





import pandas as pd
import pandas_gbq
import requests
import os
import json
from datetime import datetime
from google.oauth2 import service_account

# --- CONFIGURATION ---
PROJECT_ID = 'loyal-weaver-471905-p9' 
DATASET_ID = 'crypto_raw'
TABLE_ID = 'daily_market'
CREDENTIALS_PATH = 'gcp_key.json'

# 1. EXTRACT
print("Extracting data...")
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 50,
    'page': 1
}
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

try:
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
except Exception as e:
    print(f"API Error: {e}")
    exit(1)

# 2. TRANSFORM
df = pd.DataFrame(data)

# --- THE UPGRADE: Select MORE Columns ---
desired_columns = [
    'id', 'symbol', 'name', 'image', 
    'current_price', 'market_cap', 'market_cap_rank', 'total_volume',
    'high_24h', 'low_24h', 
    'price_change_percentage_24h', 
    'circulating_supply', 'total_supply', 'ath', 'ath_change_percentage',
    'last_updated'
]

# Keep only columns that exist in the API response (safety check)
cols_to_keep = [col for col in desired_columns if col in df.columns]
df = df[cols_to_keep]

df['loaded_at'] = datetime.now()

# 3. AUTHENTICATION
print(f"Loading to BigQuery Project: {PROJECT_ID}...")
credentials = None
if os.path.exists(CREDENTIALS_PATH):
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    except json.JSONDecodeError:
        print("❌ CRITICAL ERROR: The gcp_key.json file is corrupt.")
        exit(1)

# 4. LOAD
try:
    pandas_gbq.to_gbq(
        df,
        destination_table=f"{DATASET_ID}.{TABLE_ID}",
        project_id=PROJECT_ID,
        if_exists='append',
        credentials=credentials,
        # This allows BigQuery to add the new columns automatically
        table_schema=[{'name': 'image', 'type': 'STRING'}, {'name': 'market_cap_rank', 'type': 'INTEGER'}] 
    )
    print("Success! Data loaded.")
except Exception as e:
    print(f"BigQuery Load Error: {e}")
    exit(1)