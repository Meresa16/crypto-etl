# import pandas as pd
# import requests
# import os
# from datetime import datetime
# from google.oauth2 import service_account

# # 1. SETUP CONFIGURATION
# # We use environment variables so we don't hardcode passwords
# PROJECT_ID = 'loyal-weaver-471905-p9' # REPLACE THIS with your actual Project ID from Step 1
# DATASET_ID = 'crypto_raw'
# TABLE_ID = 'daily_market'
# CREDENTIALS_PATH = './gcp_key.json' # The file you downloaded

# # 2. EXTRACT DATA (CoinGecko API)
# print("Extracting data...")
# url = "https://api.coingecko.com/api/v3/coins/markets"
# params = {
#     'vs_currency': 'usd',
#     'order': 'market_cap_desc',
#     'per_page': 50,
#     'page': 1
# }
# response = requests.get(url, params=params)
# data = response.json()

# # 3. TRANSFORM (Lightweight Python cleaning)
# df = pd.DataFrame(data)
# # Keep only necessary columns
# df = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'last_updated']]
# # Add a timestamp so we know when we loaded this
# df['loaded_at'] = datetime.now()

# # 4. LOAD TO BIGQUERY
# print("Loading to BigQuery...")

# # Check if we are running locally or on GitHub Actions
# if os.path.exists(CREDENTIALS_PATH):
#     credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
# else:
#     # Later, GitHub Actions will handle auth automatically
#     credentials = None 

# df.to_gbq(
#     destination_table=f"{DATASET_ID}.{TABLE_ID}",
#     project_id=PROJECT_ID,
#     if_exists='append', # CRITICAL: This keeps history!
#     credentials=credentials
# )

# print("Success! Data loaded.")







import pandas as pd
import pandas_gbq # Make sure to install this in requirements.txt
import requests
import os
import json
from datetime import datetime
from google.oauth2 import service_account

# --- CONFIGURATION ---
PROJECT_ID = 'loyal-weaver-471905-p9' 
DATASET_ID = 'crypto_raw'
TABLE_ID = 'daily_market'
CREDENTIALS_PATH = './gcp_key.json'

# 1. EXTRACT
print("Extracting data...")
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 50,
    'page': 1
}

try:
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
except Exception as e:
    print(f"API Error: {e}")
    exit(1)

# 2. TRANSFORM
df = pd.DataFrame(data)
df = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'last_updated']]
df['loaded_at'] = datetime.now()

# 3. AUTHENTICATION (With Debugging)
print(f"Loading to BigQuery Project: {PROJECT_ID}...")

credentials = None
if os.path.exists(CREDENTIALS_PATH):
    try:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
        print("✅ Credentials loaded successfully.")
    except json.JSONDecodeError:
        print("❌ CRITICAL ERROR: The gcp_key.json file is corrupt.")
        print("This usually means the GitHub Secret is missing quotes.")
        # Print the first 20 chars to show the format (safe, doesn't leak full key)
        with open(CREDENTIALS_PATH, 'r') as f:
            print(f"File preview: {f.read(50)}...") 
        exit(1)
else:
    print("⚠️ Warning: No 'gcp_key.json' found. Using default environment...")

# 4. LOAD (Using the modern library)
try:
    pandas_gbq.to_gbq(
        df,
        destination_table=f"{DATASET_ID}.{TABLE_ID}",
        project_id=PROJECT_ID,
        if_exists='append',
        credentials=credentials,
    )
    print("Success! Data loaded.")
except Exception as e:
    print(f"BigQuery Load Error: {e}")
    exit(1)