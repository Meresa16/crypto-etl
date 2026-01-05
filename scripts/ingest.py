




# import pandas as pd
# import pandas_gbq
# import requests
# import os
# import json
# from datetime import datetime
# from google.oauth2 import service_account

# # --- CONFIGURATION ---
# PROJECT_ID = 'loyal-weaver-471905-p9' 
# DATASET_ID = 'crypto_raw'
# TABLE_ID = 'daily_market'
# CREDENTIALS_PATH = 'gcp_key.json'

# # 1. EXTRACT
# print("Extracting data...")
# url = "https://api.coingecko.com/api/v3/coins/markets"
# params = {
#     'vs_currency': 'usd',
#     'order': 'market_cap_desc',
#     'per_page': 500,
#     'page': 1
# }
# headers = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
# }

# try:
#     response = requests.get(url, params=params, headers=headers, timeout=30)
#     response.raise_for_status()
#     data = response.json()
# except Exception as e:
#     print(f"API Error: {e}")
#     exit(1)

# # 2. TRANSFORM
# df = pd.DataFrame(data)

# # --- THE UPGRADE: Select MORE Columns ---
# desired_columns = [
#     'id', 'symbol', 'name', 'image', 
#     'current_price', 'market_cap', 'market_cap_rank', 'total_volume',
#     'high_24h', 'low_24h', 
#     'price_change_percentage_24h', 
#     'circulating_supply', 'total_supply', 'ath', 'ath_change_percentage',
#     'last_updated'
# ]

# # Keep only columns that exist in the API response (safety check)
# cols_to_keep = [col for col in desired_columns if col in df.columns]
# df = df[cols_to_keep]

# df['loaded_at'] = datetime.now()

# # 3. AUTHENTICATION
# print(f"Loading to BigQuery Project: {PROJECT_ID}...")
# credentials = None
# if os.path.exists(CREDENTIALS_PATH):
#     try:
#         credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
#     except json.JSONDecodeError:
#         print("❌ CRITICAL ERROR: The gcp_key.json file is corrupt.")
#         exit(1)

# # 4. LOAD
# try:
#     pandas_gbq.to_gbq(
#         df,
#         destination_table=f"{DATASET_ID}.{TABLE_ID}",
#         project_id=PROJECT_ID,
#         if_exists='append',
#         credentials=credentials,
#         # This allows BigQuery to add the new columns automatically
#         table_schema=[{'name': 'image', 'type': 'STRING'}, {'name': 'market_cap_rank', 'type': 'INTEGER'}] 
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
import time
import sys
import requests.exceptions

# --- CONFIGURATION ---
PROJECT_ID = 'loyal-weaver-471905-p9' 
DATASET_ID = 'crypto_raw'
TABLE_ID = 'daily_market'
CREDENTIALS_PATH = 'gcp_key.json'

# --- API CONSTANTS ---
COINS_PER_PAGE = 250 
BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"
RATE_LIMIT_DELAY = 1.5 # Delay between successful calls
ERROR_RETRY_DELAY = 10 # Long delay after hitting a 429 error

# --- FUNCTION: Fetch ALL Pages ---
def fetch_all_crypto_data():
    """Loops through all CoinGecko pages to get full market data."""
    all_data = []
    page = 1
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    while True:
        # Status update in console
        sys.stdout.write(f"\r🔍 Fetching page {page} (Assets {((page-1)*COINS_PER_PAGE) + 1} - {page*COINS_PER_PAGE})...")
        sys.stdout.flush()
        
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': COINS_PER_PAGE,
            'page': page
        }

        try:
            response = requests.get(BASE_URL, params=params, headers=headers, timeout=60)
            response.raise_for_status()
            page_data = response.json()
        
        except requests.exceptions.HTTPError as e:
            # Handle 429 Rate Limit specifically
            if response.status_code == 429:
                print(f"\n🛑 RATE LIMIT HIT on page {page}. Waiting for {ERROR_RETRY_DELAY}s...")
                time.sleep(ERROR_RETRY_DELAY)
                continue # Try the same page again
            else:
                print(f"\n❌ HTTP Error on page {page} (Code {response.status_code}): {e}. Retrying in 5s...")
                time.sleep(5)
                continue
        except Exception as e:
            print(f"\n❌ Connection Error on page {page}: {e}. Retrying in 5s...")
            time.sleep(5)
            continue
        
        # Check for end condition (API returns empty list)
        if not page_data:
            print(f"\n✅ Finished! Total assets loaded: {len(all_data)}")
            break
            
        all_data.extend(page_data)
        page += 1
        
        # Safe delay to prevent rate limit hits
        time.sleep(RATE_LIMIT_DELAY)

    return all_data

# --- MAIN EXECUTION ---
def main():
    print("--- ETL START ---")
    
    # 1. EXTRACT ALL DATA
    data = fetch_all_crypto_data()
    if not data:
        print("🛑 No data fetched. Exiting.")
        sys.exit(1)

    # 2. TRANSFORM (Create DataFrame)
    df = pd.DataFrame(data)
    
    # --- Select ALL Desired Columns ---
    desired_columns = [
        'id', 'symbol', 'name', 'image', 'current_price', 'market_cap', 
        'market_cap_rank', 'total_volume', 'high_24h', 'low_24h', 
        'price_change_percentage_24h', 'circulating_supply', 'total_supply', 
        'ath', 'ath_change_percentage', 'last_updated'
    ]
    cols_to_keep = [col for col in desired_columns if col in df.columns]
    df = df[cols_to_keep]

    # --- CRITICAL FIX: Explicitly cast float columns to prevent INT64 parsing errors ---
    float_cols = ['current_price', 'market_cap', 'total_volume', 'high_24h', 'low_24h', 'circulating_supply', 'total_supply', 'ath', 'ath_change_percentage', 'price_change_percentage_24h']
    for col_name in float_cols:
        if col_name in df.columns:
            # pd.to_numeric with errors='coerce' safely turns invalid strings into NaN
            df[col_name] = pd.to_numeric(df[col_name], errors='coerce') 

    # Add the load timestamp
    df['loaded_at'] = datetime.now()
    
    # 3. AUTHENTICATION & LOAD
    print(f"📦 Preparing to load {len(df)} records to BigQuery via load_csv method...")
    credentials = None
    if os.path.exists(CREDENTIALS_PATH):
        try:
            credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
        except json.JSONDecodeError:
            print("❌ CRITICAL ERROR: The gcp_key.json file is corrupt.")
            sys.exit(1)
    
    # --- FINAL STABLE LOAD BLOCK ---
    try:
        pandas_gbq.to_gbq(
            df,
            destination_table=f"{DATASET_ID}.{TABLE_ID}",
            project_id=PROJECT_ID,
            if_exists='append',
            credentials=credentials,
            # Use the robust load_csv method for maximum stability
            api_method='load_csv', 
            chunksize=2000, 
            # Explicit Schema Definition (Ensuring FLOAT for all currency/volume fields)
            table_schema=[
                {'name': 'id', 'type': 'STRING'},
                {'name': 'symbol', 'type': 'STRING'},
                {'name': 'name', 'type': 'STRING'},
                {'name': 'image', 'type': 'STRING'},
                {'name': 'current_price', 'type': 'FLOAT'},
                {'name': 'market_cap', 'type': 'FLOAT'},
                {'name': 'market_cap_rank', 'type': 'INTEGER'},
                {'name': 'total_volume', 'type': 'FLOAT'},
                {'name': 'high_24h', 'type': 'FLOAT'},
                {'name': 'low_24h', 'type': 'FLOAT'},
                {'name': 'price_change_percentage_24h', 'type': 'FLOAT'},
                {'name': 'circulating_supply', 'type': 'FLOAT'},
                {'name': 'total_supply', 'type': 'FLOAT'},
                {'name': 'ath', 'type': 'FLOAT'},
                {'name': 'ath_change_percentage', 'type': 'FLOAT'},
                {'name': 'last_updated', 'type': 'TIMESTAMP'},
                {'name': 'loaded_at', 'type': 'TIMESTAMP'} 
            ]
        )
        print(f"✅ Success! Loaded {len(df)} assets to BigQuery.")
    except Exception as e:
        print(f"🔥 CRITICAL BigQuery Load Error: Failed to load data. {e}")
        # Hint for final manual check
        if "Table has too many columns" in str(e) or "Unable to parse" in str(e):
             print("\n💡 HINT: The BigQuery table might have an old schema. Try deleting the 'daily_market' table in the BigQuery console and re-running.")
        sys.exit(1)

if __name__ == "__main__":
    main()
