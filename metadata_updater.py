import time
import requests
import zipfile
import pandas as pd
import schedule
from datetime import datetime
from io import BytesIO
from sqlalchemy import create_engine

DB_URL = "postgresql://admin:password123@localhost:5432/market_data"

ICICI_ZIP_URL = "https://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip"
NSE_NIFTY50_URL = "https://niftyindices.com/IndexConstituent/ind_nifty50list.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

def update_metadata():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting daily metadata refresh...")

    try:
        print('Downloading & Extracting ICICI Security Master zip...')
        icici_res = requests.get(ICICI_ZIP_URL)

        with zipfile.ZipFile(BytesIO(icici_res.content)) as z:
            with z.open("NSEScripMaster.txt") as f:
                icici_df = pd.read_csv(f, skipinitialspace=True)
        
        print("Downloading NSE Nifty 50 List...")
        nse_res = requests.get(NSE_NIFTY50_URL, headers=HEADERS)
        nse_df = pd.read_csv(BytesIO(nse_res.content))

        icici_df.columns = icici_df.columns.str.replace('"','').str.strip()

        # print("Merging Database...")    
        nse_clean = nse_df[['Symbol', 'Company Name', 'Industry', 'ISIN Code']]
        icici_clean = icici_df[['Token','ShortName', 'ISINCode']]

        merged_df = pd.merge(
            icici_clean,
            nse_clean,
            left_on='ISINCode',
            right_on="ISIN Code",
            how="inner"
        )

        final_df = merged_df[['Token','ShortName', 'Company Name', 'Industry', 'ISIN Code']].rename(
            columns={"Token": "icici_token","ShortName": "ticker", "Company Name": "company_name", "Industry": "sector", "ISIN Code": "isin_code"}
        )

        print("Pushing to PostgreSQL dimension table...")
        engine = create_engine(DB_URL)

        final_df.to_sql('dim_stock_metadata', engine, if_exists='replace', index=False)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Success! dim_stock_metadata table updated.")
    
    except Exception as e:
        print(f"Failed to update metadata: {e}")

schedule.every().day.at("08:00").do(update_metadata)

print("Metadata Updater Service Started. Waiting for scheduled run...")

update_metadata()

while True:
    schedule.run_pending()
    time.sleep(60)