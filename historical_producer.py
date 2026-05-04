import time
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from confluent_kafka import Producer
from breeze_connect import BreezeConnect
from dotenv import load_dotenv
from sqlalchemy import create_engine

# --- 1. System Configuration ---
load_dotenv(override=True)
engine = create_engine("postgresql://admin:password123@localhost:5432/market_data")

conf = {'bootstrap.servers': 'localhost:9092', 'client.id': 'breeze-replay'}
producer = Producer(conf)
KAFKA_TOPIC = "nse_live_ticks"

# Using the unified env vars from your live producer
API_KEY = os.environ.get("ICICI_APP_KEY")
SECRET_KEY = os.environ.get("ICICI_SECRET_KEY")
SESSION_TOKEN = os.environ.get("ICICI_SESSION_KEY")

print("Authenticating with ICICI Breeze...")
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=SECRET_KEY, session_token=SESSION_TOKEN)

# --- 2. Fetch Tickers from Database ---
print("Fetching Nifty 50 list from the Database...")
# Limiting to 5 for testing so you don't hit REST API rate limits instantly
meta_df = pd.read_sql("SELECT ticker FROM dim_stock_metadata", engine)
tickers_to_track = meta_df['ticker'].tolist()

to_date = datetime.now().strftime('%Y-%m-%dT15:30:00.000Z')
from_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%dT09:15:00.000Z')

historical_payloads = []

print(f"Downloading historical data from {from_date} to {to_date}")

# --- 3. The REST API Loop ---
for ticker in tickers_to_track:
    try:
        print(f"Fetching {ticker}...")
        # Note: The historical API expects the ticker (ShortName), not the token!
        response = breeze.get_historical_data_v2(
            interval="1minute",
            from_date=from_date,
            to_date=to_date,
            stock_code=ticker,
            exchange_code="NSE",
            product_type="cash"
        )

        if response.get('Success') and response.get('Success') != []:
            data = response['Success']
            for row in data:
                base_time = datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
                quarter_vol = max(1, int(row['volume']) // 4)

                simulated_ticks = [
                    {"price": float(row['open']), "sec_offset": 0},
                    {"price": float(row['high']), "sec_offset": 15},
                    {"price": float(row['low']), "sec_offset": 30},
                    {"price": float(row['close']), "sec_offset": 45}
                ]

                for tick in simulated_ticks:
                    tick_time = base_time + timedelta(seconds=tick['sec_offset'])
                    payload = {
                        "ticker": ticker,
                        "price": tick['price'],
                        "volume": quarter_vol,
                        "timestamp": tick_time.isoformat() + "Z"
                    }
                    historical_payloads.append(payload)

    except Exception as e:
        print(f"Error Fetching {ticker}: {e}")

# --- 4. The Master Chronological Sort ---
print(f"Aggregated {len(historical_payloads)} total events. Sorting chronologically...")
historical_payloads.sort(key=lambda x: x['timestamp'])

# --- 5. The Replay Engine ---
print("Starting replay engine...")

for event in historical_payloads:
    producer.produce(KAFKA_TOPIC, key=event['ticker'], value=json.dumps(event))
    producer.poll(0)

    print(f"[REPLAY] {event['timestamp']} | {event['ticker']} @ ₹{event['price']}")

    # Lowered the sleep time. 0.1s is too slow for multi-stock replay
    # (e.g., 5 stocks * 1000 ticks = 500 seconds of waiting)
    time.sleep(0.02)

producer.flush()
print("Replay Complete")