import json
import datetime
import os
import pandas as pd
from confluent_kafka import Producer
from breeze_connect import BreezeConnect
from dotenv import load_dotenv
from sqlalchemy import create_engine

engine = create_engine("postgresql://admin:password123@localhost:5432/market_data")
load_dotenv(override=True)

conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "nse_live_ticks"
}

producer = Producer(conf)
KAFKA_TOPIC = "nse_live_ticks"

API_KEY = os.environ.get("ICICI_APP_KEY")
SECRET_KEY = os.environ.get("ICICI_SECRET_KEY")
SESSION_TOKEN = os.environ.get("ICICI_SESSION_TOKEN")

print(f"DEBUG: Attempting login with Token starting with: {SESSION_TOKEN[:5]}...")

print("Authenticating with breeze...")
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=SECRET_KEY, session_token=SESSION_TOKEN)

print("Fetching Nifty 50 list from DB...")
meta_df = pd.read_sql("SELECT ticker, icici_token FROM dim_stock_metadata limit 20;", engine)

token_map = {}
for _, row in meta_df.iterrows():
    clean_token = str(int(row['icici_token']))
    token_map[clean_token] = row['ticker']

live_tickers = meta_df['ticker'].tolist()

def on_ticks(ticks):
    try:
        raw_symbol = ticks.get('symbol', '')

        icici_token = raw_symbol.split('!')[-1] if '!' in raw_symbol else ''
        price = float(ticks.get('last', 0.0))

        if price > 0 and ticker != 'UNKNOWN':
            payload = {
                "ticker": ticker,
                "price": price,
                "volume": int(ticks.get('ltq', 1)),
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
        
            producer.produce(KAFKA_TOPIC, key=ticker, value=json.dumps(payload))
            producer.poll(0)

    except Exception as e:
        print(f"Error parsing tick: {e}")
        
breeze.on_ticks = on_ticks 
breeze.ws_connect()

for ticker in live_tickers:
    print(f"Subscribing to {ticker}...")
    breeze.subscribe_feeds(
        stock_code=ticker,
        exchange_code="NSE", 
        product_type="cash", 
        get_exchange_quotes=True, 
        get_market_depth=False
    )

print("🔌 Connected to ICICI Direct. Listening for live market ticks...")

try:
    while True:
        pass
except KeyboardInterrupt:
    print("\nDisconnecting from ICICI and flushing Kafka buffer...")
    breeze.ws_disconnect()
    producer.flush()
    print("Shutdown Complete")
