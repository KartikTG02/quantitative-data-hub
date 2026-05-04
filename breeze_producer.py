import json
import datetime
import os
import pandas as pd
from confluent_kafka import Producer
from breeze_connect import BreezeConnect
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib

engine = create_engine("postgresql://admin:password123@localhost:5432/market_data")

load_dotenv(override=True)

conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "nse_live_ticks"
}

producer = Producer(conf)
KAFKA_TOPIC = "nse_live_ticks"


API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("SECRET_KEY")
SESSION_TOKEN = os.getenv("SESSION_TOKEN")

print("Authenticating with Breeze Connect API...")
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)

print("Fethching Nifty 50 tokens from DB...")
metadata_df = pd.read_sql("SELECT ticker, icici_token FROM dim_stock_metadata", engine)

token_map = {}
subscription_tokens = []

for _, row in metadata_df.iterrows():
    clean_token = str(int(row["icici_token"]))
    token_map[clean_token] = row['ticker']
    subscription_tokens.append(f"4.1!{clean_token}")

def on_ticks(ticks):
    """
    Triggered by ICICI for every live trade.
    Decodes the token, formats the payload, and sends it to Kafka.
    """

    try:
        raw_symbol = ticks.get('symbol', '')

        icici_token = raw_symbol.split('!')[-1] if '!' in raw_symbol else ''

        ticker = token_map.get(icici_token, 'UNKNOWN')
        price = float(ticks.get('last', 0.0))

        if price > 0 and ticker != 'UNKNOWN':
            payload = {
                "ticker": ticker,
                "price": price,
                "volume": int(ticks.get('ltq' , 1)),
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }

            producer.produce(KAFKA_TOPIC, value=json.dumps(payload), key=ticker)
            producer.poll(0)

    except Exception as e:
        print(f"Error parsing tick: {e}")

breeze.on_ticks = on_ticks
breeze.ws_connect()

print(f"Opening websocket for {len(subscription_tokens)} assets...")

breeze.subscribe_feeds(
    stock_token=subscription_tokens,
    exchange_code="NSE",
    product_type="cash",
    get_exchange_quotes=True,
    get_market_depth=False
)

print("🔌 Connected! Listening to live market ticks. Press Ctrl+C to stop.")
try:
    while True:
        pass
except KeyboardInterrupt:
    print("\nHalting stream. Disconnecting from ICICI and flushing kafka buffer...")
    breeze.ws_disconnect()
    producer.flush()
    print("✅ Shutdown complete.")