import json
import datetime
from confluent_kafka import Producer
from breeze_connect import BreezeConnect
from dotenv import load_dotenv
import os

load_dotenv()

conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "breeze-live-producer"
}

producer = Producer(conf)
KAFKA_TOPIC = "nse_live_data"

API_KEY = os.environ.get("API_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
SESSION_TOKEN = os.environ.get("SESSION_TOKEN")

print("Authenticating with breeze...")
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=SECRET_KEY, session_token=SESSION_TOKEN)

def on_ticks(ticks):
    """
    This function is triggered by ICICI every time a trade happens.
    We translate their payload into our gold layer schema.
    """
    try:
        ticker = ticks.get('stock_code', 'UNKNOWN')
        price = float(ticks.get('last', 0.0))

        if price > 0:
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
    
breeze.ws_connect()

breeze.on_ticks = on_ticks # type: ignore

tickers_to_track = ["RELPET","TCS","INFTEC"]

for ticker in tickers_to_track:
    print(f"Subscribing to {ticker}...")
    breeze.subscribe_feeds(stock_code=ticker, exchange_code="NSE", product_type="cash")

print("🔌 Connected to ICICI Direct. Listening for live market ticks...")

try:
    while True:
        pass
except KeyboardInterrupt:
    print("Disconnecting from ICICI and flushing Kafka buffer...")
    breeze.ws_disconnect()
    producer.flush()
    print("Shutdown Complete")
