import time
import json
from datetime import datetime, timedelta
from confluent_kafka import Producer
from breeze_connect import BreezeConnect
from dotenv import load_dotenv
import os

load_dotenv()

conf = {'bootstrap.servers': 'localhost:9092', 'client.id': 'breeze-replay'}
producer = Producer(conf)
KAFKA_TOPIC = "nse_live_ticks"

API_KEY = os.environ.get("API_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
SESSION_TOKEN = os.environ.get("SESSION_TOKEN")

print("Authenticating with ICICI Breeze...")
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=SECRET_KEY,session_token=SESSION_TOKEN)

to_date = datetime.now().strftime('%Y-%m-%dT15:30:00.000Z')
from_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%dT09:15:00.000Z')

tickers_to_track = ["TCS"]
historical_payloads = []

print(f"Downloading historical data from {from_date} to {to_date}")

for ticker in tickers_to_track:
    try:
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
                    {"price": float(row['open']), "sec_offset":0},
                    {"price": float(row['high']), "sec_offset":15},
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

historical_payloads.sort(key=lambda x: x['timestamp'])

print(f"Loaded {len(historical_payloads)} historical events. Starting replay engine...")

for event in historical_payloads:
    producer.produce(KAFKA_TOPIC, key=event['ticker'], value=json.dumps(event))
    producer.poll(0)

    print(f"[REPLAY] {event['timestamp']} | {event['ticker']} @ ₹{event['price']}")

    time.sleep(0.1)

producer.flush()
print("Replay Complete")