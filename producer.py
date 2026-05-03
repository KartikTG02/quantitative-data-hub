import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

conf = {'bootstrap.servers' : 'localhost:9092'}
producer = Producer(conf)

topic = 'nse_live_ticks'
tickers = ['RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK']

def delivery_report(err, msg):
    """Callback to confirm if kafka received the message. """
    if err is not None:
        print(f"Delivery Failed: {err}")
    else:
        print(f"Sent {msg.value().decode('utf-8')}")

print(f"Initialising high-frequency market stream to topic: '{topic}'...")
print("Press Ctrl+C to stop.")

try:
    while True:
        payload = {
            'ticker': random.choice(tickers),
            'price': round(random.uniform(1000.0, 3500.0), 2),
            'volume': random.randint(1,500),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        producer.produce(
            topic,
            value=json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )

        time.sleep(random.uniform(0.1,0.5))

        producer.poll(0)

except KeyboardInterrupt:
    print("\nHalting market stream...")
finally:
    print("Flushing final messages to Kafka")
    producer.flush()