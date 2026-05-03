import json
import psycopg2
from confluent_kafka import Consumer

print("Connecting to postgresql...")
try:
    pg_conn = psycopg2.connect(
        host="localhost",
        database="market_data",
        user="admin",
        password="password123",
        port=5432
    )
    cursor = pg_conn.cursor()
except Exception as e:
    print(f"Database connection failed as {e}")
    exit(1)

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'postgres_landing_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

topic = 'nse_live_ticks'
consumer.subscribe([topic])

print(f"Listening to Kafka topic: {topic}...")
print("Press Ctrl+C to stop.")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        raw_value = msg.value().decode('utf-8')
        payload = json.loads(raw_value)

        insert_query = """
            INSERT INTO fact_market_ticks (ticker, price, volume, tick_time)
            VALUES (%s, %s, %s, %s)
        """
        record_to_insert = (
            payload['ticker'],
            payload['price'],
            payload['volume'],
            payload['timestamp']
        )

        cursor.execute(insert_query, record_to_insert)
        pg_conn.commit()

        print(f"Inserted into DB: {payload['ticker']} @ {payload['price']}")

except KeyboardInterrupt:
    print("\nHalting consumer...")
finally:
    cursor.close()
    pg_conn.close()
    consumer.close()
    print("Connections closed.")