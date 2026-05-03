import json
import uuid
import pandas as pd
import boto3
import io
from confluent_kafka import Consumer
from datetime import timezone, datetime

print("connecting to minio data lake...")
s3_client = boto3.client(
    's3',
    endpoint_url = 'http://localhost:9000',
    aws_access_key_id = 'admin',
    aws_secret_access_key = 'password123',
    region_name = 'us-east-1'
)
BUCKET_NAME = 'raw-market-data'

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'minio_datalake_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
topic = 'nse_live_ticks'
consumer.subscribe([topic])

BATCH_SIZE = 100
batch_data = []

print(f"Listening to '{topic}'. Batching {BATCH_SIZE} records per parquet file...")
print("Press Ctrl+C to stop.")

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue
        
        payload = json.loads(msg.value().decode('utf-8'))
        batch_data.append(payload)

        if len(batch_data) >= BATCH_SIZE:
            df = pd.DataFrame(batch_data)
            file_id = str(uuid.uuid4())[:8]
            file_name = f"ticks_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}_{file_id}.parquet"

            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            s3_client.upload_fileobj(parquet_buffer, BUCKET_NAME, file_name)

            print(f"✅ Flushed {BATCH_SIZE} records to MinIO: s3://{BUCKET_NAME}/{file_name}")

            batch_data = []

except KeyboardInterrupt:
    print("\nHalting MinIO ingestion...")
finally:
    consumer.close()
    print("Kafka connection closed.")