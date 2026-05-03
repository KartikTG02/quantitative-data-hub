#!/bin/bash

# Start the scripts in the background using the '&' operator
echo "🚀 Starting PostgreSQL Consumer (Gold Layer)..."
python consumer.py &

echo "🧊 Starting MinIO Sink (Bronze Layer)..."
python minio_sink.py &

echo "🧠 Starting PySpark Engine (Quant Transformations)..."
python spark_stream.py &

# The 'wait -n' command pauses the script here and watches the background tasks.
# If ANY of the three scripts crash or stop, the container will gracefully exit 
# instead of silently hanging.
wait -n