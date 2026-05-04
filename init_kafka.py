from confluent_kafka.admin import AdminClient, NewTopic

# 1. Connect to your local Kafka Broker
admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

# 2. Define the exact specifications of your topic
# 1 partition is fine for local dev. Replication factor 1 because you have 1 broker.
topic_name = "nse_live_ticks"
new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)

print(f"Attempting to create topic: {topic_name}...")

# 3. Ask Kafka to create it
futures = admin.create_topics([new_topic])

# 4. Check the results
for topic, future in futures.items():
    try:
        future.result()  # This blocks until the creation succeeds or fails
        print(f"✅ Success! Topic '{topic}' is permanently registered in Kafka.")
    except Exception as e:
        # If it already exists, it will throw an error, which is perfectly fine
        print(f"Notice for '{topic}': {e}")