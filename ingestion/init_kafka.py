from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

topic_name = "nse_live_ticks"
new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)

print(f"Attempting to create topic: {topic_name}...")

# 3. Ask Kafka to create it
futures = admin.create_topics([new_topic])

# 4. Check the results
for topic, future in futures.items():
    try:
        future.result() 
        print(f"✅ Success! Topic '{topic}' is permanently registered in Kafka.")
    except Exception as e:
        print(f"Notice for '{topic}': {e}")