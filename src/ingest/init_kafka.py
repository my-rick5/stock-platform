from confluent_kafka.admin import AdminClient, NewTopic

def create_topics():
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    
    # We use 3 partitions so Spark can use 3 concurrent workers later
    topic_list = [NewTopic("nyse_raw", num_partitions=3, replication_factor=1)]
    fs = admin_client.create_topics(topic_list)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

if __name__ == "__main__":
    create_topics()
