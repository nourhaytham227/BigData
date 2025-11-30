from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "output_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",  
    group_id="movie_groups",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

for message in consumer:
    data = message.value
    print("Received:", data)
