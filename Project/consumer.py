from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "test2",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",  
    group_id="movie-dataset-3",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

for message in consumer:
    data = message.value
    print("Received:", data)
