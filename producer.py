import pandas as pd
import json
from kafka import KafkaProducer 



data_file = "C:\\Project\\movies.csv"

producer = KafkaProducer(bootstrap_servers='localhost:9092')


data = pd.read_csv(data_file)

for i, row in data.iterrows():
    records = row.to_dict()
    producer.send("input_topic", value=json.dumps(records).encode("utf-8")) 
    


producer.flush()
producer.close() 
