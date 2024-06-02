from kafka import KafkaConsumer
import json

topic_name="orders"

consumer = KafkaConsumer(topic_name, bootstrap_servers=['localhost:9092'],
    value_deserialzier=lambda x: json.loads(x.decode('utf-8')),
    group_id='orders_consumer',
    auto_offset_reset='latest'
    )


for msg in consumer:
    print(message)