from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name="orders"
 producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'))


for i range(100):
    data = {'number': i}
    producer.send(topic_name, value=data)
    sleep(0.5)

producer.flush()
producer.send()