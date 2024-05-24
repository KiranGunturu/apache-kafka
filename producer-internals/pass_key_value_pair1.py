from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name="orders"

# dumps - will convert the JSON data as string and then encode will convert string to binary so it can be stored on cluster as kafka stores as binary on disk
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


producer.send(topic_name, key=b'foo',value=b'bar') # b indicates binary
producer.send(topic_name, key=b'foo',value=b'bar')

# now as the key is same, both will goto same partition

producer.close()