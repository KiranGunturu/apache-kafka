from kafka import KafkaConsumer
from kafka import TopicPartition, OffsetAndMetadata
import json

topic_name="orders"

consumer = KafkaConsumer(topic_name, bootstrap_servers=['localhost:9092'],
    value_deserialzier=lambda x: json.loads(x.decode('utf-8')),
    group_id='orders_consumer',
    auto_offset_reset='earliest',
    enable_auto_commit=False
    )


for message in consumer:
    print(message)
    print("The value is: {}".format(message.value))
    print("The key is: {}".format(message.key))
    print("The topic is: {}".format(message.topic))
    print("The partition is: {}".format(message.partition))
    print("The offset is: {}".format(message.offset))
    print("The timestamp is: {}".format(message.timestamp))
    tp=TopicPartition(message.topic,message.partition)
    om=OffsetAndMetadata(message.offset+1, message.timestamp)
    consumer.commit({tp:om})
    print('*' * 100)
