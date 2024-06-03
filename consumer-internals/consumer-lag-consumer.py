"""
consumer lag:

the difference between how fast producers place records on the broker and when consumers read those messages is called consumer lag.
there's bound to be some lag from the consumer, but ideally the consumer will catch up, ot at least have a consistent lag rather than a gradually increasing lag

producer-topic-offsets

100
101
102
103
104
105

consumer offset commited
101

so the lag is 6-2=4

"""

from kafka import KafkaConsumer
from kafka import TopicPartition , OffsetAndMetadata
from time import sleep


import json


consumer = KafkaConsumer ('hello_world1',bootstrap_servers = ['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('utf-8')),group_id='0419',auto_offset_reset='earliest',
                          enable_auto_commit =False)


for message in consumer:
    print(message)
    tp = TopicPartition(message.topic, message.partition)
    om = OffsetAndMetadata(message.offset + 1, message.timestamp)
    consumer.commit({tp: om})
    sleep(0.8)


#To get the information about Consumer Lag:
------------------------------------------------------------------------------
#F:/kafka_2.12-3.3.1/bin/windows/kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group demo112215sgtrjwrykvjh --describe