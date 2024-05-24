from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name = "orders"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'),
    key_serializer=lambda x: str(x).encode('utf-8'))

for msg in range(1000):
    data = {'number': msg}
    key=msg
    print(data)
    producer.send(topic_name, key=msg, value=data)


# as we did not specify the linger.ms. but by default it is 0 that means messages are sent to kafak as and when they are received. no batching.
lets say the above for loop executed, it sent all the 1000 messages to buffer.
i/o thread sent 997 messages and as the messages are coming pretty fast, i/o thread is busy and it did not last 3 messages to cluster but they are in the buffer.
producer is closed as the for loop ended. so we are loosing 3 messages here.

to avoid this, we have to use producer.flush() - it will forcefully psuh all the pending messages from buffer to cluster before producer shutdown


for msg in range(1000):
    data = {'number': msg}
    key=msg
    print(data)
    producer.send(topic_name, key=msg, value=data)
producer.flush()
producer.close()