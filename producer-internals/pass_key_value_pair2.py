from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
        key_serializer=str.encode,
        value_serializer=lambda x: dumps(x).encode('utf-8'))

topic_name="orders"

data1 = {'number': 1}
data2 = {'number': 2}
data3 = {'number': 3}
data4 = {'number': 4}
data5 = {'number': 5}
data6 = {'number': 6}

producer.send(topic_name, key='ping', value=data1)
producer.send(topic_name, key='ping', value=data2)
producer.send(topic_name, key='ping', value=data3)
producer.send(topic_name, key='pong', value=data4)
producer.send(topic_name, key='pong', value=data5)
producer.send(topic_name, key='pong', value=data6)

producer.close()