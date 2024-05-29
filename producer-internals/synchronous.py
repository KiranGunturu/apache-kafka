from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name="orders"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'))


for _ in range(100):
    data = {'number': msg}
    producer.send(topic_name, value=data)
    try:
        record_metadata = producer.send(topic_name, value=data).get(timeout=10)
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)
        sleep(0.5)
    except Excepton as e:
        print(e)

producer.flush()
producer.stop()