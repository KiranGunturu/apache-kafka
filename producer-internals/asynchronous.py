from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name="orders"

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer = lambda x: dumps(x).encode('utf-8'))

def on_send_success(record_metadata, message):
    print(f'successfully produced "{message}" to topic {record_metadata.topic} and partition {record_metadata.partition} at offset {record_metadata.offset}')

def on_send_error(excp, message):
    print(f'failed to write the message "{message}" and the error is "{excp}"')



for i in range(100):
    data = {'number': i}
    record_metadata = producer.send(topic_name, value=data).add_callback(on_send_success, message=data).adderrback(on_send_error)

producer.flush()
producer.stop()
