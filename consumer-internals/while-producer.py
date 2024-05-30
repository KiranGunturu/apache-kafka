from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name = "orders"
producer =  KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'))


while True:
    message = input("enter the message to be produced")
    partition_number = int(input("enter the partition number the message to be sent"))
    producer.send(topic_name, value=message, partition=partition_number)

producer.close()