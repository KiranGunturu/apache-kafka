from json import dumps
from time import sleep
from kafka import KafkaProducer

def custom_partitioner(key, all_partitions, available):
    print("The key is : {}".format(key))
    print("All partitions : {}".format(all_partitions))
    print("Afer decoding of the key : {}".format(key.decode('utf-8'))) # decoding as we get it as serialized
    return int(key.decode('utf-8'))%len(all_partitions) # converting the string key t0 int post decoding


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],partitioner=custom_partitioner)

topic_name = "orders"
producer.send(topic_name, key=b'3', value=b'Hello partitoner')
producer.send(topic_name, key=b'2', value=b'Hello partitoner')
producer.send(topic_name, key=b'369', value=b'Hello partitoner')
producer.send(topic_name, key=b'301', value=b'Hello partitoner')
