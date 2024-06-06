import boto3
from aws_schema_registry import SchemaRegistryClient
from kafka import TopicPartition , OffsetAndMetadata
from aws_schema_registry.adapter.kafka import KafkaDeserializer
from kafka import KafkaConsumer

session = boto3.Session(aws_access_key_id='{}', aws_secret_access_key='{}')

glue_client = session.client('glue', region_name='us-east-1')


Create the schema registry client, which is a façade around the boto3 glue client
client = SchemaRegistryClient(glue_client,
                              registry_name='my-registry')

Create the deserializer
deserializer = KafkaDeserializer(client)


consumer = KafkaConsumer ('topic2',bootstrap_servers = ['{}'],
value_deserializer=deserializer,group_id='{}',auto_offset_reset='earliest',
                          enable_auto_commit =False)
for message in consumer:
    print(message)
    print("The value is : {}".format(message.value))
    print("The key is : {}".format(message.key))
    print("The topic is : {}".format(message.topic))
    print("The partition is : {}".format(message.partition))
    print("The offset is : {}".format(message.offset))
    print("The timestamp is : {}".format(message.timestamp))
    tp=TopicPartition(message.topic,message.partition)
    om = OffsetAndMetadata(message.offset+1, message.timestamp)
    consumer.commit({tp:om})
    print('*' * 100)