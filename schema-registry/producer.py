#https://pypi.org/project/aws-glue-schema-registry/
# import libraries
import boto3
from time import sleep
from json import dumps
from kafka import KafkaProducer
from aws_schema_registry import DataAndSchema, SchemaRegistryClient
from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.adapter.kafka import KafkaSerializer

# create session
session = boto3.Session(aws_access_key_id='id', aws_secret_access_key='sec_key')

# create aws glue client
glue_client = session.client('glue', region_name='us-east-1')

# Create the schema registry client, which is a façade around the boto3 glue client
client = SchemaRegistryClient(glue_client,
                              registry_name='orders-registry')


# for the very first time, schema will be created inside the glue registry with the name of the topic.

# Create the serializer
serializer = KafkaSerializer(client)

# Create the producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=serializer)

# Our producer needs a schema to send along with the data.
# In this example we're using Avro, so we'll load an .avsc file.
with open('/workspaces/apache-kafka/schema-registry/orders.avsc', 'r') as schema_file:
    schema = AvroSchema(schema_file.read())

data = {
    'name': 'Krish',
    'Age':33,
    'Subject': 'Physics'
}

response_metadata =producer.send('orders', value=(data, schema)).get(timeout=10)
print(response_metadata.topic)
print(response_metadata.partition)
print(response_metadata.offset)