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

# Create the serializer
serializer = KafkaSerializer(client)

# Create the producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=serializer)

# Our producer needs a schema to send along with the data.
# In this example we're using Avro, so we'll load an .avsc file.
with open('/workspaces/apache-kafka/schema-registry/orders.avsc', 'r') as schema_file:
    schema = AvroSchema(schema_file.read())

data = {
    'name': 'Hello',
    'Age':45,
    'Subject': 'English'
}
#data={'Partiiton_no':2}
record_metadata =producer.send('orders', value=(data, schema)).get(timeout=10)
print(record_metadata.topic)
print(record_metadata.partition)
print(record_metadata.offset)