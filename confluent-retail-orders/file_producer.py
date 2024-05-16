from confluent_kafka import Producer
import json

conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'SCSPFWA4KHX2PXL6',
        'sasl.password': 'p1SuRG7NdxmJ3cpAOa82aeUdX4GegBWCg6iXfa+zDsS7BZKMyzfYerG3sXzlmMVq',
        'client.id': 'Kiran Gunturu'}

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        msg_key = msg.key().decode('utf-8')
        msg_value = msg.value().decode('utf-8')
        print(f"Message produced key : {msg_key} and value is: {msg_value}")


with open('/workspaces/apache-kafka/confluent-retail-orders/orders_input.json', 'r') as file:
    for line in file:
        order = json.loads(line)
        customer_id = str(order["customer_id"])
        producer.produce("newtopic", key=customer_id, value=line, callback=acked)
        producer.poll(1)
        producer.flush()
