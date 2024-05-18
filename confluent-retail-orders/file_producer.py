from confluent_kafka import Producer
import json

conf = {'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'N6QTO6LJ2Q5XZHEI',
        'sasl.password': 'tg9oqRFFM7A/oONjW2Hp42SU0wsf97gjuXrPtIrfQsEp83Z9i/fWDbBkjDN1GEbo',
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
        producer.produce("retail_orders_history", key=customer_id, value=line, callback=acked)
        producer.poll(1)
        producer.flush()
