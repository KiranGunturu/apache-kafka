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

customer_id = "11599"
customer_details = '{"order_id":1,"customer_id":11599,"customer_fname":"Mary","customer_lname":"Malone","city":"Hickory","state":"NC","pincode":28601,"line_items":[{"order_item_id":1,"order_item_product_id":957,"order_item_quantity":1,"order_item_product_price":299.98,"order_item_subtotal":299.98}]}'


producer.produce("retail_orders", key=customer_id, value=customer_details, callback=acked)
producer.poll(1)
producer.flush()
