# Import libraries
from pyspark.sql.functions import *

# constants
confluentBootstrapServers = 'pkc-12576z.us-west2.gcp.confluent.cloud:9092'
confluentApiKey = 'N6QTO6LJ2Q5XZHEI'
confluentSecret = 'tg9oqRFFM7A/oONjW2Hp42SU0wsf97gjuXrPtIrfQsEp83Z9i/fWDbBkjDN1GEbo'
confluentTopicName = 'retail_orders_history'
confluentTargetTopicName = "chicago_retail_orders"

# read from kafka

orders_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", confluentBootstrapServers) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config","kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey,confluentSecret)) \
    .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    .option("subscribe",confluentTopicName) \
    .option("startingTimestamp", 1) \
    .option("maxOffsetsPerTrigger", 50) \
    .load()
    
# print schema
orders_df.printSchema()

#convert binary to string

converted_orders_df = orders_df.selectExpr("CAST(key as string) AS Key", "CAST(value as string) as value", "topic", "partition", "offset", "timestamp", "timestampType")

#schema

orders_schema = "order_id long, customer_id long, customer_fname string, customer_lname string, city string, state string, pincode long, line_items array<struct<order_item_id: long, order_item_product_id: long, order_item_quantity: long, order_item_product_price: float, order_item_subtotal: float>>"

# enforce JSON schema: assign column:value format
parched_orders_df = converted_orders_df.select("key", from_json("value", orders_schema).alias("value"), "topic", "partition", "timestamp", "timestampType")

# create temp table

parched_orders_df.createOrReplaceTempView("orders")

# filter only chicago orders

filtered_orders = spark.sql("select cast(key as string), cast(value as string) from orders where value.city = 'Chicago'")


# write to kafak topic

filtered_orders \
    .writeStream \
    .queryName("ingestionquery") \
    .format("kafka") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpointdir400") \
    .option("kafka.bootstrap.servers", confluentBootstrapServers) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config","kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey,confluentSecret)) \
    .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    .option("topic",confluentTargetTopicName) \
    .start()

