from pyspark.sql.functions import *

confluentBootstrapServers = 'pkc-12576z.us-west2.gcp.confluent.cloud:9092'
confluentApiKey = 'N6QTO6LJ2Q5XZHEI'
confluentSecret = 'tg9oqRFFM7A/oONjW2Hp42SU0wsf97gjuXrPtIrfQsEp83Z9i/fWDbBkjDN1GEbo'
confluentTopicName = 'retail_orders_history'

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

orders_df.printSchema()

converted_orders_df = orders_df.selectExpr("CAST(key as string) AS Key", "CAST(value as string) as value", "topic", "partition", "offset", "timestamp", "timestampType")

display(converted_orders_df)

orders_schema = "order_id long, customer_id long, customer_fname string, customer_lname string, city string, state string, pincode long, line_items array<struct<order_item_id: long, order_item_product_id: long, order_item_quantity: long, order_item_product_price: float, order_item_subtotal: float>>"

parched_orders_df = converted_orders_df.select("key", from_json("value", orders_schema).alias("value"), "topic", "partition", "timestamp", "timestampType")

display(parched_orders_df)

parched_orders_df.createOrReplaceTempView("orders")

explodedorders = spark.sql(""" select key, 
          value.order_id as order_id, 
          value.customer_id as customer_id, 
          value.customer_fname as customer_fname, 
          value.customer_lname as customer_lname,
          value.city as city,
          value.state as state,
          value.pincode as pincode,
          explode(value.line_items) as lines
          from orders """

          )


explodedorders.createOrReplaceTempView("explodedorders")

flattened_orders = spark.sql(""" select
          order_id,
          customer_id,
          customer_fname,
          customer_lname,
          city,
          state,
          pincode,
          lines.order_item_id as item_id,
          lines.order_item_product_id as product_id,
          lines.order_item_quantity as quantity,
          lines.order_item_product_price as price,
          lines.order_item_subtotal as subtotal
          from explodedorders
          """)


display(flattened_orders)

flattened_orders \
    .writeStream \
    .queryName("ingestionquery") \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoint302") \
    .toTable("orderstablenew302")

spark.sql("select *from orderstablenew302").show()

spark.sql("select count(*) from orderstablenew302").show()



