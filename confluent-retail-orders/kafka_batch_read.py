confluentBootstrapServers = 'pkc-12576z.us-west2.gcp.confluent.cloud:9092'
confluentApiKey = 'N6QTO6LJ2Q5XZHEI'
confluentSecret = 'tg9oqRFFM7A/oONjW2Hp42SU0wsf97gjuXrPtIrfQsEp83Z9i/fWDbBkjDN1GEbo'
confluentTopicName = 'retail_orders'

orders_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", confluentBootstrapServers) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config","kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey,confluentSecret)) \
    .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    .option("subscribe",confluentTopicName) \
    .load()

# spark runs in the JVM. so when we try to read from kafka topic using spark, we need to pass the jaas config.
# this is not required with python

display(orders_df)

# here the data will be shown to us in the binary format as thats why it is serialized and stored in the kafka topic. so will have to cast it to string to see the actual data.

orders_df.printSchema()

converted_orders_df = orders_df.selectExpr("CAST(key as string) AS Key", "CAST(value as string) as value", "topic", "partition", "offset", "timestamp", "timestampType")


display(converted_orders_df)