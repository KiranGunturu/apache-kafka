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

converted_orders_df \
    .writeStream \
    .queryName("ingestionquery") \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointlocation", "checkpoint301") \
    .toTable("orderstablenew301")

spark.sql("select *from orderstablenew301").show()

