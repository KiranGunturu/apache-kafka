from pyspark.sql.functions import *
from pyspark.sql import SparkSession

def main():
    
    print("creating spark session")

    spark = SparkSession.builder \
            .appName("streaming application") \
            .config("spark.sql.shuffle.partitions", 3) \
            .master("local[2]") \
            .getOrCreate()
    
    # define schema
    orders_schema = 'order_id long, order_date date, order_customer_id long, order_status string'

    # 1. Read the data

    orders_df = spark.readStream \
            .format("json") \
            .schema(orders_schema) \
            .option("path", "/workspaces/apache-kafka/spark-structured-streaming/data/inputdir") \
            .load()

    # 2. Processing logic

    orders_df.createOrReplaceTempView("orders")
    agg_orders = spark.sql("select order_status, count(*) as total from orders group by order_status")


    # 3. write to the sink

    query = agg_orders.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("checkpointLocation", "checkpointdir1") \
            .start()

    
    query.awaitTermination()


if __name__ == "__main__":
    main()

