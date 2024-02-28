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
    completed_orders = spark.sql("select *from orders where order_status = 'COMPLETE'")


    # 3. write to the sink

    query = completed_orders.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("path", "/workspaces/apache-kafka/spark-structured-streaming/data/outputdir") \
            .option("checkpointLocation", "checkpointdir1") \
            .start()

    
    query.awaitTermination()


if __name__ == "__main__":
    main()

