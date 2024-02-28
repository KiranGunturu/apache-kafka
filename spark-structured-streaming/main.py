from pyspark.sql.functions import *
from pyspark.sql import SparkSession

def main():
    
    print("creating spark session")

    spark = SparkSession.builder \
            .appName("streaming application") \
            .config("spark.sql.shuffle.partitions", 3) \
            .master("local[2]") \
            .getOrCreate()
    
    # 1. Read the data

    lines = spark.readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 9995) \
            .load()

    # 2. Processing logic

    words = lines.select(explode(split(lines.value, " ")).alias("word"))

    wordCounts = words.groupBy("word").count()

    # 3. write to the sink

    query = wordCounts.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("checkpointLocation", "checkpointdir1") \
            .start()

    
    query.awaitTermination()


if __name__ == "__main__":
    main()

