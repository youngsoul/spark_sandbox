from pyspark.sql import SparkSession
import time

if __name__ == '__main__':
    # local[*] means use all available cores
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("streaming-activity") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    static = spark.read.json("./data/activity-data/")
    """
    root
     |-- Arrival_Time: long (nullable = true)
     |-- Creation_Time: long (nullable = true)
     |-- Device: string (nullable = true)
     |-- Index: long (nullable = true)
     |-- Model: string (nullable = true)
     |-- User: string (nullable = true)
     |-- _corrupt_record: string (nullable = true)
     |-- gt: string (nullable = true)
     |-- x: double (nullable = true)
     |-- y: double (nullable = true)
     |-- z: double (nullable = true)
     gt field specifies what activity the user was doing at that time
 """
    dataSchema = static.schema

    print(dataSchema)
    static.show()
    spark.stop()
