# in Python
from pyspark.sql import SparkSession
import time

if __name__ == '__main__':
    # local[2] means use 2 cpu cores
    # local[*] means use all available cores
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("app1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    total_runs = 5
    for i in range(total_runs):
        print(f"Running {i} or {total_runs}")
        print(spark.range(50000).where("id > 5000").selectExpr("sum(id)").collect())
        time.sleep(2)


    spark.stop()

