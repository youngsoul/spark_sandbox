from pyspark.sql import SparkSession
import datetime

"""
We load an initial DataFrame from a CSV file and then derive some new DataFrames from it using transformations.
We can avoid having to recompute the original DataFrame (i.e., load and parse the CSV file) many times by adding a 
line to cache it along the way.
"""
if __name__ == '__main__':
    # local[2] means use 2 cpu cores
    # local[*] means use all available cores
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("cache-timing") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Original loading code that does *not* cache DataFrame
    start = datetime.datetime.now()
    DF1 = spark.read.format("csv")\
      .option("inferSchema", "true")\
      .option("header", "true")\
      .load("./data/flight-data/2015-summary.csv")
    DF2 = DF1.groupBy("DEST_COUNTRY_NAME").count().collect()
    DF3 = DF1.groupBy("ORIGIN_COUNTRY_NAME").count().collect()
    DF4 = DF1.groupBy("count").count().collect()
    end = datetime.datetime.now()

    diff = end - start
    print(f"Processing 4 Dataframes with no caching took {diff.seconds} seconds")

    # Original loading code that *does* cache DataFrame
    start = datetime.datetime.now()
    DF1 = spark.read.format("csv")\
      .option("inferSchema", "true")\
      .option("header", "true")\
      .load("./data/flight-data/2015-summary.csv")

    DF1.cache()  # <--   Cache the initial Dataframe so we do not re-read it
    DF1.count()  # <--   Eagerly cache the dataframe

    DF2 = DF1.groupBy("DEST_COUNTRY_NAME").count().collect()
    DF3 = DF1.groupBy("ORIGIN_COUNTRY_NAME").count().collect()
    DF4 = DF1.groupBy("count").count().collect()
    end = datetime.datetime.now()

    diff = end - start
    print(f"Processing 4 Dataframes with caching took {diff.seconds} seconds")
