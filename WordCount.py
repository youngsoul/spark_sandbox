from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

if __name__ == "__main__":

    # local[*] means to run in local mode on all cores on the local machine
    # change * to a number to limit the cores it will run on.
    spark = SparkSession\
        .builder\
        .master("local[*]") \
        .appName("WordCount") \
        .getOrCreate()

    lines = spark.read.text("./data/sample.txt")

    wordCounts = lines\
        .select(explode(split(lines.value, "\s+"))
        .alias("word"))\
        .groupBy("word")\
        .count()

    print("Word Counts: ")
    wordCounts.show()

    spark.stop()