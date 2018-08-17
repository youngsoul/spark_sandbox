from pyspark.sql import SparkSession
import time

if __name__ == '__main__':
    # local[*] means use all available cores
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("streaming-activity") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # because this is a small local mode machine, set partitions small from the default 200
    # to avoid creating too many shuffle partitions.
    spark.conf.set("spark.sql.shuffle.partitions", 5)

    # read just a single file to grab the schema
    static = spark.read.json("./data/activity-data/part-00000-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json")
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

    """
     We discuss maxFilesPerTrigger a little later on in this chapter but essentially it allows you to
     control how quickly Spark will read all of the files in the folder. By specifying this value lower, 
     we’re artificially limiting the flow of the stream to one file per trigger. This helps us demonstrate 
     how Structured Streaming runs incrementally in our example, but probably isn’t something you’d use in production
    """

    # Like other Spark APIs, streaming DataFrame creation and execution is Lazy
    # we have to create a transformation on the dataframe BEFORE it will start to read the files
    streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1).json("./data/activity-data")

    # Define the transformation - this does NOT start the stream.  See the output definition next
    activityCounts = streaming.groupBy('gt').count()

    print(activityCounts)

    # Output definition
    # Add a 'memory sink'
    activityQuery = activityCounts.writeStream.queryName("activity_counts") \
        .format("memory").outputMode("complete") \
        .start()

    # watch the data being written to the output stream in memory.
    # the memory table name is the same as the query name
    for x in range(15):
        spark.sql("SELECT * FROM activity_counts").show()
        time.sleep(0.5)

    activityQuery.awaitTermination(timeout=10)

    spark.stop()
