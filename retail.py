from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .master("local[*]") \
        .appName("retail") \
        .getOrCreate()

    df = spark.read \
        .option("header", "true") \
        .csv("./data/online-retail-dataset.csv") \
        .repartition(2)

    result = df.selectExpr("instr(Description, 'GLASS') >= 1 as is_glass") \
        .groupBy("is_glass") \
        .count() \
        .collect()

    print(result)
    # sit here so you can look at the SparkUI
    # localhost:4040
    ip = input("Press return to end: ")

    spark.stop()