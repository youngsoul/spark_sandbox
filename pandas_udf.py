from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import pandas_udf, PandasUDFType

from pyspark.sql import SparkSession
import time
from functools import wraps


"""
Spark 2.3.x introduced the PandasUDF (aka Vectorized UDFs) that improves the performance of
Python UDFs significantly.  Prior to Spark 2.3 Python UDFs required heavy serialization and
invocation overhead.  This new feature is built upon Apache Arrow for a high performance
Python UDF implementation.

You must pip install PyArrow Pandas


The material here is from this blog:

https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html

NOTE: There are some coding differences from the blog post to make this work:

@udf('double') -> @udf(DoubleType())
 

Note that there are two important requirements when using scalar pandas UDFs:

The input and output series must have the same size.
How a column is split into multiple pandas.Series is internal to Spark, and therefore the result of user-defined function must be independent of the splitting.



"""


def timeit(method):
    @wraps(method)
    def decorated_function(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result
    return decorated_function


@udf(DoubleType())
def plus_one(v):
    """

    :param v: Type float called for every row in the dataframe
    :return:
    """
    #print(type(v))
    return v + 1

@timeit
def with_udf(df):
    return df.withColumn('v2', plus_one(df.v))

@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def pandas_plus_one(v):
    """

    :param v:  Pandas Series called with a block and in this test, only called 11 times
    :return:
    """
    #print(type(v))
    return v + 1

@timeit
def with_pandas_udf(df):
    return df.withColumn('v2', pandas_plus_one(df.v))


if __name__ == '__main__':

    spark = SparkSession\
        .builder\
        .master("local[*]") \
        .appName("Pandas_UDF") \
        .getOrCreate()

    df = spark.range(0, 10 * 1000 * 1000).withColumn('id', (col('id') / 10000).cast('integer')).withColumn('v', rand())
    df.cache()  # cache the dataframe
    df.count()  # force an eage cache of the dataframe

    df.show()

    df2 = with_udf(df)
    df2.show()

    df3 = with_pandas_udf(df)
    df3.show()

    print("Notice that 'with_pandas_udf' is faster than 'with_udf' ")
