from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, col
import matplotlib.pyplot as plt
import pandas as pd
from wordcloud import WordCloud, STOPWORDS
import random
from os import path
import os

"""
BEFORE running this spark application run the below command in terminal window

nc -lk 9999

start spark application

then start typing a sentence into terminal window

"""
# get data directory (using getcwd() is needed to support running example in generated IPython notebook)
d = path.dirname(__file__) if "__file__" in locals() else os.getcwd()

stopwords = set(STOPWORDS)
stopwords.update(["said", "&amp;", "ok?", "", 'amp'])
stopwords.update(["&lt;", "&gt;", "&quot;", "&amp;", "w/o", "w/"])
stopwords.update(['atta...rt', '#trump', "trump", "president", "donald", "don"])
stopwords.update(['him?', 'now,', '-', 'see', 'go', 'great".', "he's", 'got', 'ok?', 'gets', 'get', 'for', 'on', 'one','he', 'we', 'want', 'it.', 'O.', 'and', 'the', "will", "say", "said", "new", "day", "he's", 'But', 'A','They', 'The', 'I', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"])


def create_word_cloud(pandas_df):
    # Generate a word cloud image
    #print(pandas_df.head(20))

    # ignore until we have a couple of rows
    if pandas_df.shape[0] <= 2:
        return

    # apply stopwords manually as stopwords are ignored in WordCloud for generate_from_frequencies
    df2 = pandas_df[pandas_df['word'].apply(lambda x: x.lower() not in stopwords)]
    if df2.shape[0] < 1:
        return # must have filtered all stopwords

    word_count_dict = dict(zip(df2['word'], df2['count']))

    # print(word_count_dict)

    if len(word_count_dict) > 2:
        # NOTE: stopwords are ignored for generate_from_frequencies
        wordcloud = WordCloud(width=1000, height=860, max_words=600).generate_from_frequencies(word_count_dict)  # WordCloud().generate(text)
        wordcloud.to_file(path.join(d, 'data', 'tweet_wc.png'))


if __name__ == '__main__':
    # local[*] means use all available cores
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("network-word-count") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # because this is a small local mode machine, set partitions small from the default 200
    # to avoid creating too many shuffle partitions.
    spark.conf.set("spark.sql.shuffle.partitions", 5)

    # type(socket_input) = DataFrame
    socket_df = spark.readStream.format('socket').option('host', 'localhost')\
        .option('port',9999).load()

    # split the lines into words
    words = socket_df.select(explode(split(socket_df.value, " ")).alias("word"))


    # generate word counts
    word_counts = words.groupBy('word').count().filter(col('count') >= 20).sort(col('count').desc())

    # Start running the query that prints the running counts to the console
    # query = word_counts \
    #     .writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .start()

    query = word_counts \
        .writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("all_words") \
        .start()


    while True:
        pandas_df = spark.sql("SELECT * from all_words").toPandas()
        #print(pandas_df.head(20))
        create_word_cloud(pandas_df)
        time.sleep(2)

    query.awaitTermination()