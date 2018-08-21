# Spark Sandbox with Python

This repo will contain examples and notes on how to run Spark code using Python.

Notes on how to integrate this with PyCharm will also be included.

These notes are specifically for Spark 2.3.1


## Install Spark on MacOS
[Spark on MacOS](https://medium.com/luckspark/installing-spark-2-3-0-on-macos-high-sierra-276a127b8b85)

As of Spark 2.3, you still need Java 1.8.



## Setup Spark and PyCharm

[Spark & PyCharm](https://www.pavanpkulkarni.com/blog/12-pyspark-in-pycharm/)

Cliffnotes:

After you download the Spark distribution and unzip it:

In your PyCharm project go to:

```Preferences->Project->Project Structure```

Add Content Root

Add: path to **unzipped spark distro**/python

Add: path to **unzipped spark distro**/python/lib/py4j-0.10.7-src.zip



## Install PySpark

After installing Spark on MacOS, create a directory and a virtual environment.

```python3 -m venv venv```

Then install PySpark

```pip install pyspark```



## Spark Twitter WordCloud

In this repo is a multiple process example:

- TwitterListener.py

    This python script creates a socket listener, a Twitter filter string and will listen for tweets that match the filter.  Once a process connects to the socket on port 9999, this process will start to send the tweet text.

- streaming_socket.py

    This python script creates a Structured Streaming application, using sockets, that will connect to port 9999, and read the tweet text and create a running word count from the tweets.  This process will also create a wordcloud image and write the image to the data directory.

- WordCountHttpServer.py

    This python script creates a simple HttpServer at port 4040 that will serve up the WordCloud image on refresh


