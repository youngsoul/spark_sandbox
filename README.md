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

Add: path to <unzipped spark distro>/python

Add: path to <unzipped spark distro>/python/lib/py4j-0.10.7-src.zip



## Install PySpark

After installing Spark on MacOS, create a directory and a virtual environment.

```python3 -m venv venv```

Then install PySpark

```pip install pyspark```


