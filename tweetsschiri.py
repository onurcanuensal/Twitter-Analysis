import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.getOrCreate()
jsonFiles = "C:/Users/ArtzT/Documents/Visual Studio Code/Forschungsmodul/jsons/*.json"

df = spark.read.json(jsonFiles)
hashtags = df.select("retweeted_status.text")\
    .where("retweeted_status.text LIKE '%referee%'")\
    .withColumn("retweeted_status.text", lower(col("text")))

hashtags = hashtags.withColumnRenamed("text", "hashtags")
hashtags.toPandas().to_csv("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/Forschungsmodul Datenbanken/csvDateien/referee_tweets_all.csv")