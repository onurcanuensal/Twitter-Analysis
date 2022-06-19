import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.getOrCreate()
jsonFiles = "G:/ForschungsseminarDaten/Samples/Samples/*.json"

df = spark.read.json(jsonFiles)
hashtags = df.select("user.lang")\
    .withColumn("lang", lower(col("lang")))\
    .groupBy("lang")\
    .count()\
    .sort("count", ascending=False)

hashtags = hashtags.withColumnRenamed("text", "hashtags")
hashtags.toPandas().to_csv("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/Forschungsmodul Datenbanken/csvDateien/tweet_language_ALL.csv")