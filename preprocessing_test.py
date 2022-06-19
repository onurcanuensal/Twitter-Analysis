import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
import preprocessor as p

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.getOrCreate()
jsonFiles = "C:/Users/ArtzT/Documents/Visual Studio Code/Forschungsmodul/jsons/sample_2014-06-12-21-16-27.801+0200.json"

cleanedJson = p.clean(jsonFiles)

df = spark.read.json(cleanedJson)
hashtags = df.select("text")\
    .coalesce(1)\
    .withColumn("text", lower(col("text")))\
    .groupBy("text")\
    .count()\
    .sort("count", ascending=False)

hashtags = hashtags.withColumnRenamed("text", "hashtags")
hashtags.toPandas().to_csv("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/Forschungsmodul Datenbanken/csvDateien/text_preprocessed_small.csv")