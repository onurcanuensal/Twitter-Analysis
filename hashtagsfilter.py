import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.getOrCreate()
path = 'D:/Uni/6.Semester/ForschungsmodullDatenbanken/Tweets/2014_soccer_champoinship/Samples/sample_2014-06-12-20-25-08.820+0200.json'

df = spark.read.json(path)
hashtags = df.select("entities.hashtags.text")\
    .withColumn("text", explode("text"))\
    .withColumn("text", lower(col("text")))\
    .groupBy("text")\
    .count()\
    .sort("count", ascending=False)

hashtags = hashtags.withColumnRenamed("text", "hashtags")
hashtags.toPandas().to_csv("D:/hashtags_ALL.csv")