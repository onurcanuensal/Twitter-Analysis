import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.getOrCreate()
#jsonFiles = "C:/Users/ArtzT/Documents/Visual Studio Code/Forschungsmodul/jsons/*.json"
jsonFiles = "D:/Uni/6.Semester/ForschungsmodullDatenbanken/Tweets/2014_soccer_champoinship/Samples/sample_2014-06-12-20-25-08.820+0200.json"

df = spark.read.json(jsonFiles)
referees = df.select("user.lang", "retweeted_status.text")\
    .withColumn("text", lower(col("text")))
    #.groupBy("text")\
    #.count()\
    #.sort("count", ascending=False)

referees = referees.withColumnRenamed("text", "Text")
referees.toPandas().to_csv("Desktop/country_text_ALL.csv")
