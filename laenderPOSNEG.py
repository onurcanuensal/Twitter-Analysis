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
jsonFiles = "G:/ForschungsseminarDaten/Samples/Samples/sample_2014-06-12-18-58-38.348+0200.json"

#user.lang
#coordinates.country

df = spark.read.json(jsonFiles)
referees = df.select("user.lang", "retweeted_status.text")\
    .withColumn("lang", lower(col("text")))\
    #.groupBy("text")\
    #.count()\
    #.sort("count", ascending=False)

referees = referees.withColumnRenamed("country", "Text")
referees.toPandas().to_csv("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/Forschungsmodul Datenbanken/csvDateien/country_and_text_ALL.csv")
