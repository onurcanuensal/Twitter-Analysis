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

df = spark.read.json(jsonFiles)
referees = df.select("retweeted_status.text")\
    .withColumn("text", lower(col("text")))\
    #.where("retweeted_status.text LIKE '%referee%' OR retweeted_status.text LIKE '%arbito%' retweeted_status.text LIKE '%ref%' OR retweeted_status.text LIKE '%umpire%' OR retweeted_status.text LIKE '%arbitrator%' OR retweeted_status.text LIKE '%arbiter%' OR retweeted_status.text LIKE '%adjudicator%'")\
    #.groupBy("text")\
    #.count()\
    #.sort("count", ascending=False)

referees = referees.withColumnRenamed("text", "Text")
referees.toPandas().to_csv("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/Forschungsmodul Datenbanken/csvDateien/text_for_ngramsVSpolarity.csv")
