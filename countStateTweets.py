import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


test = "D:/Uni/6.Semester/ForschungsmodullDatenbanken/Tweets/2014_soccer_champoinship/Test/*.json"
jsonFiles = "G:/ForschungsseminarDaten/Samples/Samples/*.json"

df = spark.read.json(test)

df.createOrReplaceTempView("Anzahl")
anzahlDerTweets = spark.sql("SELECT count(*) AS AnzahlderTweets FROM text")
anzahlDerTweets.show(False)