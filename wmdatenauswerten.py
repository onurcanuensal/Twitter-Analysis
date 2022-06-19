from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import pyspark.sql.functions as func
import os
import sys
from pyspark.sql import SparkSession
import pyspark

#mit open ssh kopieren den key,
#neues public key erzeugnen 
#.ssh authorized keys anhängen den neuen key

# environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#sc = pyspark.SparkContext
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#path
pathsample_1 = "C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/SOES/Übung/uebung07/input/*.json"
pathsample_2 = "C:/Users/ArtzT/Documents/Visual Studio Code/Forschungsmodul/jsons/jsons/sample_2014-06-12-18-58-38.348+0200.json"

#Laender/Timezones die am meisten mitgefiebert haben
"""
laender = spark.read.json(pathsample_2)

laender.createTempView("Zeitzonen")

laender_ausgabe = spark.sql("SELECT user.time_zone, count(user.time_zone) FROM Zeitzonen GROUP BY user.time_zone ORDER BY count(user.time_zone) DESC")

laender_ausgabe.show(20, False)
"""
#Schirierwahnungen

referee_tweets = spark.read.json(pathsample_1)

referee_tweets.createTempView("Schiedsrichtererwaehnungen")

referee = spark.sql("SELECT user.name, retweeted_status.text FROM Schiedsrichtererwaehnungen WHERE retweeted_status.text LIKE '%referee%'")

referee.show(200, False)