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

path5_100 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-5-100.json"
path10_100 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-10-100.json"
path10 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-10.json"
path5 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-5.json"
path1 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-1.json"
pathsample_2 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/sample_2014-06-24-03-53-52.189+0200.json"

rt_1 = spark.read.json(path1)
rt_5 = spark.read.json(path5)
rt_10 = spark.read.json(path10)
rt_10_100 = spark.read.json(path10_100)
rt_5_100 = spark.read.json(path5_100)
sample2 = spark.read.json(pathsample_2)

#tweetsDF.printSchema()

#Erstellen einer neuen temporärern Tabelle und ausgeben mit der Methode show 

#tweetsDF.createOrReplaceTempView("tweets")

#twitternickname = spark.sql("SELECT text FROM tweets WHERE contributors is not NULL")

#twitternickname.show()
"""
rt_1.createOrReplaceTempView("Follower")
famous1 = spark.sql("SELECT id, user.name, user.time_zone FROM Follower WHERE  user.followers_count >= 1000000")
famous1.show(10, False)

rt_5.createOrReplaceTempView("Follower")
famous2 = spark.sql("SELECT id, user.name, user.time_zone FROM Follower WHERE  user.followers_count >= 1000000")
famous2.show(10, False)

rt_10.createOrReplaceTempView("Follower")
famous3 = spark.sql("SELECT id, user.name, user.time_zone FROM Follower WHERE  user.followers_count >= 1000000")
famous3.show(10, False)

rt_10_100.createOrReplaceTempView("Follower")
famous4 = spark.sql("SELECT id, user.name, user.time_zone FROM Follower WHERE  user.followers_count >= 1000000")
famous4.show(10, False)
"""
sample2.createOrReplaceTempView("Follower")
famous5 = spark.sql("SELECT id, user.name, text, user.followers_count FROM Follower WHERE  user.followers_count >= 1000000 ORDER BY user.followers_count DESC")
famous5.show(100, False)

"""
rt_1.createOrReplaceTempView("TweetsofFamousAccount")
tweets = spark.sql("SELECT text FROM TweetsofFamousAccount WHERE id = 231658792209743872")
tweets.show(20, False)

rt_5.createOrReplaceTempView("TweetsofFamousAccount")
tweets2 = spark.sql("SELECT text FROM TweetsofFamousAccount WHERE id = 231658792209743872")
tweets2.show(20, False)

rt_10.createOrReplaceTempView("TweetsofFamousAccount")
tweets3 = spark.sql("SELECT text FROM TweetsofFamousAccount WHERE id = 231658792209743872")
tweets3.show(20, False)
"""

#eine Zeile nur verwenden und dann weiter probieren
#jqube
#am ende werden nur wenige attribute relevant sein
#log beachten 
#zuerst kleinere datenmengen verwenden
#für spark java 8 und 11
