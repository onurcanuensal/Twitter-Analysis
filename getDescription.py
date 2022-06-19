
# -- coding: utf-8 --

from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import pyspark.sql.functions as func
import os
import sys
from pyspark.sql import SparkSession
import pyspark

# environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#sc = pyspark.SparkContext
sc = SparkContext.getOrCreate()
spark = SparkSession(sc) 

#Pfade zu den jeweiligen Json Dateien
path5_100 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-5-100.json"
path10_100 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-10-100.json"
path10 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-10.json"
path5 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-5.json"
path1 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/rt-1.json"

rt_1 = spark.read.json(path1)
rt_5 = spark.read.json(path5)
rt_10 = spark.read.json(path10)
rt_10_100 = spark.read.json(path10_100)
rt_5_100 = spark.read.json(path5_100)




#Ausgabe Profilbeschreibung
#rt_1.createOrReplaceTempView("Profilbeschreibung")
#description = spark.sql("SELECT user.id, user.name, user.description FROM Profilbeschreibung")
#description.show(50, False)

#Nach Zeitzone gruppieren aller erhältlichen json Datein/Testdateine um zu sehen welche Zeitzone am aktivsten ist
rt_1.createOrReplaceTempView("Zeitzone")
timezone1 = spark.sql("SELECT user.time_zone, count(user.time_zone) FROM Zeitzone GROUP BY user.time_zone ORDER BY count(user.time_zone) DESC")
timezone1.show(20, False)

rt_5.createOrReplaceTempView("Zeitzone")
timezone5 = spark.sql("SELECT user.time_zone, count(user.time_zone) FROM Zeitzone GROUP BY user.time_zone ORDER BY count(user.time_zone) DESC")
timezone5.show(20, False)

rt_10.createOrReplaceTempView("Zeitzone")
timezone10 = spark.sql("SELECT user.time_zone, count(user.time_zone) FROM Zeitzone GROUP BY user.time_zone ORDER BY count(user.time_zone) DESC")
timezone10.show(20, False)

#rt_5_100.createOrReplaceTempView("Zeitzone")
#timezone5_100 = spark.sql("SELECT user.time_zone, count(user.time_zone) FROM Zeitzone GROUP BY user.time_zone ORDER BY count(user.time_zone) DESC")
#timezone5_100.show(20, False)

rt_10_100.createOrReplaceTempView("Zeitzone")
timezone10_100 = spark.sql("SELECT user.time_zone, count(user.time_zone) FROM Zeitzone GROUP BY user.time_zone ORDER BY count(user.time_zone) DESC")
timezone10_100.show(20, False)