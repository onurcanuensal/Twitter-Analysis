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
path = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/*.json"
test = r"E:\\Uni\\SoSe21\\ForschungsmodullDatenbanken\\Tweets\\2014_soccer_champoinship\\recorded-cascades1404922701441.json"

#Laender/Timezones die am meisten mitgefiebert haben
"""
laender = spark.read.json(path)

laender.createTempView("Zeitzonen")
laender2.createTempView("Zeitzonen2")

laender2_ausgabe = spark.sql("SELECT user.time_zone AS Zeitzone1, count(user.time_zone) AS Zuschauer1 FROM Zeitzonen2 GROUP BY user.time_zone ORDER BY count(user.time_zone) DESC")

laender_ausgabe = spark.sql("SELECT user.time_zone AS Zeitzone2, count(user.time_zone) AS Zuschauer2 FROM Zeitzonen GROUP BY user.time_zone ORDER BY count(user.time_zone) DESC")


#Join
j1 = laender_ausgabe.alias('ta')
j2 = laender2_ausgabe.alias('tb')

inner_join = j1.join(j2, j1.Zeitzone2 == j2.Zeitzone1).createTempView("Zuschauer")
inner_join = spark.sql("SELECT Zeitzone1, add(Zuschauer1, Zuschauer2) FROM Zuschauer WHERE Zeitzone1 = Zeitzone2")



inner_join.show(20, False)
"""

schiri = spark.read.json(test)
schiri.createTempView("Schiedsrichterbemerkungen")
schiri_ausgabe = spark.sql("SELECT user.name, retweeted_status.text FROM Schiedsrichterbemerkungen WHERE retweeted_status.text LIKE '%fuck%'")

#schiri_ausgabe = spark.sql("SELECT user.name, retweeted_status.text FROM Schiedsrichterbemerkungen WHERE retweeted_status.text LIKE '%ronaldo%'")
#schiri_ausgabe = spark.sql("SELECT user.name, retweeted_status.text FROM Schiedsrichterbemerkungen WHERE retweeted_status.text LIKE '%referee%'")
schiri_ausgabe.show(200, False)
