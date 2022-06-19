#from collections import Counter
import os
import sys
from pyspark.sql.session import SparkSession
from pyspark import SparkContext



os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

path = 'D:/Uni/6.Semester/ForschungsmodullDatenbanken/Tweets/2014_soccer_champoinship/Samples/sample_2014-06-12-20-25-08.820+0200.json'

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


test = spark.read.json(path)
test.createTempView("Schiedsrichterbemerkungen")
filter_schiri = spark.sql("SELECT user.name, retweeted_status.text, retweeted_status.entities.hashtags[0] AS Hashtags FROM Schiedsrichterbemerkungen WHERE retweeted_status.text LIKE '%referee%' ")

filter_schiri.show(25, False) 