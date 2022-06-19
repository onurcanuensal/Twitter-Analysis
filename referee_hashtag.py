from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import os
import sys
from pyspark.sql import SparkSession
import pyspark
from textblob import TextBlob
from pyspark.sql.functions import concat, split, explode, col, udf
#ohne panda
# environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#sc = pyspark.SparkContext
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#path
path = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/*.json"
test = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/sample_2014-06-12-18-58-38.348+0200.json"

schiri = spark.read.json(test)
schiri.createTempView("Schiedsrichterbemerkungen")
filter_schiri = spark.sql("SELECT user.name, retweeted_status.text, retweeted_status.entities.hashtags[0] AS Hashtags FROM Schiedsrichterbemerkungen WHERE retweeted_status.text LIKE '%referee%' ")


filter_schiri.show(200, False)
# filter_schiri.write.csv("referee_tweets.csv")
#.sql("select regexp_extract_all(Schiedsrichterbemerkungen, '(#\\\\w+)', 1) as a from Schiedsrichterbemerkungen where retweeted_status.text lisparkke '%#%'")

#filter_schiri.select(explode(split("text", "\\s+")).alias("word")) \
#    .where(col("word").startswith("#"))