from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import pyspark.sql.functions as func
import os
import sys
from pyspark.sql import SparkSession
import pyspark
from textblob import TextBlob
from pyspark.sql.functions import split, explode, col
#ohne panda
# environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#sc = pyspark.SparkContext
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#paths
path = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/*.json"
test = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/sample_2014-06-12-18-58-38.348+0200.json"
test2 = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/sample_2014-06-12-21-16-27.801+0200.json"

hashtags = spark.read.json(test2)
hashtags.createTempView("Hash")
filter_hashtag = spark.sql("SELECT user.name, retweeted_status.text, retweeted_status.entities.hashtags[0] AS Hashtags, count(Hashtags) FROM Hash GROUP BY count(Hashtags)")


filter_hashtag.show(20, False)
