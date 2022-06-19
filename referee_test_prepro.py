from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import pyspark.sql.functions as func
import os
import sys
from pyspark.sql import SparkSession
import pyspark
from textblob import TextBlob

# environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#sc = pyspark.SparkContext
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#path
path = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/*.json"
test = "E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/sample_2014-06-12-18-58-38.348+0200.json"

df = spark.read.json(test)
df.createTempView("Schiedsrichtererwaehnungen")
df_output  = spark.sql("SELECT user.name AS username, text AS text FROM Schiedsrichtererwaehnungen WHERE retweeted_status.text LIKE '%referee%'")
#df_output.show(100, False)
df_output.toPandas().to_csv("E:/Uni/SoSe21/Forschungsmodull Datenbanken/referee_test.csv")