from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
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

# Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json, schema=None):
  # SparkSessions are available with Spark 2.0+
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))