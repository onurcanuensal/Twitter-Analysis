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

schiri = spark.read.json(test)
schiri.createTempView("Schiedsrichterbemerkungen")
schiri_ausgabe = spark.sql("SELECT user.name, retweeted_status.text FROM Schiedsrichterbemerkungen WHERE retweeted_status.text LIKE '%referee%'")

schiri_ausgabe.show(50, False)

#df_output.write.csv('tweets.csv')
#fragen warum das nicht läuft


def umwandeln(text):
    count_parse = 0
    count_polarity = 0
    daten = None
    pol = None
    sub = None
    try:
        daten = TextBlob(text)
    except:
        count_parse +=1
        print("Fehler bei Textumwandlung: TextBlob")

    try:
        pol = daten.polarity
    except:
        count_polarity += 1
        print("Fehler bei Polarity Zuweisung")

    return pol

#pol = df_output["retweeted_status.text"].apply(umwandeln)

df2 = schiri_ausgabe.toPandas()

pol = df2["text"].apply(umwandeln)



#textblob_output = TextBlob(text).polarity

for i in range(100):
    print(str(df2["text"][i])+ "Polarity: " + str(pol[i]) + "\n")


