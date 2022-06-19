from typing import Text
from nltk.util import pr
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import pyspark.sql.functions as func
import os
import sys
from pyspark.sql import SparkSession
import pyspark
from textblob import TextBlob 
import pandas as pd
from textblob.en import polarity


def umwandeln(text):
    daten = None
    pol = None
    sub = None
    try:
        daten = TextBlob(text)
    except:
        print("Fehler bei Textumwandlung: TextBlob")

    try:
        pol = daten.polarity
    except:
        print("Fehler bei Polarity Zuweisung")

    return pol


# environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#sc = pyspark.SparkContext
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#path
df = pd.read_json("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/SOES/Übung/uebung07/input/sample_2014-06-12-19-51-21.725+0200.json", lines=True)

pol = df["text"].apply(umwandeln)



posCounter = 0
negCounter = 0
for i in range(len(pol)):
    if posCounter >= 10 and negCounter >= 10:
        break
    if pol[i] > 0.9:
        try:
            posCounter += 1
            print("Positiver Text: " + str(df["text"][i]) + "\n" + str(pol[i]))
        except:
            print("Fehler")

    if pol[i] < 0.1:
        try:
            negCounter += 1
            print("Negativer Text: " + str(df["text"][i]) + "\n" + str(pol[i]))

        except:
            print("Fehler")

print("\n")
print("Positive Tweets: " + str(posCounter))
print("Negative Tweets: " + str(negCounter))
print("\n")

"""
df = "\U0001f648" 
value = TextBlob(df).polarity
print(value)

gibt uns für den Affen eine 0.0 als Polarity
"""
