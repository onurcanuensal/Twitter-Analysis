from textblob import TextBlob
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import pyspark.sql.functions as func
import os
import sys
from pyspark.sql import SparkSession
import pyspark
import pandas as pd

"""
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
schiri.createGlobalTempView("Schiedsrichterbemerkunguen")
schiri_output = spark.sql("SELECT retweeted_status.text FROM Schiedsrichterbemerkungen WHERE retweeted_status.text LIKE '%referee%'")


df = pd.DataFrame(schiri_output)
"""

#ngrams
def find_ngrams(n, input_sequence):
    # Split sentence into tokens.
    tokens = input_sequence.split()
    ngrams = []
    for i in range(len(tokens) - n + 1):
        # Take n consecutive tokens in array.
        ngram = tokens[i:i+n]
        # Concatenate array items into string.
        ngram = ' '.join(ngram)
        ngrams.append(ngram)

    return ngrams

if __name__ == '__main__':
    my_string = "this game was so fucking amazing" # normal: 0.1, durchschn. = -0.19
    my_string2 = "Fuck this referee"    #-0.4
    my_string3 = "I dont know what to say but this referee is such an idiot" # normal: -0.4, durchschn. = -0.03
    my_string4 = "The best referee in the world!" #normal: 1.0, durchschn. = 0.5

# --> Normal polarity performs better than ngrams

    totalPolarity = 0

    ngrams = find_ngrams(3, my_string)
    analysis = {}
    for ngram in ngrams:
        blob = TextBlob(ngram)
        print('Ngram: {}'.format(ngram))
        print('Polarity: {}'.format(blob.sentiment.polarity))
        print('Subjectivity: {}'.format(blob.sentiment.subjectivity))
        totalPolarity += blob.sentiment.polarity

    dividedPolarity = totalPolarity / len(ngrams)


    print("Die Gesamtpolarity beträgt: " + str(totalPolarity))
    print("Anzahl der ngrams beträgt: " + str(len(ngrams)))
    print("durchschnittliche Polarity: " + str(dividedPolarity))
    print("Normale Polarity: " + str(TextBlob(my_string).polarity))