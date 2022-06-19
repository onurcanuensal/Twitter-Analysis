from typing import Iterable, Set, Tuple
from numpy.core.arrayprint import dtype_is_implied
from pandas._config.config import options
from pandas.core.series import Series
import preprocessor as p
import pandas as pd
from textblob import TextBlob
import numpy as np
from matplotlib import pyplot as plt
#jsonFiles = "C:/Users/ArtzT/Documents/Visual Studio Code/Forschungsmodul/jsons/*.json"
dataset = pd.read_csv("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/Forschungsmodul Datenbanken/csvDateien/states_all2ALL.csv", low_memory=False)

count = 0
fehlercount = 0
zahlListe = []
polarities = []
alleTweets = 0

for index, row in dataset.iterrows():
    
    
    try:

        zeile = row[2]
        print(zeile)

        alleTweets += zeile

        #count += 1
        #if(count == 5):
         #   break

    except Exception as e:
        print("Fehler " + str(e))
        fehlercount +=1

print("Alle Tweets: " + str(alleTweets))


