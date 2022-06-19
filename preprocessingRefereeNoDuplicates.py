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
dataset = pd.read_csv("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/Forschungsmodul Datenbanken/csvDateien/popular_text_ALL.csv", skiprows=1, low_memory=False)

count = 0
fehlercount = 0
textListe = []
polarities = []

for index, row in dataset.iterrows():
    
    
    try:
    
        zeile = p.clean(row[1])             # preprocessing
        #print(str(index), " ",zeile)

        #print("CleanedText: " + str(zeile))
        
        #print("Polarity: " + str(polarity))
        #print()


        textListe.append(zeile)                   # save in list
       
    
    except:
        print("Fehler")
        fehlercount +=1

positiveTweets = 0
negativeTweets = 0
neutraleTweets = 0

# eliminate duplicates
myList = list(dict.fromkeys(textListe))

for item in myList:
   
   polarity = TextBlob(item).polarity

   polarities.append(polarity)

   print(str(count) + " " + str(polarity) + " " +str(item))
   count += 1 

for index in range(len(myList)):
    if polarities[index] > 0.0:
        positiveTweets += 1
    if polarities[index] < 0.0:
        negativeTweets += 1
    if polarities[index] == 0:
        neutraleTweets += 1


#-0.06666666666666665 seriously though, i wanna know, how much does it cost to pay off a referee, brazil?
# 0.16666666666666666 yes, but brazil played without their main player, the referee
# -0.625 fuck that referee!!
# 0.6 nice to see this referee isn't bowing to the pressure of the brazil fans

print("Länge der polarities: " + str(len(polarities)))
print("Länge der Textliste: " + str(len(textListe)))
print("Länge ohne Duplikate: " + str(len(myList)))
print("Fehlercount: " + str(fehlercount))
print()
print("Positive Tweets: " + str(positiveTweets))
print("Negative Tweets: " + str(negativeTweets))
print("Neutrale Tweets: " + str(neutraleTweets))


bins = np.arange(-1.1, 1.1, 0.01)
plt.xlim(-1.1, 1.1)

plt.hist(polarities, bins = bins)
plt.title("Polarity Verteilung")
plt.xlabel("Polarities")
plt.ylabel("Anzahl")

plt.show()



#print(dataset.iteritems)

#dataset.drop(['url'],axis=1)

#dataset2.toPandas().to_csv("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/Forschungsmodul Datenbanken/csvDateien/referee_tweets_all_cleared.csv")


