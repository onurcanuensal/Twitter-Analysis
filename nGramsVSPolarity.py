from typing import Iterable, Set, Tuple
from nltk.util import ngrams
from numpy.core.arrayprint import dtype_is_implied
from pandas._config.config import options
from pandas.core.series import Series
import preprocessor as p
import pandas as pd
from textblob import TextBlob
import numpy as np
from matplotlib import colors, pyplot as plt
from textblob.blob import Sentence

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


#jsonFiles = "C:/Users/ArtzT/Documents/Visual Studio Code/Forschungsmodul/jsons/*.json"
dataset = pd.read_csv("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/Forschungsmodul Datenbanken/csvDateien/text_for_ngramsVSpolarity.csv", low_memory=False)

count = 0
fehlercount = 0
textListe = []
polarities = []
polaritiesNGRAM = []

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

    if len(textListe) == 300:
        break



# eliminate duplicates
myList = list(dict.fromkeys(textListe))

print(len(myList))

count = 0
for item in myList:
   polarityNgram = 0
   countNGrams = 0

   polarity = TextBlob(item).polarity

   polarities.append(polarity)

   nGrams = find_ngrams(5, item)

   for splitSentence in nGrams:
       polarityNgram += TextBlob(splitSentence).polarity
       countNGrams += 1

   try: 
        totalnGramPolarity = polarityNgram / countNGrams
   except:
       totalnGramPolarity = 0
   polaritiesNGRAM.append(totalnGramPolarity)

   if polarity < 0.0:
    print("Satz " + str(count+1) + ":" + str(item))
    print("Normale Polarity: " + str(polarity))
    print("nGram Polarity: " + str(totalnGramPolarity))
    print()
    count += 1 

polar = np.array(polarities)
ngram = np.array(polaritiesNGRAM)

#plt.plot(range(1,len(polar)+1), polar, label="ohne NGRAM", color="green")
#plt.plot(range(1, len(ngram)+1), ngram, label="mitNGRAM (3-GRAM)", color="red")
X = np.arange(len(polar))
#fig = plt.figure()
#ax = fig.add_axes([0,0,1,1])
#ax.bar(X + 0.00, polar, color = 'g', width = 0.5)
#ax.bar(X + 0.5, ngram, color = 'r', width = 0.5)

plt.bar(X, polar, width=0.5, label="ohne Ngram")
plt.bar(X+0.5, ngram, width=0.5, label="mit NGRAM (5-GRAM)")

plt.xlabel("Tweets")
plt.ylabel("Polarity")
plt.legend()
plt.show()


#-0.06666666666666665 seriously though, i wanna know, how much does it cost to pay off a referee, brazil?
# 0.16666666666666666 yes, but brazil played without their main player, the referee
# -0.625 fuck that referee!!
# 0.6 nice to see this referee isn't bowing to the pressure of the brazil fans


