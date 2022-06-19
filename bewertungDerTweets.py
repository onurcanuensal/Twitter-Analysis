import pandas as pd
from textblob import TextBlob

def sub(text):
    daten = None
    try:
        daten = TextBlob(text).subjectivity
    except:
        print("Fehler bei der Subjectivity")
    
    return daten

def pol(text):
    daten = None
    try:
        daten = TextBlob(text).polarity
    except:
        print("Fehler bei der Polarity")

    return daten

df = pd.read_json("C:/Users/ArtzT/Documents/WordPad Dokumente/Universität/INHALT STUDIENGANG INFORMATIK/6 Semester/SOES/Übung/uebung07/input/sample_2014-06-12-19-51-21.725+0200.json", lines=True)
#print(df[["text", "id_str"]])
#df["Subjectivity"] = df["text"].apply(sub)
df["Polarity"] = df["text"].apply(pol)

posCounter = 0
negCounter = 0
for i in range(len(df["Polarity"])):
    if df["Polarity"][i] > 0.9:
        try:
            posCounter += 1
        except:
            print("Fehler")
    
    if df["Polarity"][i] < 0.1:
        try:
            negCounter += 1
        except:
            print("Fehler")

print("Positiv: " + str(posCounter))
print("Negativ: " + str(negCounter))