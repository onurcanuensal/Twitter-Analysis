from os import read
from nltk import text
import pandas as pd
from textblob import TextBlob


def sub(text):
    return TextBlob(text).subjectivity

def pol(text):
    return TextBlob(text).polarity

df = pd.read_json("E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/sample_2014-06-12-18-58-38.348+0200.json", lines=True)

print(df[["text", "id_str"]])
df["Subjectivity"] = df["text"].apply(sub)
df["Polarity"] = df["text"].apply(pol)
print(df[["text", "Subjectivity", "Polarity"]])