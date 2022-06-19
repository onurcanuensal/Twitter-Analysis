import pandas as pd
from textblob import TextBlob


#eigentlich csv data jedoch klappt das mit dem to csv bei referee_hashtag.py nicht..
test_df = pd.read_json("E:/Uni/SoSe21/Forschungsmodull Datenbanken/Code/Tweets/sample_2014-06-12-18-58-38.348+0200.json")

def pol(text):
    polartiy_tweets = TextBlob(text).polarity
    text = str(text)
    if polartiy_tweets > 0:
        print("Positiver Tweet\n")
    print("Negativer Tweet\n")


test_df["textblob"] = test_df["text"].apply(pol)
#dataframe in csv umwandeln
test_df.to_csv("schiri_tweets.csv")