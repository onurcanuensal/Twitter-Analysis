#importing necessary libraries
from afinn import Afinn
import pandas as pd
  
#instantiate afinn
afn = Afinn()
  
#creating list sentences
news_df = ['son of a bitch', 'les gens pensent aux chiens','i hate flowers',
         'hes kind and smart','we are kind to good people', 'i love you']
           
# compute scores (polarity) and labels
scores = [afn.score(article) for article in news_df]
sentiment = ['positive' if score > 0 
                          else 'negative' if score < 0 
                              else 'neutral' 
                                  for score in scores]
      
# dataframe creation
df = pd.DataFrame()
df['topic'] =  news_df
df['scores'] = scores
df['sentiments'] = sentiment
print(df)

#afinn nicht so nice da es nur English, Spanisch, Französisch  und Italienisch kann
