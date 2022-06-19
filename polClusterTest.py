import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


#liste der wert für die polarity und subjectivity
polarities = [-0.9, -0.5, 0.1, 0.3, 0.365, 0.876, 0.555, 0.996, 0.7, -0.9, 1.0,  0.15, 0.63, 0.2345, 0.8756, 0.244,0.4940561937319864]


subjectivities = [0.4940561937319864 , 0.7, -0.9, 1.0,  0.15, 0.63, 0.2345, 0.8756, 0.244, -0.9, -0.5, 0.1, 0.3, 0.365, 0.876, 0.555, 0.996]

#aus den beiden listen ein
#pandas dataframe erstellen
#da ich irgendwie keine andere lösung für das 
#clustering gefunden haben
df = pd.DataFrame(list(zip(polarities, subjectivities)), columns=['Polarity', 'Subjectivity'])


#kmeans
from sklearn.cluster import KMeans

kmeans = KMeans(n_clusters=3, random_state=0)
df['cluster'] = kmeans.fit_predict(df[['Polarity', 'Subjectivity']])


#get centroids
centroids = kmeans.cluster_centers_
cen_x = [i[0] for i in centroids]
cen_y = [i[1] for i in centroids]

#add to dataframe
df['cen_x'] = df.cluster.map({0:cen_x[0], 1:cen_x[1], 2:cen_x[2]})
df['cen_y'] = df.cluster.map({0:cen_y[0], 1:cen_y[1], 2:cen_y[2]})


#Farben definieren
customPalette = ['#630C3A', '#39C8C6', '#D3500C']
df['c'] = df.cluster.map({0:customPalette[0], 1:customPalette[1], 2:customPalette[2]})

plt.scatter(df.Polarity, df.Subjectivity, c=df.c, alpha=0.6, s=10)
plt.show()