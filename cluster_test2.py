from pandas.core.frame import DataFrame
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt

polarities = [0.1, 0.3, 0.365, 0.876, 0.996]
subjectivities = [0.15, 0.63, 0.2345, 0.8756, 0.244]

df = [polarities, subjectivities]

# k means
kmeans = KMeans(n_clusters=3, random_state=0)
df['cluster'] = kmeans.fit_predict(polarities, subjectivities)
# get centroids
centroids = kmeans.cluster_centers_
cen_x = [i[0] for i in centroids] 
cen_y = [i[1] for i in centroids]
## add to df
df['cen_x'] = df.cluster.map({0:cen_x[0], 1:cen_x[1], 2:cen_x[2]})
df['cen_y'] = df.cluster.map({0:cen_y[0], 1:cen_y[1], 2:cen_y[2]})
# define and map colors
colors = ['#DF2020', '#81DF20', '#2095DF']
df['c'] = df.cluster.map({0:colors[0], 1:colors[1], 2:colors[2]})

plt.scatter(df.Attack, df.Defense, c=df.c, alpha = 0.6, s=10)