import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

colors = ['#DF2020', '#81DF20']

polarities = [0.1, 0.3, 0.365, 0.876, 0.996]
subjectivities = [0.15, 0.63, 0.2345, 0.8756, 0.244]


fig, ax = plt.subplots(1, figsize=(8,8))
# plot data
plt.scatter(polarities, subjectivities, alpha = 0.6, s=1)
# plot centroids
plt.scatter(polarities, subjectivities, marker='^', c=colors, s=10)
# plot lines
# legend

legend_elements = [Line2D([0], [0], marker='o', color='w', label='Cluster {}'.format(i+1), 
                   markerfacecolor=mcolor, markersize=5) for i, mcolor in enumerate(colors)]
legend_elements.extend([Line2D([0], [0], marker='^', color='w', label='Centroid - C{}'.format(i+1), 
            markerfacecolor=mcolor, markersize=10) for i, mcolor in enumerate(colors)])
plt.legend(handles=legend_elements, loc='upper right', ncol=2)
# x and y limits
plt.xlim(-1.1, 1.1)
plt.ylim(0,1.1)
# title and labels
plt.title('Verteilung der Polartiy\n', loc='left', fontsize=22)
plt.xlabel('Polarity')
plt.ylabel('Subjectivity')
plt.show()