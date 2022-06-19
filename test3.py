import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib.pyplot as plt

plt.rc('font', size=16)

sns.set_style('white')

#define a custom palette
customPalette = ['#630C3A', '#39C8C6', '#D3500C', '#FFB139']
sns.set_palette(customPalette)
sns.palplot(customPalette)