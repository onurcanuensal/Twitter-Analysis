# import pandas as pd
import pandas as pd
  
# list of strings
lst = ['Geeks', 'For', 'Geeks', 'is', 'portal', 'for', 'Geeks']
  
# list of int
lst2 = [11, 22, 33, 44, 55, 66, 77]
  
# Calling DataFrame constructor after zipping
# both lists, with columns specified
df = pd.DataFrame(list(zip(lst, lst2)),
               columns =['Name', 'val'])
df.head(5)