"""
int, str, float, set, dict, list, tuple
"""

import pandas as pd

df = pd.read_excel("simple.xlsx")
df1 = df.to_csv("simple.csv")
print(df1)

a = set()
# a.add('as')
a.add([1, 2, 3])
print(a)
