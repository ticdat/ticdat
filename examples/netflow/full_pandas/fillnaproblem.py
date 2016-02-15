from gurobipy import *
import  pandas as pd

m = Model()

v = m.addVar()

m.update()

df = pd.DataFrame({"a":[1, 2, 3], "b" : [4, 5, 6], "data_1":[1, 12, 19]})
df.set_index(["a", "b"], inplace=True)

df2 = pd.DataFrame({"a":[1, 2, 5], "b" : [4, 5, 2], "data_1":[1, 12, 19]})
df2.set_index(["a", "b"], inplace=True)

df.join(df2, how="outer", rsuffix="_2").fillna(v)


