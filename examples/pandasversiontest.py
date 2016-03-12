import pandas as pd
from itertools import product

citypairs = [('Miami', 'Boston'), ('Miami','New York'), ('New York', 'San Francisco'),
             ('Boston', 'New York'), ('Boston', 'San Francisco')]

vals = [i*10+10 for i in range(5)]

df = pd.DataFrame({'orig' : [p[0] for p in citypairs],
                   'dest' : [p[1] for p in citypairs],
                   'vals' : vals})

df['orig'] = df['orig'].astype("category")
df['dest'] = df['dest'].astype("category")

df = pd.DataFrame({'orig' : [p[0] for p in citypairs],
                   'dest' : [p[1] for p in citypairs],
                   'vals' : vals})
df.index= pd.MultiIndex.from_tuples(tuple(map(tuple, df[["orig","dest"]].values)), names=["orig", "dest"])


index = pd.MultiIndex.from_tuples(citypairs, names=['origin','dest'])

s = pd.Series([i*10+10 for i in range(5)], index=index)

sourcecities = {p[0] for p in citypairs}

destcities = {p[1] for p in citypairs}

assert not s.loc[next(iter(sourcecities)),:].empty

assert not s.loc[:,next(iter(destcities))].empty

class Sloc(object):
    def __init__(self, s):
        self._s = s
        self._empty = pd.Series([])
    def __getitem__(self, key):
        try:
            return self._s[key]
        except KeyError:
            return self._empty
    @staticmethod
    def add_sloc(s):
        s.sloc = Sloc(s)

Sloc.add_sloc(df["vals"])

assert len(df.vals.sloc["New York",:]) == 1
assert len(df.vals.sloc[:,"New York"]) == 2

assert len(df.vals.sloc["Miami",:]) == 2
assert len(df.vals.sloc[:,"Miami"]) == 0


