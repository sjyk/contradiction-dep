import pandas as pd
from cgd.dataflow import *

raw_data =  [{'title': 'Employee', 'branch':'SF' , 'salary': 60.0}, 
             {'title': 'Employee' , 'branch': 'SF', 'salary': 60.0},
             {'title': 'Employee', 'branch': 'NY' , 'salary': 100.0},
             {'title': 'Manager' , 'branch': 'SF','salary': 300.0},
             {'title': 'Manager', 'branch':'NY' ,'salary': 390.0},
             {'title': 'Manager', 'branch':'NY' ,'salary': 306.0},
             {'title': 'Sub' , 'branch': 'SF','salary': 10.0},
             {'title': 'Temp', 'branch': 'SF' ,'salary': 20.0},
             {'title': 'Manager', 'branch': 'NY' ,'salary': 400.0}]

df = pd.DataFrame(raw_data, columns=['title', 'branch', 'salary'])
#for i in ThetaJoin(df, df, lambda s,t: s['salary']==t['salary']):
#    print(i)

#print(df[['title','salary']])

from cgd.constraint import *

e = EmbeddedDependency(lambda x: x['title'] == 'Employee', lambda x: x['salary'] < 100)
for i in XOR(e[df],e[df]):
    print(i)