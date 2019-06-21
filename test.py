import pandas as pd
from cgd.dataflow import *
from cgd.fact import *

raw_data1 =  [{'id': 1, 'title': 'Employee', 'branch':'SF' , 'salary': 60.0}, 
             {'id': 2, 'title': 'Employee' , 'branch': 'SF', 'salary': 60.0},
             {'id': 3, 'title': 'Employee', 'branch': 'NY' , 'salary': 100.0},
             {'id': 4, 'title': 'Manager' , 'branch': 'SF','salary': 300.0},
             {'id': 5, 'title': 'Manager', 'branch':'NY' ,'salary': 390.0},
             {'id': 6, 'title': 'Manager', 'branch':'NY' ,'salary': 306.0},
             {'id': 7, 'title': 'Sub' , 'branch': 'SF','salary': 10.0},
             {'id': 8, 'title': 'Temp', 'branch': 'SF' ,'salary': 20.0},
             {'id': 9, 'title': 'Manager', 'branch': 'NY' ,'salary': 400.0}]

raw_data2 =  [{'id': 1, 'title': 'Employee', 'branch':'SF' , 'salary': 100.0}, 
             {'id': 2, 'title': 'Employee' , 'branch': 'SF', 'salary': 60.0},
             {'id': 3, 'title': 'Employee', 'branch': 'NY' , 'salary': 100.0},
             {'id': 4, 'title': 'Manager' , 'branch': 'SF','salary': 300.0},
             {'id': 5, 'title': 'Manager', 'branch':'NY' ,'salary': 390.0},
             {'id': 6, 'title': 'Manager', 'branch':'NY' ,'salary': 306.0},
             {'id': 7, 'title': 'Sub' , 'branch': 'SF','salary': 10.0},
             {'id': 8, 'title': 'Temp', 'branch': 'SF' ,'salary': 20.0},
             {'id': 9, 'title': 'Manager', 'branch': 'NY' ,'salary': 400.0}]

df1 = pd.DataFrame(raw_data1, columns=['id', 'title', 'branch', 'salary'])
df2 = pd.DataFrame(raw_data2, columns=['id', 'title', 'branch', 'salary'])

mdf = match(df1,df2, ['id'])
#print(mdf)

from cgd.constraint import *

e = EmbeddedDependency(lambda x: x['title'] == 'Employee', lambda x: x['salary'] < 100)

for i in e[df1]:
    print("a",i)

for i in e[df2]:
    print("b",i)

for i in e[mdf]:
    print("c",i)

"""
from cgd.constraint import *

e = EmbeddedDependency(lambda x: x['title'] == 'Employee', lambda x: x['salary'] < 100, suffix='_right')
for i in e[mdf]:
    print(i)
"""
