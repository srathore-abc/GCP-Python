import pandas as pd, numpy as np
# import random,requests
# from bs4 import BeautifulSoup
from faker import Faker
# import phonenumbers
#initialize Faker
fake=Faker()
name = []
empid = []
salary = []
#
n_names = 20
for n in range(n_names):
    name.append(fake.name())
    empid.append(fake.random_int(1, n_names))
    salary.append(fake.random_int())

dept_choice = ["IT","CS","ECE","EEE"]
dept = np.random.choice(dept_choice, p=[.55, .15, .15, .15])
variables=[empid,name,dept,salary]

df=pd.DataFrame(variables).transpose()
# print(df)
df.columns=["empid","name","dept","salary"]
# pprint(df_columns)
print(df.head(2))
df.to_csv(r'output\dgfile_1.txt', header=True, index=None, sep=",", mode='w')