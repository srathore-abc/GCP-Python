import pandas as pd, numpy as np
# import random,requests
# from bs4 import BeautifulSoup
from faker import Faker
# import phonenumbers
#initialize Faker
fake=Faker()
names = []
address = []
company = []
date = []
phone= []
#


n_names = 20
for n in range(n_names):
    names.append(fake.name())
    address.append(fake.address())
    company.append(fake.company())
    date.append(fake.date_between(start_date='today', end_date='+30y'))
    phone.append(fake.random_int(92345678, 99999999))

variables=[names,address,phone,company,date]
# variables=[names,address,company,date]

df=pd.DataFrame(variables).transpose()
print(df)
# For below clumns uncomment 30 31
df.columns=["Customer Name","Customer Address","Phone","Company Name","Date"]
df["Customer Address"]=df["Customer Address"].str.replace("\n",",")




# pprint(df_columns)
print(df.head(2))
df.to_csv(r'output\dgfile_6.txt', header=True, index=None, sep="|", mode='w')