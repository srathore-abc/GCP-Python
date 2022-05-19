import pandas as pd, numpy as np
from faker import Faker
#initialize Faker
fake=Faker()
names = []
address = []
company = []

n_names = 20
for n in range(n_names):
    names.append(fake.name())
    address.append(fake.address())
    company.append(fake.company())

claim_reason = ["Medical", "Travel", "Phone", "Other"]
Confidentiality_level = ["High", "Low", "Medium", "Very low"]

claim_confidentiality_dict = dict(zip(claim_reason, Confidentiality_level))

claim_reasons = np.random.choice(claim_reason, n_names, p=[.55, .15, .15, .15])
claim_confidentiality_levels = [claim_confidentiality_dict[claim_reasons[i]] for i in range(len(claim_reasons))]
variables=[names,address,company,claim_reasons,claim_confidentiality_levels]

df=pd.DataFrame(variables).transpose()
print(df)

# df.columns=["Customer Name","Customer Address","Company Name","Claim Reason","Data confidentiality"]
#
# df["Customer Address"]=df["Customer Address"].str.replace("\n",",")

df.columns=["Customer Name","Customer Address","Company Name","Claim Reason","Data confidentiality"]

# pprint(df_columns)
print(df.head(2))
df.to_csv(r'output\dgfile_2.txt', header=True, index=None, sep="|", mode='w')