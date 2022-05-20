from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('C:/Users/SHILRATH/gcp/sa_key.json')

project_id = 'neat-acre-335214'
client = bigquery.Client(credentials= credentials,project=project_id)
count=1

queries = ["select count(*) from neat-acre-335214.testdataset.table3",
           "select * from neat-acre-335214.testdataset.table3"

           ]
for query in queries:

    query_job = client.query(query)
    data=query_job.result()
    print("Result of Query:" +str(count))
    count+=1
    for row in data:
        print(row)