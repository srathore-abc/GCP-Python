from google.cloud import bigquery
import os
import sys

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/shiratho1/Documents/Falabella/QA/JSON_GCP/cl.json'

project_id = 'tc-sc-bi-bigdata-cdl-corp-dev'
client = bigquery.Client(project=project_id)

query_schema_mpl = """
 select count(*) from 
  tc-sc-bi-bigdata-cdl-corp-dev.acc_fal_cl_dev.vw_fal_cl_shipments_fact
 
 """

results_mpl_schema = client.query(query_schema_mpl)
rows = results_mpl_schema.result()  # Waits for query to finish

for row in rows:
    print(row)
    # print("Key" + row)
    # print("email id" +row)


