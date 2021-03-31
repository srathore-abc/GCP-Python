from google.cloud import bigquery
import os
import sys
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/shiratho1/Documents/Falabella/QA/JSON_GCP/cl.json'

project_id = 'tc-sc-bi-bigdata-cdl-corp-dev'
client = bigquery.Client(project=project_id)
count=1

queries = ["select count(*) from tc-sc-bi-bigdata-cdl-corp-dev.acc_fal_cl_dev.vw_fal_cl_shipments_fact",
           "select count(*) from tc-sc-bi-bigdata-cdl-corp-dev.acc_sod_cl_dev.vw_sod_cl_shipments_fact",
           "select count(*) from tc-sc-bi-bigdata-cdl-corp-dev.acc_tot_cl_dev.vw_tot_cl_shipments_fact",
           "select count(*) from tc-sc-bi-bigdata-cdl-corp-dev.acc_fal_cl_dev.vw_fal_cl_shipment_events_fact",
           "select count(*) from tc-sc-bi-bigdata-cdl-corp-dev.acc_sod_cl_dev.vw_sod_cl_shipment_events_fact",
           "select count(*) from tc-sc-bi-bigdata-cdl-corp-dev.acc_tot_cl_dev.vw_tot_cl_shipment_events_fact"
           ]
for query in queries:

    query_job = client.query(query)
    data=query_job.result()
    print("Result of Query:" +str(count))
    count+=1
    for row in data:
        print(row)