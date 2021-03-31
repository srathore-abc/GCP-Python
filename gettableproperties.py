from google.cloud import bigquery
import os
# Construct a BigQuery client object.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/shiratho1/Documents/Falabella/QA/JSON_GCP/cl.json'
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the model to fetch.
table_id = 'tc-sc-bi-bigdata-cdl-corp-dev.acc_fal_cl_dev.vw_fal_cl_shipment_events_fact'

table = client.get_table(table_id)  # Make an API request.

# View table properties
print(
    "Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)
)
print("Table schema: {}".format(table.schema))
print("Table description: {}".format(table.description))
print("Table has {} rows".format(table.num_rows))