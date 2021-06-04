from google.cloud import bigquery
import os
import time

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/shiratho1/Documents/Falabella/QA/JSON_GCP/pe_kube.json'
project_id = 'tc-sc-bi-bigdata-cdl-co-dev'
client = bigquery.Client(project=project_id)

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "tc-sc-bi-bigdata-cdl-pe-dev.trf_cor_pe_cdp_dev.xref_process_inventory"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, Autodetect=True)

with open("FILES/xref_pi_pe.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)