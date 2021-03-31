from google.cloud import bigquery
import os
import sys

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/shiratho1/Documents/Falabella/QA/JSON_GCP/cl.json'

project_id = 'tc-sc-bi-bigdata-cdl-corp-dev'
client = bigquery.Client(project=project_id)

query_schema_mpl = """
select distinct key, value, true as active_ind,'2021-03-17' as last_updated_dt, '' as comments from 
(select 'cust_key_id' as key ,cust_key_id as value, bu_id,count(1) from tc-sc-bi-bigdata-cdl-corp-dev.acc_mpl_cl_dev.vw_mpl_cl_customers_dim group by cust_key_id,bu_id having count(*) >10 union all select 'cust_key_email_id' as key, cust_key_email_id as value, bu_id,count(1) from tc-sc-bi-bigdata-cdl-corp-dev.acc_mpl_cl_dev.vw_mpl_cl_customers_dim group by cust_key_email_id,bu_id having count(*) >10 union all select 'cust_key_phone_num' as key, cust_key_phone_num as value, bu_id,count(1) from tc-sc-bi-bigdata-cdl-corp-dev.acc_mpl_cl_dev.vw_mpl_cl_customers_dim group by cust_key_phone_num,bu_id having count(*) >10 )
 where bu_id = 'MPL' """

results_mpl_schema = client.query(query_schema_mpl)

rows = results_mpl_schema.result()  # Waits for query to finish

for row in rows:
    print(row.key,",",row.value,",","true",",",row.last_updated_dt,",",row.comments)


