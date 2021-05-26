from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/Users/shiratho1/Documents/Falabella/QA/JSON_GCP/pe_kube.json'

project_id = 'tc-sc-bi-bigdata-cdl-corp-dev'
client = bigquery.Client(project=project_id)

job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

# Start the query, passing in the extra configuration.
query_job = client.query(
    (
"""
insert into `tc-sc-bi-bigdata-cdl-pe-stage.trf_tot_pe_cdp_staging.btd_ebim_tot_transactions` (cdp_cust_id, bu_id, bu_country, tran_id, tran_dt, tran_tm, tran_process_dttm, tran_type_cd, tran_type_desc, tran_click_and_collect_flg, shp_id, ord_id, tran_doc_type, tran_store_flag, tran_channel, tran_sub_channel, loc_id, tran_pos_id, tran_pos_seq_id, tran_slsman_id, tran_return_reason, tran_currency_cd, prod_id, tran_promo_id, tran_promo_desc, tran_total_amt, tran_qty, tran_disc_amt, tran_total_tax_amt, tran_net_amt, tran_cost_amt, tran_number_line, cdp_dag_id, cdp_run_id, cdp_load_timestamp, tran_sub_channel2, tran_sub_channel3)

WITH
ids AS (SELECT cdp_cust_id,cust_id FROM `tc-sc-bi-bigdata-cdl-pe-stage.trf_cor_pe_cdp_staging.btd_ebim_customers_mapping` WHERE bu_id='TOT' and end_date is null),
bu AS (SELECT b.cdp_cust_id, a.* EXCEPT(cust_id) FROM `tc-sc-bi-bigdata-cdl-pe-stage.trf_cor_pe_cdp_staging.btd_ebim_tot_transactions_fact` a LEFT JOIN ids b ON a.cust_id = b.cust_id)

SELECT cdp_cust_id, bu_id, bu_country, tran_id, tran_dt, tran_tm, tran_process_dttm, tran_type_cd, tran_type_desc, tran_click_and_collect_flg, shp_id, ord_id, tran_doc_type, tran_store_flag, tran_channel, tran_sub_channel, loc_id, tran_pos_id, tran_pos_seq_id, tran_slsman_id, tran_return_reason, tran_currency_cd, prod_id, tran_promo_id, tran_promo_desc, tran_total_amt, tran_qty, tran_disc_amt, tran_total_tax_amt, tran_net_amt, tran_cost_amt, tran_number_line, cdp_dag_id, cdp_run_id, cdp_load_timestamp, tran_sub_channel2, tran_sub_channel3
FROM bu
where tran_process_dttm between '2020-01-01' and '2020-01-07';
"""
),
    job_config=job_config,
)  # Make an API request.

# A dry run query completes immediately.
print("This query will process {} bytes.".format(query_job.total_bytes_processed))