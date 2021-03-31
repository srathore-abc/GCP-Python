from google.cloud import bigquery

client = bigquery.Client.from_service_account_json('C:/Users/shiratho1/Documents/Falabella/QA/JSON_GCP/cl.json')

def test_extract_schema(client): 
    project = 'tc-sc-bi-bigdata-cdl-corp-dev'
    dataset_id = 'acc_sod_cl_dev'
    table_id = 'vw_sod_cl_shipments_fact'

    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref) 
    filename = table_id.replace('vw_','')
    result = ["{0} {1}".format(schema.name,schema.field_type) for schema in table.schema] # API Request
    print(type(table.schema))
    print(type(result))
    list = str(result).lower().replace('[',' ').replace(']','').replace("'",'').split(',')
    file1 = open(r"C:/Users/shiratho1/PycharmProjects/basicgcp/bq_raw/"+filename+"1.yaml","w+")
    file2 = open(r"C:/Users/shiratho1/PycharmProjects/basicgcp/raw_trf/"+filename+"1.yaml","w+")

    file1.write('---\n')
    file2.write('---\n')

    for field in list:
         fld = field.split(' ')
         file1.write(fld[1]+':\n')
         file1.write('  datatype: string\n')
         file1.write('  transformation_rule: '+fld[1]+' is transferred directly from the origin\n')
         file1.write('  sources:\n')
         file1.write('    '+fld[1]+': '+fld[2]+'\n')
         file2.write(fld[1]+':\n')
         file2.write('  datatype: '+fld[2]+'\n')
         file2.write('  transformation_rule: '+fld[1]+' is transferred directly from the origin\n')
         file2.write('  sources:\n')
         file2.write('    '+fld[1]+': string\n')
    file1.close()
    file2.close()
    replacements = { '@bu':'sod', '@country':'cl', '@entity':'shipment_events_fact', '#bu':'SOD','#country':'CL','@dataloader':'DataLoader'}

    with open('dag_sample/dim_dag_template.yaml') as infile, open("C:/Users/shiratho1/PycharmProjects/basicgcp/dag_configs/"+table_id+".yaml", 'w+') as outfile:
        for line in infile:
            for src, target in replacements.items():
                line = line.replace(src, target)
            outfile.write(line)
# View table properties

if __name__ == '__main__':
    test_extract_schema(client)