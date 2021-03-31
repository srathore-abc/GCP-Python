from google.cloud import bigquery
from google.cloud import storage
import json

client = bigquery.Client.from_service_account_json('C:/Users/shiratho1/Documents/Falabella/QA/JSON_GCP/cl.json')


def test_extract_schema(client):
    project = 'tc-sc-bi-bigdata-cdl-corp-dev'
    dataset_id = 'acc_sod_cl_dev'
    table_id = 'vw_sod_cl_shipments_fact'

    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    filename = table_id.replace('vw_', '')
    result = ["{0} {1}".format(schema.name, schema.field_type) for schema in table.schema]  # API Request
    list = str(result).lower().replace('[', ' ').replace(']', '').replace("'", '').split(',')
    # fields in the sample file
    fields = ['mode', 'name', 'description', 'type']
    json_data = []
    file1 = open(r"schemajson/" + filename + "1.json", "w+")
    print(r"Schema of the table " + filename + " is :")
    for field in list:
        fld = field.split(' ')
        description = ['NULLABLE', fld[1], fld[1].replace('_', ' '), fld[2]]
        dict2 = {}
        i = 0

        while i < len(fields):
            # creating dictionary for each employee
            dict2[fields[i]] = description[i]
            i = i + 1

        json_line = json.loads(json.dumps(dict2, indent=4, sort_keys=True))

        print(json_line)
        json_data.append(json_line)
        # print(json_data)
    file1.write(str(json_data).replace("'", '"'))


# View table properties
if __name__ == '__main__':
    test_extract_schema(client)
