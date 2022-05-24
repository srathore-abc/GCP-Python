
import apache_beam as beam
import pandas as pd
import pyarrow
from apache_beam.options.pipeline_options import PipelineOptions

parquet_data = pd.read_parquet("C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\io\\userdata1.parquet", engine ="pyarrow")
# parquet_schema = pyarrow.schema([])
#
# schema_map = {
# "STRING": pyarrow.string(),
# "FLOAT": pyarrow.float64(),
# "STRING": pyarrow.string(),
# "DATE": pyarrow.date64()
# }
#
# for item in parquet_data.schema:
#     parquet_schema = parquet_schema.append(pyarrow.field(item.name, schema_map[item.field_type]))

parquet_write = beam.Pipeline()
content = parquet_write | beam.io.ReadFromParquet("C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\io\\userdata1.parquet")
content | 'Print contents' >> beam.Map(print)
          # | beam.io.parquetio.WriteToParquet("C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\output\\out_parquet.parquet", schema = parquet_schema))

parquet_write.run()