import apache_beam as beam
import pandas as pd


parquet_data = pd.read_parquet("C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\io\\userdata1.parquet", engine ="pyarrow")
parquet_write = beam.Pipeline()
content = parquet_write | beam.io.ReadFromParquet("C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\io\\userdata1.parquet")
content | 'Print contents' >> beam.Map(print)
parquet_write.run()