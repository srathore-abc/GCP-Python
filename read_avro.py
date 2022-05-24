import apache_beam as beam
from avro import schema
import avro
from apache_beam.io import ReadFromAvro
from apache_beam.io import WriteToAvro
schemafile="C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\schema\\schema_Av.avsc"
outputfile="C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\schema\\output5.avro"

schema = avro.schema.parse(open(schemafile, "rb").read())

input_files = 'C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\io\\twitter.avro'
with beam.Pipeline() as pipeline:
  (
      pipeline
      | 'Read files' >> beam.io.ReadFromAvro(input_files)
      | 'Print contents' >> beam.Map(print)
  )