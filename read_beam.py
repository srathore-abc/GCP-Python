import apache_beam as beam

input_files = 'C:/Users/SHILRATH/PycharmProjects/GCPBasics/io/testout.txt'
with beam.Pipeline() as pipeline:
  (
      pipeline
      | 'Read files' >> beam.io.ReadFromText(input_files)
      | 'Print contents' >> beam.Map(print)
  )