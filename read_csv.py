import pandas as pd
import apache_beam as beam

with beam.Pipeline() as pipeline:
  (
      pipeline
      | 'Filename' >> beam.Create(["C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\io\\solar_events.csv"])

      # Each element is a Pandas DataFrame, so we can do any Pandas operation.
      | 'Read CSV' >> beam.Map(pd.read_csv)

      # We yield each element of all the DataFrames into a PCollection of dictionaries.
      | 'To dictionaries' >> beam.FlatMap(lambda df: df.to_dict('records'))

      # Reshuffle to make sure parallelization is balanced.
      | 'Reshuffle' >> beam.Reshuffle()

      # Print the elements in the PCollection.
      | 'Print' >> beam.Map(print)
  )