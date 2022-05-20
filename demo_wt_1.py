import apache_beam as beam
import time

class GetTimestamp(beam.DoFn):
  def process(self, plant, timestamp=beam.DoFn.TimestampParam):
    yield '{} - {}'.format(timestamp.to_utc_datetime(), plant['name'])
input_files = 'C:/Users/SHILRATH/PycharmProjects/GCPBasics/io/testout.txt'
with beam.Pipeline() as pipeline:
  plant_processing_times = (
      pipeline
      | 'Read files' >> beam.io.ReadFromText(input_files)

| 'With timestamps' >> beam.Map(lambda plant: \
          beam.window.TimestampedValue(plant, time.time()))
      | 'Get timestamp' >> beam.ParDo(GetTimestamp())
      | beam.Map(print)
  )