import apache_beam as beam
import time

class GetTimestamp(beam.DoFn):
  def process(self, lines, timestamp=beam.DoFn.TimestampParam):
    yield '{},{}'.format(lines,timestamp.to_utc_datetime())


input_files = 'C:/Users/SHILRATH/PycharmProjects/GCPBasics/io/testout.txt'

with beam.Pipeline() as pipeline:

     lines = pipeline | 'Read files' >> beam.io.ReadFromText(input_files,skip_header_lines=1,)
     l1_time= lines| 'With timestamps' >> beam.Map(lambda lines: beam.window.TimestampedValue(lines, time.time()))
     l1_time| 'Get timestamp' >> beam.ParDo(GetTimestamp())| beam.Map(print)
