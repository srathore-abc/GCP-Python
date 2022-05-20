import apache_beam as beam

class GetTimestamp(beam.DoFn):
  def process(self, plant, timestamp=beam.DoFn.TimestampParam):
    yield '{} - {}'.format(timestamp.to_utc_datetime(), plant['name'])

with beam.Pipeline() as pipeline:
  plant_timestamps = (
      pipeline
      | 'Garden plants' >> beam.Create([
          {'name': 'Strawberry', 'season': 1585699200}, # April, 2020
          {'name': 'Carrot', 'season': 1590969600},     # June, 2020
          {'name': 'Artichoke', 'season': 1583020800},  # March, 2020
          {'name': 'Tomato', 'season': 1588291200},     # May, 2020
          {'name': 'Potato', 'season': 1598918400},     # September, 2020
      ])
      | 'With timestamps' >> beam.Map(
          lambda plant: beam.window.TimestampedValue(plant, plant['season']))
      | 'Get timestamp' >> beam.ParDo(GetTimestamp())
      | beam.Map(print)
  )