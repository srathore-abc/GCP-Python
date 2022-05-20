import apache_beam as beam

output_file_name_prefix = 'outputs/file'
with beam.Pipeline() as pipeline:
  (
      pipeline
      | 'Create file lines' >> beam.Create([
          'Each element must be a string.',
          'It writes one element per line.',
          'There are no guarantees on the line order.',
          'The data might be written into multiple files.',
      ])
      | 'Write to files' >> beam.io.WriteToText(
          output_file_name_prefix,
          file_name_suffix='.txt')
  )