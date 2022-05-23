import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.io import ReadFromText
import re
with beam.Pipeline() as p:
    lines = p | 'Read' >> ReadFromText("C:\\Users\\SHILRATH\\PycharmProjects\\GCPBasics\\io\\read.txt")

    words = (
        lines
        | 'Split' >> beam.FlatMap(
            lambda line: re.findall(r'[\w]+', line)).with_output_types(str)
        # Map to Row objects to generate a schema suitable for conversion
        # to a dataframe.
        | 'ToRows' >> beam.Map(lambda word: beam.Row(word=word)))

    df = to_dataframe(words)
    df['count'] = 1
    counted = df.groupby('word').sum()
    counted.to_csv("output\\write.csv")


