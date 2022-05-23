import   apache_beam as beam
pipeline1 = beam.Pipeline()

dept_count = (
 pipeline1
  |beam.io.ReadFromText(r"C:\Users\SHILRATH\PycharmProjects\GCPBasics\io\testdata.txt")
  | beam.Map(lambda line: line.split(","))
  | beam.Filter(lambda line: line[2] == "Critical")
  | beam.Map(lambda line: (line[2], 1))
  | beam.CombinePerKey(sum)
  | beam.io.WriteToText("output\o1.txt")
 )


pipeline1.run()
