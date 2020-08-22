
import argparse
import os
import csv


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.coders.coders import ToStringCoder

from custom_functions.PropertyIdentifierFn import PropertyIdentifierFn
from custom_functions.Formatters import JSONFormatterFn
from custom_functions.FileWriters import WriteToNewlineJsonSink


def parse_csv_data(element):
    for line in csv.reader([element], delimiter=','):
        return line


if __name__ == "__main__":
    ########## ARGUMENT PARSING ###########
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      required = True,
      help='Input file to process.')

    parser.add_argument(
      '--output',
      dest='output',
      default = "output/newline_data",
      help='Output file to write results to.')

    args = parser.parse_args()


    ########## PIPELINE CREATION ###########
    pipeline_options = PipelineOptions(
        temp_location = "tmp/",
        save_main_session = True
    )

    p = beam.Pipeline(
        runner = 'DirectRunner',
        options = pipeline_options
    )

    data = (
        p
        | "Reading CSV File" >> ReadFromText(args.input)
        | "Parsing CSV Data" >> beam.Map(parse_csv_data)
        | "Creating Unique Property Key" >> beam.ParDo(PropertyIdentifierFn())
        | "Grouping Transactions" >> beam.GroupByKey()
        | "Formatting Results" >> beam.ParDo(JSONFormatterFn())
        | "Writing To Files" >> WriteToNewlineJsonSink(args.output, coder = ToStringCoder())
    )


