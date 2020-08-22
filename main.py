
import argparse
import os


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText




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
      default = "output/output_json.json",
      help='Output file to write results to.')

    args = parser.parse_args()


    ########## PIPELINE CREATION ###########
    pipeline_options = PipelineOptions(
        temp_location = "tmp/",
        save_main_session = False
    )

    p = beam.Pipeline(
        runner = 'DirectRunner',
        options = pipeline_options
    )

    data = p | "Reading CSV Data" >> ReadFromText(args.input)

    """
    Breaking down the task

    - Read CSV data

    - Create Unique Property Identifier by combining some columns to create a new column

    - Attach meta data about property to Unique Property Identifier

    - Group Transactions by Unique Property Identifier

    - CoGroupByKey on Unique Property Identifier

    - Output to JSON

    - Include arg parsing for reading csv file of choice and outputting to filename of choice

    I noticed some duplicate records, for now as long as they have different IDs I have let them through
    Later it might be a good idea to check for this in an analytics tool such as BigQ to investigate whether
    that duplication is intended in the data or not.

    """

    ####### SAMPLE TESTING #######
    data = data | "Printing" >> beam.Map(print)
    p.run()


