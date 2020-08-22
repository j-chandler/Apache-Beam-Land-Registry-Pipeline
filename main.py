
import argparse
import os
import csv


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText



class PropertyIdentifierFn(beam.DoFn):
    headers = [
        "Transaction Unique Identifier", 
        "Price",
        "Date of Transfer",
        "Postcode",
        "Property Type",
        "Old/New",
        "Duration",
        "PAON",
        "SAON",
        "Street",
        "Locality",
        "Town/City",
        "District",
        "County",
        "PPD Category Type",
        "Record Status"
    ]

    def process(self, element):
        row = {}

        for i, data in enumerate(element):
            try:
                row[self.headers[i]] = data.strip()
            except:
                print((i, data))
                #TODO: Handle Incorrect Column Amounts


        ###### Joining Cells To Create Overall Property Name #######
        property_name = ""
        address_order = [
            row["PAON"],
            row["SAON"],
            row["Street"],
            row["Locality"],
            row["Town/City"],
            row["District"],
            row["County"]
        ]

        for placement in address_order:
            if placement:
                if not property_name:
                    property_name = placement

                else:
                    property_name += " {}".format(placement)

        row["Property Name"] = property_name

        yield row


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
      default = "output/output_json.json",
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
    )

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


