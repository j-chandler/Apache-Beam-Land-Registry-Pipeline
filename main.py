
import argparse


import apache_beam as beam




if __name__ == "__main__":
    pipeline_options = PipelineOptions(
        temp_location = "tmp/",
        save_main_session = False,
        setup_file = os.path.join(os.getcwd(), "setup.py")
    )

    p = beam.Pipeline(
        runner = 'DirectRunner',
        options = pipeline_options
    )

    #p = p | "Reading CSV Data" >> 

    """
    Breaking down the task

    - Read CSV data

    - Create Unique Property Identifier by combining some columns to create a new column

    - Attach meta data about property to Unique Property Identifier

    - Group Transcations by Unique Property Identifier in it's own list?

    - Output to JSON

    - Include arg parsing for reading csv file of choice and outputting to filename of choice

    I noticed some duplicate records, for now as long as they have different IDs I have let them through
    Later it might be a good idea to check for this in an analytics tool such as BigQ to investigate whether
    that duplication is intended in the data or not.

    """

