
import apache_beam as beam




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