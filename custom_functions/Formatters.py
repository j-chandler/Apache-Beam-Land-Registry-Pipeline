
import apache_beam as beam


import apache_beam as beam

class JSONFormatterFn(beam.DoFn):
    property_keys = [
        "PAON",
        "SAON",
        "Street",
        "Locality",
        "Town/City",
        "District",
        "County"
    ]


    def process(self, element):
        formatted = {
        }

        transactions = element[1]
        formatted["property_data"] = self.get_property_data(transaction = transactions[0])

        for transaction in transactions:
            for key in self.property_keys:
                del transaction[key]

        formatted["transactions"] = transactions

        yield {element[0] : formatted}


    def get_property_data(self, transaction):
        property_data = {}

        for key in self.property_keys:
            property_data[key] = transaction[key]

        return property_data