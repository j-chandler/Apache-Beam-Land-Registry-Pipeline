
import apache_beam as beam


class TransactionSeparatorFn(beam.DoFn):
    transaction_columns = [
        "Transaction Unique Identifier", 
        "Price",
        "Date of Transfer",
        "Postcode",
        "Property Type",
        "Old/New",
        "Duration",
        "PPD Category Type",
        "Record Status",
        "PAON",
        "SAON",
        "Street",
        "Locality",
        "Town/City",
        "District",
        "County"
    ]

    def process(self, element):
        transaction_data = {}

        try:
            for col in self.transaction_columns:
                transaction_data[col] = element[col]
        except:
            ##TODO: Handle missing columns
            print(("Missing Columns", element, self.transaction_columns))

        yield (element["Property Name"], transaction_data)


class AddressSeparatorFn(beam.DoFn):
    address_columns = [
        "PAON",
        "SAON",
        "Street",
        "Locality",
        "Town/City",
        "District",
        "County"
    ]

    def process(self, element):
        data = []

        try:
            for col in self.address_columns:
                data.append(element[col])
        except:
            ##TODO: Handle missing columns
            print(("Missing Columns", element, self.address_columns))

        yield (element["Property Name"], *data)

