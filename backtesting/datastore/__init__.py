from backtesting.datastore.csv_datastore import CsvDataStore


def get_datastore(datastore):
    if datastore == CsvDataStore.__name__:
        datastore = CsvDataStore
    return datastore
