from backtesting.writers.writer import Writer
from ..writers.csv_writer import CsvWriter


def get_writer(datastore):
    if datastore == CsvWriter.__name__:
        writer = CsvWriter
    return writer


def create_writer(writer_name: str, datastore_parameters) -> Writer:
    _writer = get_writer(writer_name)
    writer_ = _writer.create(
        datastore_attributes=datastore_parameters
    )

    return writer_

