from typing import Dict, Any, AnyStr

import datetime as dt
import os

import pandas as pd

from backtesting.writers.writer import Writer
from backtesting.datastore.csv_datastore import CsvDataStore


class CsvWriter(Writer):
    def __init__(self, datastore):
        self.datastore: CsvDataStore = datastore
        super().__init__('CsvWriter')

    @classmethod
    def create(
            cls,
            datastore_attributes: Dict[AnyStr, Any]
    ):

        _auth = datastore_attributes.get('auth', {})

        datastore = CsvDataStore.create(
            {k: v for (k, v) in datastore_attributes.items() if k != 'auth'}
        )

        datastore.authenticate(
            auth=_auth
        )

        instance = cls(datastore=datastore)

        return instance

    def write_results(
            self,
            results: pd.DataFrame,
            mode: str,
            store_index: bool,
            file: str = None,
            date: dt.date = None,
    ):
        results["creation_timestamp"] = dt.datetime.now()
        results["trading_session"] = results["trading_session"].apply(
            lambda x: x.strftime("%Y-%m-%d")
        )

        path = os.path.join(self.datastore.entry_point, file)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        results.to_csv(path)
