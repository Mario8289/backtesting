import datetime as dt
import os

import pandas as pd

from risk_backtesting.writers.writer import Writer


class LocalWriter(Writer):
    def __init__(self):
        super().__init__("writer_local")

    def write_results(
            self,
            results: pd.DataFrame,
            mode: str,
            datasource_label: str,
            prefix: str,
            store_index: bool,
            bucket: str = None,
            date: dt.date = None,
            file: str = None,
    ):
        results["creation_timestamp"] = dt.datetime.now()
        results["trading_session"] = results["trading_session"].apply(
            lambda x: x.strftime("%Y-%m-%d")
        )

        path = os.path.join(prefix, file)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        results.to_csv(path)
