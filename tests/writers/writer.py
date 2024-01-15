import datetime as dt
from abc import ABC, abstractmethod

import pandas as pd


class Writer(ABC):
    def __init__(self, name: str):
        self.name: str = name

    @abstractmethod
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
        pass
