import datetime as dt
from abc import abstractmethod

import pandas as pd


class Writer:
    def __init__(
            self,
            name: str,
    ):
        self.name: str = name

    @classmethod
    def create(cls, attributes):
        instance = cls(**attributes)
        return instance

    @abstractmethod
    def write_results(
            self,
            results: pd.DataFrame,
            mode: str,
            store_index: bool,
            file: str = None,
            date: dt.date = None,
    ):
        pass
