from abc import ABC
from typing import Dict, AnyStr
from pathlib import Path

import pandas as pd
import yaml
import os


class CsvDataStore(ABC):
    def __init__(self, entry_point: AnyStr):
        self.entry_point: Path = Path(entry_point)  # Corrected assignment

    @classmethod
    def create(cls, attributes):
        instance = cls(**attributes)
        return instance

    def authenticate(self, auth):
        pass

    def load(self, subscription, parameters):
        pass

    @staticmethod
    def build_path(
            base: AnyStr, file: AnyStr = None
    ) -> str:
        if file:
            path = (Path(base) / file).as_posix()
        else:
            path = Path(base).as_posix()

        return path

    @staticmethod
    def load_yaml(
            file: AnyStr
    ) -> Dict:
        with open(file, "r") as inf:
                return yaml.safe_load(inf)

    @staticmethod
    def load_csv(
            file: AnyStr,
    ) -> pd.DataFrame:
        if os.path.exists(file):
            return pd.read_csv(file)

    def get(
            self,
            subscription,
            start_date,
            end_date,
            instruments,
            interval
    ):
        dates = pd.date_range(start_date, end_date)
        combinations = [[d.strftime("%Y-%m-%d"), i] for d in dates for i in instruments]

        files = [
            self.base_directory / subscription / interval / _date / _instrument / '.csv'
            for (_date, _instrument) in combinations
        ]

        data = pd.DataFrame()
        for file in files:
            try:
                _data = pd.read_csv(file)
                data = pd.concat([data, _data])
            except Exception as e:
                raise e

        return data



