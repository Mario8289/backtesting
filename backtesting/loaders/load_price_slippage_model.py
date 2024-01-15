import datetime as dt
import os

import pandas as pd
import s3fs

# import logging
from pandas.errors import ParserError

from ..save_simulations import construct_path


class PriceSlippageLoader:
    def __init__(self):
        self.name = "price_slippage_loader"

    def get_model(self, file: str, prefix: str, datasource_label: str, date: dt.date):
        pass


class PriceSlippageS3Loader(PriceSlippageLoader):
    def __init__(self, loader: s3fs):
        self.loader = loader
        self.name = "price_slippage_s3_loader"
        self.execution_attempts = 3
        # self.logger: logging.Logger = logging.getLogger(self.name)

    def get_model(self, file: str, prefix: str, datasource_label: str, date: dt.date):
        path = os.path.join(prefix, construct_path(datasource_label, date), file)

        for i in range(self.execution_attempts):
            try:
                with self.loader.open(path, "rb") as f:
                    df = pd.read_csv(f)
                    return df
            except UnicodeDecodeError:
                pass
                # self.logger.error(f"  method: {self.name}.get_model, attempt: {i + 1}, UnicodeDecodeError {e}")
            except ParserError:
                pass
                # self.logger.error(
                #     f"  method: {self.name}.get_model, attempt: {i + 1}, Pandas ParserError {e}")
        else:
            raise ValueError(f"unable to load data from path: {path}")
