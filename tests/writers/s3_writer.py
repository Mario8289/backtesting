import datetime as dt
import os

import numpy as np
import pandas as pd
import s3fs

from risk_backtesting.writers.writer import Writer


def construct_path(datasource_label: str, date: dt.date = None) -> str:
    if date is not None:
        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")
        path = "datasource_label={}/year={}/month={}/day={}".format(
            datasource_label, year, month, day
        )
    else:
        path = "datasource_label={}".format(datasource_label)

    return path


class S3Writer(Writer):
    def __init__(self, filesystem: s3fs.S3FileSystem):
        super().__init__("s3_writer")
        self.filesystem: s3fs.S3FileSystem = filesystem

    def write_results(
            self,
            results: pd.DataFrame,
            datasource_label: str,
            prefix: str,
            store_index: bool,
            mode: str = "w",
            bucket: str = None,
            date: dt.date = None,
            file: str = None,
    ):
        results["creation_timestamp"] = dt.datetime.now()
        if (
                "trading_session" in results.columns
        ):  # when no resample is set this is not in the dataframe?
            results["trading_session"] = results["trading_session"].apply(
                lambda x: x.strftime("%Y-%m-%d")
            )

        path = os.path.join(
            bucket,
            prefix,
            construct_path(datasource_label=datasource_label, date=date),
            file,
        )

        if self.filesystem.exists(path) and mode == "a":
            with self.filesystem.open(path, mode) as f:
                results_cache = pd.read_csv(self.filesystem.open(path, mode="rb"))
                for col in [
                    x for x in results_cache.columns if x not in results.columns
                ]:
                    results[col] = np.nan

                results[results_cache.columns].to_csv(
                    f, header=False, index=store_index
                )
        else:
            with self.filesystem.open(path, mode) as f:
                results.to_csv(f, header=True, index=store_index)
