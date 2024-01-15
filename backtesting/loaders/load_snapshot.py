from abc import ABC, abstractmethod
from datetime import date

import pandas as pd

from lmax_analytics.dataloader.liquidity_profile_snapshot import (
    get_liquidity_profile_snapshot,
)
from lmax_analytics.dataloader.parquet import Parquet


class SnapshotLoader(ABC):
    def __init__(self, loader: Parquet):
        self.loader: Parquet = loader

    @abstractmethod
    def get_liquidity_profile_snapshot(
            self,
            datasource_label: str,
            start_date: date,
            end_date: date,
            instruments: list = None,
            internalisation_account_id: int = None,
    ) -> pd.DataFrame:
        pass


class SnapshotParquetLoader(SnapshotLoader):
    def get_liquidity_profile_snapshot(
            self,
            datasource_label: str,
            start_date: date,
            end_date: date,
            instruments: list = None,
            internalisation_account_id: int = None,
    ) -> pd.DataFrame:

        snapshot = get_liquidity_profile_snapshot(
            loader=self.loader,
            datasource_label=datasource_label,
            start_date=start_date,
            end_date=end_date,
            account_id=None,
            instruments=instruments,
            internalisation_account_id=internalisation_account_id,
        )

        snapshot = parse_snapshot(snapshot)

        return snapshot


def parse_snapshot(snapshot: pd.DataFrame) -> pd.DataFrame:
    possible_columns = [
        "timestamp",
        "shard",
        "account_id",
        "instrument_id",
        "internalisation_position_limit",
        "internalisation_account_id",
        "internalisation_risk",
        "booking_risk",
    ]

    cols = [x for x in snapshot.columns if x in possible_columns]
    return snapshot[cols]
