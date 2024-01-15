import datetime as dt
from typing import List

import pandas as pd

from lmax_analytics.dataloader.parquet import Parquet
from lmax_analytics.dataloader.trades import get_broker_trades


class TradesLoader:
    def __init__(self, loader):
        self.loader = loader

    def get_broker_trades(
            self,
            datasource_label: str,
            start_date: dt.date,
            end_date: dt.date,
            account: List,
            instrument: List,
    ):
        pass


class TradesParquetLoader(TradesLoader):
    def __init__(self, loader: Parquet):
        super().__init__(loader)

    def get_broker_trades(
            self,
            datasource_label: str,
            start_date: dt.date,
            end_date: dt.date,
            account: List,
            instrument: List,
    ):
        trades = get_broker_trades(
            self.loader,
            datasource_label=datasource_label,
            start_date=start_date,
            end_date=end_date,
            account=account,
            instrument=instrument,
        )

        return trades


def load_trades(
        loader: TradesLoader,
        datasource_label: str,
        instrument: List,
        account: List,
        start_date: dt.date,
        end_date: dt.date,
        order_book_details: pd.DataFrame = pd.DataFrame,
        batch_size=31,
) -> pd.DataFrame:

    broker_trades = pd.DataFrame()

    for day in pd.date_range(
            start_date - dt.timedelta(days=1), end_date, freq=f"{batch_size}D"
    ):
        batch_end_date = min(end_date, (day + dt.timedelta(days=batch_size - 1)).date())
        broker_trades_batch = loader.get_broker_trades(
            datasource_label=datasource_label,
            start_date=day.date(),
            end_date=batch_end_date,
            account=account,
            instrument=instrument,
        )

        broker_trades = pd.concat([broker_trades, broker_trades_batch])

    if not order_book_details.empty:
        broker_trades = (
            broker_trades.reset_index()
            .merge(order_book_details, how="left")
            .set_index("timestamp")
        )

    broker_trades = parse_broker_trades(broker_trades, datasource_label)
    return broker_trades


def parse_broker_trades(broker_trades, datasource_label):
    broker_trades["shard"] = datasource_label
    broker_trades["event_type"] = "trade"
    broker_trades["timestamp_micros"] = broker_trades["time_stamp"] * 1000
    return broker_trades
