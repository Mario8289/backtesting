import datetime as dt
from abc import ABC, abstractmethod
from typing import List

import pandas as pd
import pytz

from risk_backtesting.event import Event

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")
eastern_tz = pytz.timezone("US/Eastern")


class EventStream(ABC):
    def __init__(
            self,
            sample_rate: str,
            name: str = "event_stream",
            include_eod_snapshot: bool = False,
            excl_period: List[List[int]] = None,
    ):
        self.sample_rate: str = sample_rate
        self.name: str = name
        self.include_eod_snapshot: bool = include_eod_snapshot
        self.excl_period: List[List[int]] = excl_period

    def generate_events(
            self,
            date: dt.date,
            trades: pd.DataFrame,
            tob: pd.DataFrame = pd.DataFrame(),
            closing_prices: pd.DataFrame = pd.DataFrame(),
            account_migrations: pd.DataFrame = pd.DataFrame(),
    ) -> pd.DataFrame:
        event = Event()
        events = pd.DataFrame(columns=list(event.__dict__.keys()))

        date_time = eastern_tz.localize(
            dt.datetime.combine(date, dt.datetime.min.time())
        )

        if not tob.empty:

            tob = self.set_untrusted_tag(tob, date_time)

            _events = pd.concat([tob, trades], sort=True, copy=True).sort_index().copy()

            _events = self.fill_time_tags(_events)
            _events = self.fill_order_book_tags(_events)
            _events = self.fill_tob_tags(_events)
            _events = self.set_lifespan_exit_tags(_events, date_time)

        else:
            _events = trades

        if not account_migrations.empty:
            _events = (
                pd.concat([_events, account_migrations], sort=True).sort_index().copy()
            )

        if not closing_prices.empty and self.include_eod_snapshot:
            _events = (
                pd.concat([_events, closing_prices], sort=True).sort_index().copy()
            )

        _events.fillna(0, inplace=True)

        _events = self.standardise_events(_events)

        _events.index.name = "timestamp"
        _events.sort_values(["timestamp", "execution_id"], inplace=True)

        events = events.append(
            _events[[x for x in _events.columns if x in events.columns]], sort=True
        )
        return events.fillna(0)

    @abstractmethod
    def sample(self, tob: pd.DataFrame, trading_session: dt.datetime) -> pd.DataFrame:
        raise NotImplementedError("Should implement generate_events")

    def set_untrusted_tag(self, df: pd.DataFrame, date: dt.datetime) -> pd.DataFrame:
        df["untrusted"] = 0

        if self.excl_period:
            exclusion_t1 = (
                (date - dt.timedelta(days=1))
                .replace(hour=self.excl_period[0][0], minute=self.excl_period[0][1])
                .astimezone(utc_tz)
            )
            exclusion_t2 = (
                (date - dt.timedelta(days=1))
                .replace(hour=self.excl_period[1][0], minute=self.excl_period[1][1])
                .astimezone(utc_tz)
            )
            exclusion_t3 = date.replace(
                hour=self.excl_period[0][0], minute=self.excl_period[0][1]
            ).astimezone(utc_tz)

            df.loc[
                (
                        ((df.index >= exclusion_t1) & (df.index <= exclusion_t2))
                        | (df.index >= exclusion_t3)
                ),
                "untrusted",
            ] = 1

        return df

    @staticmethod
    def fill_order_book_tags(df: pd.DataFrame) -> pd.DataFrame:
        potential_fill_columns = [
            "symbol",
            "shard",
            "price_increment",
            "unit_price",
            "rate_to_usd",
            "trade_date",
            "currency",
            "contract_unit_of_measure",
        ]

        for field in [x for x in potential_fill_columns if x in df.columns]:
            df[field] = df.groupby("order_book_id")[field].transform(
                lambda x: pd.Series(x.bfill())
            )
            df[field] = df.groupby("order_book_id")[field].transform(
                lambda x: pd.Series(x.ffill())
            )
        return df

    @staticmethod
    def fill_tob_tags(df: pd.DataFrame) -> pd.DataFrame:
        for field in ["bid_price", "bid_qty", "ask_price", "ask_qty"]:
            df[field] = df.groupby("order_book_id")[field].transform(
                lambda x: pd.Series(x.ffill())
            )
        return df

    @staticmethod
    def fill_time_tags(df: pd.DataFrame) -> pd.DataFrame:
        for col in ["trade_date", "trading_session"]:
            df[col] = df[col].bfill()
            df[col] = df[col].ffill()
        return df

    @staticmethod
    def set_lifespan_exit_tags(df: pd.DataFrame, date: dt.datetime) -> pd.DataFrame:
        lifespan_exit_start_time = date.replace(hour=17).astimezone(utc_tz)
        df["gfd"] = 0
        df["gfw"] = 0
        df.loc[lifespan_exit_start_time - dt.timedelta(minutes=5) :, "gfd"] = 1
        if date.weekday() == 4:
            df.loc[lifespan_exit_start_time - dt.timedelta(minutes=5) :, "gfw"] = 1
        return df

    @staticmethod
    def standardise_events(df: pd.DataFrame) -> pd.DataFrame:
        standardise_factors = [
            {
                "columns": [
                    "ask_price",
                    "bid_price",
                    "price",
                    "tob_snapshot_ask_price",
                    "tob_snapshot_bid_price",
                ],
                "factor": 1000000,
            },
            {"columns": ["order_qty", "contract_qty"], "factor": 100},
        ]
        for col_factors in standardise_factors:
            for col in col_factors["columns"]:
                if col in df.columns:
                    # noinspection PyUnresolvedReferences
                    df.loc[:, col] = round((df[col] * col_factors["factor"])).astype(
                        int
                    )

        return df
