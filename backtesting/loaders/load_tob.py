import datetime as dt
import os
from datetime import datetime, date
from typing import List

import pandas as pd
import pytz

from lmax_analytics.dataloader.tob import get_tob

eastern = pytz.timezone("US/Eastern")


class TobLoader:
    def __init__(self, loader):
        self.loader = loader

    def get_tob(
            self,
            datasource_label: str,
            order_book: List,
            start_date: date,
            end_date: date,
            tier: List,
    ):
        pass


class TobParquetLoader(TobLoader):
    def get_tob(
            self,
            datasource_label: str,
            order_book: List,
            start_date: date,
            end_date: date,
            tier: List,
    ) -> pd.DataFrame:
        tob: pd.DataFrame = get_tob(
            self.loader,
            datasource_label=datasource_label,
            order_book=order_book,
            start_date=start_date,
            end_date=end_date,
            tier=tier,
        )
        return tob

    def get_tob_minute(
            self,
            datasource_label: str,
            order_book: List,
            start_date: datetime,
            end_date: datetime,
            tier: List,
            datetimes: List[dt.datetime],
    ) -> pd.DataFrame:
        if datetimes is not None:
            paths = [
                os.path.join(
                    "parquet",
                    self.loader.construct_file_path(
                        datasource_label=datasource_label,
                        filename=f"{datasource_label}-tiered-top-of-book-{day.strftime('%Y')}-{day.strftime('%m')}-{day.strftime('%d')}-{day.strftime('%H')}-{day.strftime('%M')}.parquet",
                        date=day,
                    ),
                )
                for day in datetimes
            ]
        else:
            datetimes = list(pd.date_range(start_date, end_date, freq="1T"))
            paths = [
                os.path.join(
                    "parquet",
                    self.loader.construct_file_path(
                        datasource_label=datasource_label,
                        filename=f"{datasource_label}-tiered-top-of-book-{day.strftime('%Y')}-{day.strftime('%m')}-{day.strftime('%d')}-{day.strftime('%H')}-{day.strftime('%M')}.parquet",
                        date=day,
                    ),
                )
                for day in datetimes
            ]
        tob = self.loader.load_endpoints("top-of-book", paths, schema=None)

        if order_book:
            tob = tob[tob.order_book_id.isin(order_book)]
        if tier:
            tob = tob[tob.tier.isin(tier)]

        # load order book details
        order_book_columns = [
            "order_book_id",
            "price_increment",
            "unit_price",
            "currency",
            "symbol",
        ]
        order_book_metadata = pd.read_parquet(
            "broker/metadata/order_books.parquet", filesystem=self.loader.filesystem
        )

        tob = tob.query(
            "bid_price != 0 & ask_price != 0 & bid_quantity != 0 & ask_quantity != 0"
        )

        tob_with_order_books = tob.merge(
            order_book_metadata[order_book_columns], on="order_book_id", how="left"
        )

        return tob_with_order_books


def parse_tob(unparsed_tob: pd.DataFrame):
    tob: pd.DataFrame = unparsed_tob.copy()
    tob["event_type"] = "market_data"
    parsed_tob = (
        tob[~((tob.ask_price == 0) | (tob.bid_price == 0))]
        .rename(columns={"ask_quantity": "ask_qty", "bid_quantity": "bid_qty"})
        .copy()
    )
    parsed_tob["trading_session"] = (
            parsed_tob.index.tz_convert(eastern) + dt.timedelta(hours=7)
    ).date
    return parsed_tob


def load_tob(
        loader: TobLoader,
        datasource_label: str,
        instrument: List,
        start_date: datetime,
        end_date: datetime,
        tier: List,
        period: str = "day",
        datetimes: List[dt.datetime] = None,
) -> pd.DataFrame:

    # noinspection PyTypeChecker
    if period == "day":
        tob: pd.DataFrame = loader.get_tob(
            datasource_label=datasource_label,
            order_book=instrument,
            start_date=start_date,
            end_date=end_date,
            tier=tier,
        )
        tob = parse_tob(tob)
    elif period == "minute":
        tob: pd.DataFrame = loader.get_tob_minute(
            datasource_label=datasource_label,
            order_book=instrument,
            start_date=start_date,
            end_date=end_date,
            tier=tier,
            datetimes=datetimes,
        )

    return tob
