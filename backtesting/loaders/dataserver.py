import pandas as pd

from lmax_analytics.dataloader.data_server import DataServer
from typing import Dict, List, Any
from getpass import getpass
import datetime as dt

import pytz

eastern = pytz.timezone("US/Eastern")


class DataServerLoader(DataServer):
    def __init__(
            self,
            token: str = None,
            username: str = None,
            password: str = None,
            data_server_api_url: str = "https://analytics.ix3.lmax/lmax-data-server/api",
    ):
        super().__init__(
            token=token,
            username=username,
            password=password,
            data_server_api_url=data_server_api_url,
        )

    @classmethod
    def initialise(cls, auth: Dict[str, Dict[str, Any]], section: str) -> DataServer:
        sect = auth[section]
        keys = sect.keys()
        if "token" in keys:
            return cls(token=sect["token"], data_server_api_url=sect["uri"])
        else:
            username = sect["username"]
            password = (
                sect["password"]
                if "password" in keys
                else getpass("enter dataserver password")
            )
            return cls(
                username=username, password=password, data_server_api_url=sect["uri"],
            )

    def get_usd_rates_for_instruments(
            self, date: dt.date, instruments: List[int]
    ) -> pd.DataFrame:
        rate_date = date.strftime("%Y-%m-%d")
        instruments = ", ".join(map(str, instruments))

        return self.load_data(
            query_api_path="/risk_backtesting/fetch/rates",
            column_names=["instrument_id", "currency", "rate", "class"],
            params={"rate_date": rate_date, "instrument_ids": instruments},
        )

    def get_usd_rate_for_instruments_unit_of_measure(
            self, shard: str, date: dt.date, instruments: List[int]
    ) -> pd.DataFrame:
        rate_date = date.strftime("%Y-%m-%d")

        return self.load_data(
            query_api_path="/risk_backtesting/fetch/instrument_unit_of_measure_rates",
            column_names=[
                "instrument_id",
                "unit_price",
                "currency",
                "class",
                "contract_unit_of_measure",
                "rate",
            ],
            params={
                "shard": shard,
                "rate_date": rate_date,
                "instrument_ids": instruments,
            },
        )

    def parse_upnl_reversal(self, df):
        df["event_type"] = "closing_price"
        df["timestamp"] = (
            pd.to_datetime(df["trade_date"], utc=False)
            .dt.tz_localize(eastern)
            .map(
                lambda x: x.replace(hour=17).astimezone(pytz.utc)
                          - dt.timedelta(milliseconds=1)
            )
        )
        df["timestamp_micros"] = df["timestamp"].astype(int) / 1e4
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.set_index("timestamp")
        df["upnl_reversal"] = df["upnl_reversal"] * -1
        return df

    def get_upnl_reversal(
            self, datasource_label, start_date, end_date, instruments, accounts
    ):

        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        upnl_reversal = pd.DataFrame()

        max_accounts = min(len(accounts), 100)
        i1 = 0

        for i2 in range(0, len(accounts), max_accounts):
            i2 = min(len(accounts), i2 + max_accounts)

            accounts_grp = accounts[i1:i2]
            upnl_reversal_grp = self.load_data(
                query_api_path="/risk_backtesting/fetch/upnl_reversal",
                column_names=[
                    "datasource",
                    "shard",
                    "trade_date",
                    "account_id",
                    "instrument_id",
                    "symbol",
                    "contract_unit_of_measure",
                    "upnl_reversal",
                ],
                params={
                    "shard": datasource_label,
                    "start_date": start_date,
                    "end_date": end_date,
                    "account_ids": accounts_grp,
                    "instrument_ids": instruments,
                },
            )
            upnl_reversal = pd.concat([upnl_reversal, upnl_reversal_grp])
            i1 = i2

        upnl_reversal = self.parse_upnl_reversal(upnl_reversal)

        return upnl_reversal

    def get_opening_positions(
            self, datasource_label, start_date, end_date, instruments, accounts
    ):
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        opening_positions = pd.DataFrame()

        max_accounts = min(len(accounts), 100)
        i1 = 0

        for i2 in range(0, len(accounts), max_accounts):
            i2 = min(len(accounts), i2 + max_accounts)
            accounts_grp = accounts[i1:i2]

            opening_positions_grp = self.load_data(
                query_api_path="/risk_backtesting/fetch/opening_positions",
                column_names=[
                    "datasource",
                    "shard",
                    "instrument_id",
                    "symbol",
                    "unit_price",
                    "price_increment",
                    "contract_unit_of_measure",
                    "currency",
                    "snapshot_date",
                    "next_trading_day",
                    "account_id",
                    "position",
                    "open_cost",
                    "cumulative_cost",
                    "realised_profit",
                ],
                params={
                    "shard": datasource_label,
                    "start_date": start_date,
                    "end_date": end_date,
                    "account_ids": accounts_grp,
                    "instrument_ids": instruments,
                },
            )
            opening_positions = pd.concat([opening_positions, opening_positions_grp])
            i1 = i2

        return opening_positions

    def get_order_book_details(
            self, shard: str, instruments: List[int]
    ) -> pd.DataFrame:

        ob = self.load_data(
            query_api_path="/risk_backtesting/fetch/order_book_details",
            column_names=["order_book_id", "contract_unit_of_measure"],
            params={"shard": shard, "instrument_ids": instruments},
        )

        return ob

    @staticmethod
    def parse_closing_prices(closing_prices):
        closing_prices["event_type"] = "closing_price"
        closing_prices["bid_price"] = closing_prices["price"]
        closing_prices["ask_price"] = closing_prices["price"]
        closing_prices["timestamp"] = (
            pd.to_datetime(closing_prices["trading_session"], utc=False)
            .dt.tz_localize(eastern)
            .map(
                lambda x: x.replace(hour=17).astimezone(pytz.utc)
                          - dt.timedelta(milliseconds=1)
            )
        )
        closing_prices["timestamp_micros"] = (
                closing_prices["timestamp"].astype(int) / 1e4
        )
        closing_prices = closing_prices.set_index("timestamp")
        return closing_prices

    def get_closing_prices(
            self, shard: str, instruments: List[int], start_date, end_date
    ) -> pd.DataFrame:
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        instruments = ", ".join(map(str, instruments))
        closing_prices = self.load_data(
            query_api_path="/risk_backtesting/fetch/closing_prices",
            column_names=[
                "datasource",
                "trading_session",
                "price",
                "order_book_id",
                "symbol",
                "unit_price",
                "contract_unit_of_measure",
                "currency",
                "rate_to_usd",
            ],
            params={
                "shard": shard,
                "instrument_ids": instruments,
                "start_date": start_date,
                "end_date": end_date,
            },
        )

        closing_prices = self.parse_closing_prices(closing_prices)

        return closing_prices


if __name__ == "__main__":
    pass
