import datetime as dt
from typing import List

import numpy as np
import logging
import pandas as pd
import pytz
from math import floor

from ..event import Event
from ..exit_strategy.exit_default import ExitDefault
from ..loaders.load_tob import load_tob
from ..loaders.dataserver import DataServerLoader
from ..order import Order
from ..strategy.base import AbstractStrategy

eastern = pytz.timezone("US/Eastern")


class BbookingStrategy(AbstractStrategy):
    __slots__ = ["name", "exit_strategy"]

    def __init__(self, exit_strategy=ExitDefault()):
        self.name = "bbooking"
        self.exit_strategy = exit_strategy
        self.account_booking_risk: dict = dict()

    def filter_snapshot(
            self, snapshot: pd.DataFrame, relative_type: str, relative_accounts: List[int]
    ) -> pd.DataFrame:
        if "booking_risk" in snapshot.columns:
            return snapshot[snapshot.booking_risk > 0]
        return snapshot

    @staticmethod
    def position_balanced(
            contract_qty: int, lmax_net_position, client_net_position, booking_risk: int
    ) -> bool:
        return (
                lmax_net_position * -1
                == (client_net_position - contract_qty) * booking_risk
        )

    @staticmethod
    def booking_risk_increased(
            contract_qty: int, lmax_net_position, client_net_position, booking_risk: int
    ) -> bool:
        projected_net_position_at_current_boooking_risk = (
                                                                  (client_net_position - contract_qty) * booking_risk
                                                          ) * -1
        rebalance_trade_size = (
                projected_net_position_at_current_boooking_risk - lmax_net_position
        )
        return rebalance_trade_size

    def get_trade_size(
            self,
            contract_qty: int,
            lmax_net_position,
            client_net_position,
            booking_risk: int,
    ) -> int:

        if self.position_balanced(
                contract_qty, lmax_net_position, client_net_position, booking_risk
        ):
            trade_size = floor(contract_qty * booking_risk) * -1

        elif lmax_net_position == 0:
            trade_size = floor((client_net_position + contract_qty) * booking_risk) * -1

        else:  # need to rebalance the portfolio

            rebalanced_trade_size = self.booking_risk_increased(
                contract_qty, lmax_net_position, client_net_position, booking_risk
            )
            new_trade_size = floor(contract_qty * booking_risk) * -1
            trade_size = new_trade_size + rebalanced_trade_size

        return trade_size

    def get_migrations_for_day(self, day, target_accounts):
        start = dt.datetime.combine(day, dt.time())
        end = dt.datetime.combine(day, dt.time()) + dt.timedelta(days=1)
        start_migration_timestamp = int(
            (pytz.UTC.localize(start).astimezone(eastern).replace(hour=17))
            .astimezone(pytz.UTC)
            .timestamp()
            * 1000
        )
        end_migration_timestamp = int(
            (pytz.UTC.localize(end).astimezone(eastern).replace(hour=17, minute=5))
            .astimezone(pytz.UTC)
            .timestamp()
            * 1000
        )

        migrations = target_accounts[
            (target_accounts["timestamp"] >= start_migration_timestamp)
            & (target_accounts["timestamp"] < end_migration_timestamp)
            ]
        return migrations

    def get_account_migrations(
            self,
            day: dt.date,
            target_accounts: pd.DataFrame,
            shard: str,
            instruments: List[int],
            tob_loader,
            dataserver: DataServerLoader,
            load_booking_risk_from_snapshot: bool = False,
            load_booking_risk_from_target_accounts: bool = False,
            load_internalisation_risk_from_snapshot: bool = False,
            load_internalisation_risk_from_target_accounts: bool = False,
            snapshot: pd.DataFrame = pd.DataFrame(),
            order_book_details: pd.DataFrame = pd.DataFrame(),
    ) -> pd.DataFrame:
        logger: logging.Logger = logging.getLogger("bbook: get_account_migrations")

        migrations = pd.DataFrame()

        if load_booking_risk_from_snapshot:
            snapshot = snapshot[
                (snapshot.account_id.isin(target_accounts.account_id.tolist()))
                & (snapshot.instrument_id.isin(instruments))
                ][
                ["timestamp", "instrument_id", "account_id", "booking_risk"]
            ].drop_duplicates()

            snapshot["booking_risk"] = snapshot["booking_risk"] / 100

            current_booking_risk = (
                snapshot.reset_index().groupby(["account_id", "instrument_id"]).first()
            ).reset_index()

            self.account_booking_risk = (
                current_booking_risk[["account_id", "booking_risk"]]
                .drop_duplicates()
                .set_index("account_id")["booking_risk"]
                .to_dict()
            )
            migrations = (
                snapshot[
                    ~snapshot.index.get_level_values(0).isin(
                        current_booking_risk["index"]
                    )
                ]
                .rename(columns={"instrument_id": "order_book_id"})
                .copy()
            )
        if load_booking_risk_from_target_accounts:
            if "timestamp" in target_accounts.columns:
                migrations = self.get_migrations_for_day(day, target_accounts).rename(
                    columns={"instrument_id": "order_book_id"}
                )
            else:
                target_accounts_list = target_accounts["account_id"].to_list()
                accounts_without_risk_assigned = [
                    x
                    for x in target_accounts_list
                    if x not in self.account_booking_risk.keys()
                ]

                if accounts_without_risk_assigned:
                    self.account_booking_risk.update(
                        target_accounts[
                            target_accounts["account_id"].isin(
                                accounts_without_risk_assigned
                            )
                        ][["account_id", "booking_risk"]]
                        .drop_duplicates()
                        .set_index("account_id")["booking_risk"]
                        .to_dict()
                    )

        if not migrations.empty:
            migrations["timestamp_micros"] = migrations["timestamp"] * 1000
            migrations["timestamp"] = pd.to_datetime(
                migrations["timestamp"], unit="ms", utc=True
            )
            migrations["trading_session"] = (
                    pd.to_datetime(migrations["timestamp"], unit="ms")
                    + dt.timedelta(hours=7)
            ).dt.strftime("%Y-%m-%d")
            migrations["event_type"] = "account_migration"
            migrations = migrations.set_index("timestamp").sort_index()
            minute_datetimes_to_load = migrations.index.floor(freq="1T").to_pydatetime()

            start_timestamp = migrations.index.min().to_pydatetime()
            end_timestamp = migrations.index.max().to_pydatetime()

            rates_for_day = dataserver.get_usd_rates_for_instruments(day, instruments)

            tob = load_tob(
                tob_loader,
                datasource_label=shard,
                instrument=instruments,
                start_date=start_timestamp,
                end_date=end_timestamp,
                tier=[1],
                period="minute",
                datetimes=minute_datetimes_to_load,
            ).sort_values("timestamp_micros")

            next_timestamp = end_timestamp
            while tob.shape[0] == 0:

                next_timestamp = next_timestamp + dt.timedelta(minutes=1)
                tob = load_tob(
                    tob_loader,
                    datasource_label=shard,
                    instrument=instruments,
                    start_date=next_timestamp,
                    end_date=next_timestamp,
                    tier=[1],
                    period="minute",
                ).sort_values("timestamp_micros")
                logger.info(
                    f"instruments {', '.join(map(str, instruments))}, load tob for timestamp {next_timestamp}, {len(tob)} tob entries found"
                )
                if next_timestamp > end_timestamp + dt.timedelta(hours=3):

                    message = (
                        f"no TOB prices found on between {start_timestamp}-{end_timestamp + dt.timedelta(hours=3)}, "
                        f"meaning all migrations will be ignored for date {day} for instruments {','.join(map(str, instruments))}"
                    )
                    # logger.error(message)
                    logger.warning(message)
                    return pd.DataFrame()

            tob_with_rates = tob.merge(
                rates_for_day.rename(columns={"rate": "rate_to_usd"}),
                left_on="order_book_id",
                right_on="instrument_id",
            )

            migrations["order_book_id"] = migrations["order_book_id"].astype(int)
            migrations["timestamp_micros"] = migrations["timestamp_micros"].astype(int)
            migrations["account_id"] = migrations["account_id"].astype(int)
            migrations["booking_risk"] = migrations["booking_risk"].astype(float)

            migrations = pd.merge_asof(
                migrations.reset_index(),
                tob_with_rates,
                direction="nearest",
                on="timestamp_micros",
                by=["order_book_id"],
            ).set_index("timestamp")

        else:
            # if their are no migrations for this date but the target_account_list includes accounts that
            # haven't traded then add them with a default booking risk of 1
            target_accounts_list = target_accounts["account_id"].to_list()
            accounts_without_risk_assigned = [
                x
                for x in target_accounts_list
                if x not in self.account_booking_risk.keys()
            ]

            if accounts_without_risk_assigned:
                self.account_booking_risk.update(
                    {
                        accounts_without_risk_assigned[i]: 1
                        for i in range(len(accounts_without_risk_assigned))
                    }
                )

        if not order_book_details.empty and not migrations.empty:
            columns = [
                x
                for x in order_book_details.columns
                if x not in migrations.columns or x == "order_book_id"
            ]
            migrations = (
                migrations.reset_index()
                .merge(order_book_details[columns], on="order_book_id", how="left")
                .set_index("timestamp")
            )

        return migrations

    def get_trade_price(self, ask_price: int, bid_price: int, trade_size: int):
        direction = np.sign(trade_size)
        if direction == 1:
            price = ask_price
        else:
            price = bid_price
        return price

    def calculate_market_signals(self, event: Event) -> List[Order]:
        # Signal Triggers
        return self.exit_strategy.generate_exit_order_signal(event=event, account=None)

    def calculate_migration_signals(
            self, client_portfolio, lmax_portfolio, event: Event
    ) -> List[Order]:
        # Signal Triggers
        orders: List[Order] = []

        # set account booking risk for future trades
        booking_risk = event.booking_risk
        self.account_booking_risk[event.account_id] = booking_risk

        client_positions = client_portfolio.get_positions_for_account(event.account_id)

        if client_positions:
            for key, position in client_positions.items():
                client_net_position = position.net_position
                # if account has a position then we need to trade to rebalance position using new booking risk
                if client_net_position:
                    lmax_net_position = self.get_net_position(lmax_portfolio, *key)

                    trade_size = self.get_trade_size(
                        event.contract_qty,
                        lmax_net_position,
                        client_net_position,
                        booking_risk,
                    )
                    trade_price = self.get_trade_price(
                        event.ask_price, event.bid_price, trade_size
                    )
                    if trade_size != 0:
                        orders.append(
                            Order(
                                event.timestamp,
                                key[1],
                                event.account_id,
                                trade_size,
                                order_type="M",
                                time_in_force="K",
                                price=trade_price,
                                signal="account_migration",
                                event_type="internal",
                            )
                        )

        return orders

    def calculate_trade_signals(
            self, client_portfolio, lmax_portfolio, event: Event
    ) -> List[Order]:
        # Signal Triggers
        orders: List[Order] = []
        booking_risk = self.account_booking_risk.get(event.account_id, 0)

        lmax_net_position = self.get_net_position(
            lmax_portfolio, event.venue, event.order_book_id, event.account_id
        )
        client_net_position = self.get_net_position(
            client_portfolio, event.venue, event.order_book_id, event.account_id
        )

        trade_size = self.get_trade_size(
            event.contract_qty, lmax_net_position, client_net_position, booking_risk
        )

        # print(f'TRADE {event.timestamp}, account {event.account_id}, lmax_net_position: {lmax_net_position}, client_net_position: {client_net_position},, trade_size: {trade_size}, contract_qty: {event.contract_qty}, booking risk {booking_risk}')

        if trade_size != 0:
            orders.append(
                Order(
                    event.timestamp,
                    event.order_book_id,
                    event.account_id,
                    trade_size,
                    order_type="N",
                    time_in_force="K",
                    price=event.price,
                    symbol=event.symbol,
                    signal="client_trade",
                    event_type="internal",
                )
            )
        return orders

    def on_state(self, client_portfolio, lmax_portfolio, event: Event) -> List[Order]:
        switcher = {
            "trade": self.calculate_trade_signals,
            "market_data": self.calculate_market_signals,
            "account_migration": self.calculate_migration_signals,
        }
        func = switcher.get(event.event_type)
        # todo: python doesn't do class method switching very nicely (or at least IntelliJ doesn't like it)
        #       maybe clean it up a bit
        return func(client_portfolio, lmax_portfolio, event)

    @staticmethod
    def get_net_position(portfolio, venue, order_book_id, account_id):
        position = portfolio.positions.get((venue, order_book_id, account_id))

        if position:
            net_position = position.net_position
        else:
            net_position = 0

        return net_position

    def get_name(self) -> str:
        return self.name
