import datetime
from typing import List, Dict, Any, AnyStr
from dateutil.relativedelta import relativedelta
import datetime as dt
import pandas as pd
import pytz

from ..event import Event
from ..exit_strategy import AbstractExitStrategy
from ..order import Order
from ..strategy.base import AbstractStrategy

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")


class DCA(AbstractStrategy):
    __name__ = 'dca'
    __input_slots__ = [
        "account_id",
        "contract_qty",
        "day",
        "time",
        "freq",
        "exit_strategy"
    ]
    __slots__ = ["name"] + __input_slots__

    def __init__(
            self,
            account_id: int,
            contract_qty: float,
            time: object,
            day: object,
            freq: object,
            exit_strategy: AbstractExitStrategy,
    ):
        self.name: str = 'dca'
        self.account_id: int = account_id
        self.contract_qty = contract_qty
        self.time: object = time
        self.day: object = day.capitalize()
        self.freq: object = freq
        self.exit_strategy: AbstractExitStrategy = exit_strategy

        self.next_trade_timestamp: dt.datetime = None

    @property
    def weekday(self):
        days_map = {
            "Monday": 0,
            "Tuesday": 1,
            "Wednesday": 2,
            "Thursday": 3,
            "Friday": 4,
            "Saturday": 5,
            "Sunday": 6,
        }
        return days_map[self.day]

    def set_next_trade_timestamp(self):
        if self.freq[-1] == 'm':
            delta = dt.timedelta(minutes=int(self.freq[:-1]))
        elif self.freq[-1] == 'h':
            delta = dt.timedelta(hours=int(self.freq[:-1]))
        elif self.freq[-1] == 'd':
            delta = relativedelta(self.next_trade_timestamp, days=int(self.freq[:-1]))
        elif self.freq[-1] == 'M':
            delta = relativedelta(self.next_trade_timestamp, months=int(self.freq[:-1]))

        next_trade_timestamp = self.next_trade_timestamp + delta
        self.next_trade_timestamp = next_trade_timestamp

    def update(self, **kwargs):
        if self.next_trade_timestamp is None:
            day = kwargs.get("start_date")
            start_date = f"{day} {self.time}"
            today = dt.datetime.strptime(start_date, "%Y-%m-%d %H%M")

            current_weekday = today.weekday()

            next_weekday = (self.weekday - current_weekday + 7) % 7

            self.next_trade_timestamp = today + dt.timedelta(days=next_weekday)

    @classmethod
    def create(cls, **kwargs):
        attributes = dict()
        for (k, v) in kwargs.items():
            if k in cls.__input_slots__:
                attributes[k] = v
            else:
                pass

        return cls(**attributes)

    def get_account_migrations(self, **kwargs):
        # TODO: implement this
        return pd.DataFrame()

    def calculate_market_signals(self, portfolio, event: Event) -> List[Order]:
        # Signal Triggers
        orders: List[Order] = []
        timestamp = event.get_timestamp()

        if timestamp >= self.next_trade_timestamp:
            orders = [
                Order(
                    timestamp=event.timestamp,
                    source=event.source,
                    symbol_id=event.symbol_id,
                    account_id=self.account_id,
                    contract_qty=self.contract_qty,
                    order_type="R",
                    time_in_force="K",
                    symbol=event.symbol,
                    signal="DCA",
                )
            ]

            self.set_next_trade_timestamp()
        else:

            if len(portfolio.positions) != 0:
                position = portfolio.positions.get(
                    (event.source, event.symbol_id, self.account_id)
                )

                avg_price = position.get_price()

                tick_price = event.get_price(
                    is_long=position.is_long(),
                    matching_method=portfolio.matching_method
                )

                orders = self.exit_strategy.generate_exit_order_signal(
                        event=event,
                        account=self.account_id,
                        avg_price=avg_price,
                        tick_price=tick_price,
                        position=position,
                        contract_qty=self.contract_qty
                )
            else:
                pass

        return orders

    def on_state(self, portfolio, event: Event) -> List[Order]:
        if event.event_type == 'market_data':
            orders = self.calculate_market_signals(portfolio, event)
        else:
            orders = []

        return orders

    def get_name(self) -> str:
        return self.name
