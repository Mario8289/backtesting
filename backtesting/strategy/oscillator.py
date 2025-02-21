from typing import List, Dict, Any, AnyStr

import pandas as pd
import pytz

from ..event import Event
from ..exit_strategy import AbstractExitStrategy
from ..order import Order
from ..strategy.base import AbstractStrategy

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")


class Oscillator(AbstractStrategy):
    __name__ = 'oscillator'
    __output_slots__ = [
        "long_trade_cnt",
        "short_trade_cnt"
    ]
    __input_slots__ = [
        "account_id",
        "opening_qty",
        "trade_qty",
        "long_trade_limit",
        "short_trade_limit",
        "min_position_size",
        "max_position_size",
        "exit_strategy"
    ]
    __slots__ = ["name"] + __input_slots__ + __output_slots__

    def __init__(
            self,
            account_id: int,
            opening_qty: float,
            trade_qty: float,
            long_trade_limit: int,
            short_trade_limit: int,
            min_position_size: float,
            max_position_size: float,
            exit_strategy: AbstractExitStrategy,
    ):
        self.name: str = 'threshold_bouncer'
        self.account_id: int = account_id
        self.opening_qty: float = opening_qty
        self.trade_qty: float = trade_qty
        self.long_trade_limit: int = long_trade_limit
        self.short_trade_limit: int = short_trade_limit
        self.min_position_size: float = min_position_size
        self.max_position_size: float = max_position_size
        self.exit_strategy: AbstractExitStrategy = exit_strategy

        self.last_trade_sign: int = 0
        self.short_trade_cnt: int = 0
        self.long_trade_cnt: int = 0
        self.last_purchase_price: float = None

    @classmethod
    def create(cls, **kwargs):
        attributes = dict()
        for (k, v) in kwargs.items():
            if k in cls.__input_slots__:
                attributes[k] = v
            else:
                pass

        return cls(**attributes)

    def update(self, **kwargs):
        """if you need to update the strategy at the start of each day"""
        pass

    def get_account_migrations(self, **kwargs):
        # TODO: implement this
        return pd.DataFrame()

    def calculate_market_signals(self, portfolio, event: Event) -> List[Order]:
        # Signal Triggers
        orders: List[Order] = []

        price = event.get_price()

        if 0 == len(portfolio.positions) == 0:
            orders = [
                Order(
                    timestamp=event.timestamp,
                    source=event.source,
                    symbol_id=event.symbol_id,
                    account_id=self.account_id,
                    contract_qty=self.opening_qty,
                    order_type="R",
                    time_in_force="K",
                    symbol=event.symbol,
                    signal="Open_Position",
                )
            ]

            self.last_purchase_price = price

        else:
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
                    avg_price=self.last_purchase_price,
                    tick_price=tick_price,
                    position=position,
                    contract_qty=self.trade_qty
            )

            if self.last_trade_sign == 1:
                self.short_trade_cnt = 0
            elif self.last_trade_sign == -1:
                self.long_trade_cnt = 0

            for order in orders:
                if order.is_long:
                    if self.long_trade_cnt >= self.long_trade_limit:
                        order.cancelled = 1
                        order.cancellation_reason = 'long_trade_limit'
                    elif position.net_position+self.trade_qty > self.max_position_size:
                        order.cancelled = 1
                        order.cancellation_reason = 'max_position_size'
                    else:
                        self.long_trade_cnt += 1
                        self.last_trade_sign = 1
                        self.last_purchase_price = price
                else:
                    if self.short_trade_cnt >= self.short_trade_limit:
                        order.cancelled = 1
                        order.cancellation_reason = 'short_trade_limit'
                    elif position.net_position-self.trade_qty < self.min_position_size:
                        order.cancelled = 1
                        order.cancellation_reason = 'min_position_size'
                    else:
                        self.short_trade_cnt += 1
                        self.last_trade_sign = -1
                        self.last_purchase_price = price

        return orders

    def on_state(self, portfolio, event: Event) -> List[Order]:
        if event.event_type == 'market_data':
            orders = self.calculate_market_signals(portfolio, event)
        else:
            orders = []

        return orders

    def get_name(self) -> str:
        return self.name
