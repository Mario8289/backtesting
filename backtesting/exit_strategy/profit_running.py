import operator as op

import numpy as np
from math import ceil

from ..exit_strategy.base import AbstractExitStrategy
from ..order import Order
from ..position import Position


class ProfitRunning(AbstractExitStrategy):
    __slots__ = (
        "name",
        "min_trade_size",
        "cut_ratio",
        "stoploss_limit",
        "takeprofit_limit",
    )

    def __init__(self, min_trade_size, cut_ratio, stoploss_limit, takeprofit_limit):
        self.name: str = "profit_running"
        self.min_trade_size: float = min_trade_size
        self.cut_ratio: float = cut_ratio
        self.stoploss_limit: int = stoploss_limit
        self.takeprofit_limit: int = takeprofit_limit

    @staticmethod
    def calc_average_price(position):
        total_opening_cost = sum(
            [
                x.running_price * x.quantity
                if hasattr(x, "running_price")
                else x.price * x.quantity
                for x in position.open_positions
            ]
        )
        total_opening_quantity = sum([x.quantity for x in position.open_positions])
        return int(round((total_opening_cost / total_opening_quantity)))

    @staticmethod
    def set_running_price(position, tp_price):
        [setattr(x, "running_price", tp_price) for x in position.open_positions]

    def profitloss_price(self, price: float, position: float, price_increment: float):
        if position > 0:
            sl_price = int(price - (price_increment * 1000000) * self.stoploss_limit)
            tp_price = int(price + (price_increment * 1000000) * self.takeprofit_limit)
        else:
            sl_price = int(price + (price_increment * 1000000) * self.stoploss_limit)
            tp_price = int(price - (price_increment * 1000000) * self.takeprofit_limit)
        return sl_price, tp_price

    def calc_order_qty(self, net_position):
        order_qty = ceil(abs(net_position) * self.cut_ratio)
        if order_qty < self.min_trade_size:
            order_qty = min([abs(net_position), self.min_trade_size])
        return order_qty * np.sign(net_position) * -1

    def generate_exit_order_signal(
            self,
            event,
            account,
            tick_price: float = None,
            position: Position = None,
            **kwargs,
    ):
        if position.net_position != 0:
            position.exit_attr["running_price"] = self.calc_average_price(position)
        else:
            return []

        price_sl, price_tp = self.profitloss_price(
            position.exit_attr["running_price"],
            position.net_position,
            position.price_increment,
        )

        ops = [op.ge, op.le] if position.net_position > 0 else [op.le, op.ge]

        orders = []
        if ops[0](tick_price, price_tp):
            orders.append(
                Order(
                    event.timestamp,
                    event.order_book_id,
                    account,
                    self.calc_order_qty(position.net_position),
                    symbol=event.symbol,
                    order_type="R",
                    time_in_force="K",
                    signal="TP_close_position",
                    event_type="hedge",
                )
            )
            self.set_running_price(position, price_tp)

        if ops[1](tick_price, price_sl):
            orders.append(
                Order(
                    event.timestamp,
                    event.order_book_id,
                    account,
                    position.net_position * -1,
                    symbol=event.symbol,
                    order_type="S",
                    time_in_force="K",
                    signal="SL_close_position",
                    event_type="hedge",
                    )
            )

        return orders
