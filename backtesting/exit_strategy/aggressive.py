import operator as op

from ..event import Event
from ..exit_strategy.base import AbstractExitStrategy
from ..order import Order
from ..position import Position


class Aggressive(AbstractExitStrategy):
    __slots__ = ("name", "stoploss_limit", "takeprofit_limit")

    def __init__(self, stoploss_limit, takeprofit_limit):
        self.name: str = "aggressive"
        self.stoploss_limit: int = stoploss_limit
        self.takeprofit_limit: int = takeprofit_limit

    def profitloss_price(self, price: float, position: float, price_increment: float):

        ops = [op.add, op.sub] if position > 0 else [op.sub, op.add]

        # Set TakeProfit limit
        tp_price = int(
            ops[0](price, ((price_increment * 1000000) * self.takeprofit_limit))
        )

        # Set StopLoss limit
        sl_price = int(
            ops[1](price, ((price_increment * 1000000) * self.stoploss_limit))
        )

        return sl_price, tp_price

    def generate_exit_order_signal(
            self,
            event: Event,
            account: int,
            avg_price: float = None,
            tick_price: float = None,
            position: Position = None,
            **kwargs,
    ):

        price_sl, price_tp = self.profitloss_price(
            avg_price, position.net_position, position.price_increment
        )

        ops = [op.ge, op.le] if position.net_position > 0 else [op.le, op.ge]

        orders = []

        if ops[0](tick_price, price_tp):
            orders.append(
                Order(
                    event.timestamp,
                    event.order_book_id,
                    account,
                    position.net_position * -1,
                    order_type="R",
                    time_in_force="K",
                    symbol=event.symbol,
                    signal="TP_close_position",
                    event_type="hedge",
                    )
            )

        if ops[1](tick_price, price_sl):
            orders.append(
                Order(
                    event.timestamp,
                    event.order_book_id,
                    account,
                    position.net_position * -1,
                    order_type="S",
                    time_in_force="K",
                    symbol=event.symbol,
                    signal="SL_close_position",
                    event_type="hedge",
                    )
            )

        return orders
