import operator

from ..exit_strategy.base import AbstractExitStrategy
from ..order import Order
from ..position import Position


class TrailingStopLoss(AbstractExitStrategy):
    __slots__ = ("name", "stoploss_limit")

    def __init__(self, stoploss_limit):
        super().__init__()
        self.name: str = "trailing_stoploss"
        self.stoploss_limit: int = stoploss_limit

    @staticmethod
    def set_tick_peak(position, tick_price, op):
        if hasattr(position, "tick_peak"):
            if op(tick_price, position.tick_peak):
                position.tick_peak = tick_price  # move peak
        else:
            position.tick_peak = position.last_tick_peak

    def profitloss_price(self, price: float, position: float, price_increment: float):

        if position > 0:
            return int(price - ((price_increment * 1000000) * self.stoploss_limit))
        else:
            return int(price + ((price_increment * 1000000) * self.stoploss_limit))

    def set_trailing_stoploss(self, position, avg_price):
        price_sl = self.profitloss_price(
            avg_price, position.net_position, position.price_increment
        )

        position.exit_attr["trailing_stoploss"] = price_sl

    def generate_exit_order_signal(
            self,
            event,
            account,
            avg_price: float = None,
            tick_price: float = None,
            position: Position = None,
    ):

        orders = []

        # set operator based on direction of position
        op = operator.gt if position.net_position > 0 else operator.lt

        last_tick_price = (
            position.exit_attr["last_tick_price"]
            if position.exit_attr.get("last_tick_price")
            else avg_price
        )

        # price moves into profit
        if op(tick_price, last_tick_price):
            if position.exit_attr.get("tick_peak"):
                # set new peak if it exceeds previous highs
                if op(tick_price, position.exit_attr["tick_peak"]):
                    position.exit_attr["tick_peak"] = tick_price
                    self.set_trailing_stoploss(position, tick_price)

            else:
                position.exit_attr["tick_peak"] = tick_price
                self.set_trailing_stoploss(position, tick_price)

        # price moves into loss
        else:
            if not position.exit_attr.get("tick_peak"):
                position.exit_attr["tick_peak"] = last_tick_price
                self.set_trailing_stoploss(position, last_tick_price)

            # set operator based on direction of position
            op = operator.ge if position.net_position > 0 else operator.le

            if op(position.exit_attr["trailing_stoploss"], tick_price):
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

        position.exit_attr["last_tick_price"] = tick_price

        return orders
