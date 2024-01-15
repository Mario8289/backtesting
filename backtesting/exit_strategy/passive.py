import operator as op

from ..exit_strategy.base import AbstractExitStrategy
from ..order import Order
from ..position import Position


class Passive(AbstractExitStrategy):
    __slots__ = ("name", "skew_at", "skew_by", "passive_limit")

    def __init__(self, skew_at="same_side", skew_by=1, passive_limit=None):
        super().__init__()
        self.name: str = "passive"
        self.skew_at: str = skew_at
        self.skew_by: str = skew_by
        self.passive_limit = passive_limit

    def generate_exit_order_signal(
            self,
            event,
            account,
            avg_price: float = None,
            tick_price: float = None,
            position: Position = None,
            **kwargs,
    ):
        match_price = event.ask_price if position.net_position > 0 else event.bid_price
        ops = op.add if position.net_position > 0 else op.sub
        price = ops(tick_price, (self.skew_by * event.price_increment * 1000000))

        orders = []
        if not position.exit_attr.get("lastprice"):
            position.exit_attr["lastprice"] = price
            position.exit_attr["hold_time"] = 0
            position.exit_attr["start_time"] = event.timestamp

        else:
            if position.exit_attr["lastprice"] == match_price:
                orders.append(
                    Order(
                        event.timestamp,
                        event.order_book_id,
                        account,
                        position.net_position * -1,
                        price=match_price,
                        order_type="N",
                        time_in_force="K",
                        signal="passive",
                        symbol=event.symbol,
                        event_type="hedge",
                        )
                )

            position.exit_attr["lastprice"] = price
            position.exit_attr["hold_time"] = (
                    event.timestamp - position.exit_attr["start_time"]
            )

        return orders
