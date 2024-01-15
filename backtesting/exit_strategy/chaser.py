import operator

from ..exit_strategy.base import AbstractExitStrategy
from ..order import Order
from ..position import Position


class Chaser(AbstractExitStrategy):
    __slots__ = ("name", "starttick", "uptick", "downtick", "maxuptick", "maxdowntick")

    def __init__(self, uptick, downtick, maxuptick, maxdowntick, starttick):
        self.name: str = "chaser"
        self.uptick: float = uptick
        self.downtick: float = downtick
        self.maxuptick: float = maxuptick
        self.maxdowntick: float = maxdowntick
        self.starttick: float = starttick

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
        op = (
            (operator.gt, operator.sub, operator.add, operator.le)
            if position.net_position > 0
            else (operator.lt, operator.add, operator.sub, operator.ge)
        )

        # set the start tick to the price that position was opened at for first market data event of open position
        if not position.exit_attr.get("starttick"):
            position.exit_attr["starttick"] = round(
                position.open_positions[0].price
                if type(position.open_positions) == list
                else position.open_positions.price
                     + (self.starttick * position.price_increment)
            )
            position.exit_attr["tick_price"] = round(position.exit_attr["starttick"])
            position.exit_attr["chaser_price"] = round(position.exit_attr["starttick"])

        # check with direction the market has moved and then see if chaser has exceed max tick limits
        if op[0](
                tick_price, position.exit_attr["tick_price"]
        ):  # tick moved into profit
            position.exit_attr["chaser_price"] = op[2](
                position.exit_attr["chaser_price"],
                (round((self.uptick * position.price_increment) * 1000000)),
            )

            chaser_dist = (
                operator.sub(position.exit_attr["chaser_price"], tick_price)
                if position.net_position > 0
                else operator.sub(tick_price, position.exit_attr["chaser_price"])
            )

            # if new chaser price is further away then the maxuptick set the chaser price to tick_price +- maxuptick
            if operator.gt(
                    round(chaser_dist / (1000000 * position.price_increment)),
                    self.maxuptick,
            ):
                position.exit_attr["chaser_price"] = op[2](
                    tick_price,
                    (round((self.maxuptick * position.price_increment) * 1000000)),
                )

        elif op[0](
                position.exit_attr["tick_price"], tick_price
        ):  # tick moved into loss
            position.exit_attr["chaser_price"] = op[1](
                position.exit_attr["chaser_price"],
                (round((self.downtick * position.price_increment) * 1000000)),
            )

            chaser_dist = (
                operator.sub(tick_price, position.exit_attr["chaser_price"])
                if position.net_position > 0
                else operator.sub(position.exit_attr["chaser_price"], tick_price)
            )

            # if new chaser price is further away then the maxdowntick set the chaser price to tick_price +- downuptick
            if operator.gt(
                    round(chaser_dist / (1000000 * position.price_increment)),
                    self.maxdowntick,
            ):
                position.exit_attr["chaser_price"] = op[1](
                    tick_price,
                    (round((self.maxdowntick * position.price_increment) * 1000000)),
                )

        # check to see if the chaser can now match the order book

        if op[3](position.exit_attr["chaser_price"], tick_price):
            orders.append(
                Order(
                    event.timestamp,
                    event.order_book_id,
                    account,
                    position.net_position * -1,
                    symbol=event.symbol,
                    order_type="P",
                    time_in_force="K",
                    price=tick_price,
                    signal="chaser_price_meet",
                    event_type="hedge",
                    )
            )

        position.exit_attr["tick_price"] = tick_price

        return orders
