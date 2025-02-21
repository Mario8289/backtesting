import operator as op

from ..event import Event
from ..exit_strategy.base import AbstractExitStrategy
from ..order import Order
from ..position import Position


class Aggressive(AbstractExitStrategy):
    __slots__ = ("name", "stoploss_limit", "takeprofit_limit", "exit_method")

    def __init__(self, stoploss_limit, takeprofit_limit, exit_method):
        self.name: str = "aggressive"
        self.stoploss_limit: int = stoploss_limit
        self.takeprofit_limit: int = takeprofit_limit
        self.exit_method: str = exit_method

    def profitloss_price(self, price: float, position: float, price_increment: float):

        ops = [op.add, op.sub] if position > 0 else [op.sub, op.add]

        if self.exit_method == 'tick':
            # Set TakeProfit limit
            tp_price = int(
                ops[0](price, (price_increment * self.takeprofit_limit))
            )

            # Set StopLoss limit
            sl_price = int(
                ops[1](price, (price_increment * self.stoploss_limit))
            )

        elif self.exit_method == 'percent':
            # Set TakeProfit limit
            tp_price = price * (ops[0](1, (self.takeprofit_limit)))

            # Set StopLoss limit
            sl_price = price * (ops[1](1, (self.stoploss_limit)))
        else:
            raise ValueError(f"exit method {self.exit_method} is not a valid choice.")

        return sl_price, tp_price

    def generate_exit_order_signal(
            self,
            event: Event,
            account: int,
            avg_price: float = None,
            tick_price: float = None,
            position: Position = None,
            contract_qty: float = None
    ):

        price_sl, price_tp = self.profitloss_price(
            avg_price, position.net_position, position.price_increment
        )

        ops = [op.ge, op.le] if position.net_position > 0 else [op.le, op.ge]

        orders = []

        contract_qty = contract_qty if contract_qty is not None else position.net_position

        if ops[0](tick_price, price_tp):  # TP
            orders.append(
                Order(
                    timestamp=event.timestamp,
                    symbol_id=event.symbol_id,
                    account_id=account,
                    contract_qty=contract_qty * -1,
                    order_type="R",
                    time_in_force="K",
                    symbol=event.symbol,
                    signal="Take_Profit",
                    source=event.source
                    )
            )

        if ops[1](tick_price, price_sl):  # SP
            orders.append(
                Order(
                    timestamp=event.timestamp,
                    symbol_id=event.symbol_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    order_type="S",
                    time_in_force="K",
                    symbol=event.symbol,
                    signal="Stop_Loss",
                    source=event.source
                    )
            )

        return orders
