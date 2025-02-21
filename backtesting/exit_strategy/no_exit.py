from ..exit_strategy.base import AbstractExitStrategy
from ..position import Position


class NoExit(AbstractExitStrategy):
    __slots__ = "name"

    def __init__(self):
        super().__init__()
        self.name = "exit_default"

    def generate_exit_order_signal(
            self,
            event,
            account,
            avg_price: float = None,
            tick_price: float = None,
            position: Position = None,
            contract_qty: float = None
    ):

        orders = []

        return orders
