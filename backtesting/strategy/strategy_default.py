from ..strategy.base import AbstractStrategy
from ..order import Order


class StrategyDefault(AbstractStrategy):
    __slots__ = ("name", "exit_strategy")

    def __init__(self, exit_strategy):
        self.name = "strategy_default"
        self.exit_strategy = exit_strategy

    def calculate_market_signals(self, portfolio, event):
        orders = []
        if portfolio.total_net_position == 0:
            orders.append(
                Order(
                    timestamp=event.get_timestamp(),
                    source=event.source,
                    symbol_id=event.symbol_id,
                    account_id=1,
                    contract_qty=1,
                    order_type='P',
                    time_in_force='K',
                    symbol=event.symbol,
                    price=event.price,
                    limit_price=None,
                    signal='Random Buy',
                    event_type='hedge'
                )
            )
        return orders

    def calculate_trade_signals(self, portfolio, event):

        return []

    def process_state(self, portfolio, event):
        if event.event_type == 'market_data':
            result = self.calculate_market_signals(portfolio, event)
        else:
            orders = []
        return result

    def on_state(self, portfolio, event):
        orders = self.process_state(portfolio, event)
        return orders

    def get_name(self) -> str:
        return self.name
