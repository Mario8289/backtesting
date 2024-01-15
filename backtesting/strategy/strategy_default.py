from ..strategy.base import AbstractStrategy


class StrategyDefault(AbstractStrategy):
    __slots__ = ("name", "exit_strategy")

    def __init__(self, exit_strategy):
        self.name = "strategy_default"
        self.exit_strategy = exit_strategy

    def calculate_market_signals(self, client_portfolio, lmax_portfolio, event):

        return []

    def calculate_trade_signals(self, client_portfolio, lmax_portfolio, event):

        return []

    def process_state(self, client_portfolio, lmax_portfolio, event):
        switcher = {
            "trade": self.calculate_trade_signals,
            "market_data": self.calculate_market_signals,
        }
        func = switcher.get(event.event_type)
        return func(client_portfolio, lmax_portfolio, event)

    def on_state(self, client_portfolio, lmax_portfolio, event):
        orders = self.process_state(client_portfolio, lmax_portfolio, event)
        return orders

    def get_name(self) -> str:
        return self.name
