import unittest

from risk_backtesting.backtester import Backtester
from risk_backtesting.risk_manager.base import AbstractRiskManager
from risk_backtesting.strategy.base import AbstractStrategy


class TestBacktester(unittest.TestCase):
    def setUp(self):
        self.backtester = Backtester(
            risk_manager=AbstractRiskManager,
            strategy=AbstractStrategy,
            netting_engine={"client": "fifo", "lmax": "fifo"},
            matching_method="mid",
        )

    def test_fill_order_order_filled(self):
        pass

    def test_fill_order_order_not_filled(self):
        pass

    def test_match_order_book_order_fully_filled(self):
        pass

    def test_match_order_book_order_partial_filled(self):
        pass

    def test_on_event_market_data_no_hedge(self):
        pass

    def test_on_event_market_data_hedge(self):
        pass

    def test_on_event_trade_open_position(self):
        pass

    def test_on_event_trade_close_position(self):
        pass


if __name__ == "__main__":
    unittest.main()
