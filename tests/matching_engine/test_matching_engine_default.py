import unittest

from risk_backtesting.matching_engine.matching_engine_default import (
    MatchingEngineDefault,
)
from risk_backtesting.event import Event
from risk_backtesting.order import Order


class TestDefaultMatching(unittest.TestCase):
    def setUp(self):
        self.eng = MatchingEngineDefault(matching_method="side_of_book")

    def test_order_matched_for_long_order_with_price(self):

        event = Event(
            order_book_id=100,
            unit_price=10000,
            symbol="A/B",
            currency="USD",
            price_increment=0.00001,
            timestamp=1,
            counterparty_account_id=2,
            ask_price=1.00001 * 1000000,
            bid_price=1.00003 * 1000000,
            bid_qty=100 * 100,
            ask_qty=100 * 100,
            venue=1,
            rate_to_usd=1,
            trading_session_year=2020,
            trading_session_month=6,
            trading_session_day=9,
        )

        order = Order(
            timestamp=1,
            order_book_id=100,
            symbol="A/B",
            account_id=1,
            order_qty=0.1 * 100,
            order_type="N",
            time_in_force="K",
            price=1.00001 * 1000000,
            event_type="hedge",
        )

        trades = self.eng.match_order(event, order)

        for trade in trades:
            self.assertIsInstance(trade, Event)
            self.assertEqual(trade.order_book_id, 100)
            self.assertEqual(trade.symbol, "A/B")
            self.assertEqual(trade.price, 1000010)
            self.assertEqual(trade.contract_qty, 10)
            self.assertEqual(trade.account_id, 1)
            self.assertEqual(trade.timestamp, 1)
            self.assertEqual(trade.event_type, "hedge")
            self.assertEqual(trade.venue, 1)

    def test_order_matched_for_long_order_without_price(self):

        event = Event(
            order_book_id=100,
            unit_price=10000,
            symbol="A/B",
            currency="USD",
            price_increment=0.00001,
            timestamp=1,
            counterparty_account_id=2,
            ask_price=1.00001 * 1000000,
            bid_price=1.00003 * 1000000,
            bid_qty=100 * 100,
            ask_qty=100 * 100,
            venue=1,
            rate_to_usd=1,
            trading_session_year=2020,
            trading_session_month=6,
            trading_session_day=9,
        )

        order = Order(
            timestamp=1,
            order_book_id=100,
            symbol="A/B",
            account_id=1,
            order_qty=0.1 * 100,
            order_type="N",
            time_in_force="K",
            event_type="hedge",
        )

        trades = self.eng.match_order(event, order)

        for trade in trades:
            self.assertIsInstance(trade, Event)
            self.assertEqual(trade.order_book_id, 100)
            self.assertEqual(trade.symbol, "A/B")
            self.assertEqual(trade.price, 1000010)
            self.assertEqual(trade.contract_qty, 10)
            self.assertEqual(trade.account_id, 1)
            self.assertEqual(trade.timestamp, 1)
            self.assertEqual(trade.event_type, "hedge")
            self.assertEqual(trade.venue, 1)


if __name__ == "__main__":
    unittest.main()
