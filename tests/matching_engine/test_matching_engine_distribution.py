import unittest

import pandas as pd

from risk_backtesting.matching_engine.matching_engine_distribution import (
    MatchingEngineDistribution,
)
from risk_backtesting.event import Event
from risk_backtesting.order import Order


class TestDefaultMatching(unittest.TestCase):
    def setUp(self):
        self.eng = MatchingEngineDistribution(matching_method="side_of_book")

        self.instrument = 100
        self.eng.model["orderbook_distribution"] = pd.DataFrame(
            {
                "instrument_id": [self.instrument] * 7,
                "pips_order": range(0, 7, 1),
                "contracts_scaled": [1, 1.2, 1.3, 1.5, 1.7, 1.5, 1.9],
                "price_increment": [0.00001] * 7,
            }
        )

        self.eng.model["orderbook_pip_depth"] = pd.DataFrame(
            {
                "instrument_id": [self.instrument] * 7,
                "pips_order": range(0, 7, 1),
                "pips_diff": list(range(0, 6, 1)) + [7],
            }
        )

        self.eng.model["spread_contracts"] = pd.DataFrame(
            {
                "instrument_id": [self.instrument] * 6,
                "spread": [
                    x * 1000000
                    for x in [-0.00001, 0.00000, 0.00001, 0.00002, 0.00003, 0.00004]
                ],
                "quantity": [x * 100 for x in [50, 75] * 2]
                            + [x * 100 for x in [50, 80]],
            }
        )

    def test_order_matched_for_long_SL_order_with_price_fully_fill_at_tob(self):

        event = Event(
            order_book_id=self.instrument,
            unit_price=10000,
            symbol="A/B",
            currency="USD",
            price_increment=0.00001,
            timestamp=1,
            counterparty_account_id=2,
            ask_price=1.00003 * 1000000,
            bid_price=1.00001 * 1000000,
            bid_qty=50 * 100,
            ask_qty=50 * 100,
            venue=1,
            rate_to_usd=1,
            trading_session_year=2020,
            trading_session_month=6,
            trading_session_day=9,
        )

        order = Order(
            timestamp=1,
            order_book_id=self.instrument,
            symbol="A/B",
            account_id=1,
            order_qty=0.1 * 100,
            order_type="S",
            time_in_force="K",
            price=1.00003 * 1000000,
            event_type="hedge",
        )

        trades = self.eng.match_order(event, order)

        for trade in trades:
            self.assertIsInstance(trade, Event)
            self.assertEqual(trade.order_book_id, self.instrument)
            self.assertEqual(trade.symbol, "A/B")
            self.assertEqual(trade.price, 1000030)
            self.assertEqual(trade.contract_qty, 10)
            self.assertEqual(trade.account_id, 1)
            self.assertEqual(trade.timestamp, 1)
            self.assertEqual(trade.event_type, "hedge")
            self.assertEqual(trade.venue, 1)

    def test_order_matched_for_long_SL_order_with_price_consume_one_layer(self):

        event = Event(
            order_book_id=self.instrument,
            unit_price=10000,
            symbol="A/B",
            currency="USD",
            price_increment=0.00001,
            timestamp=1,
            counterparty_account_id=2,
            ask_price=1.00003 * 1000000,
            bid_price=1.00001 * 1000000,
            bid_qty=50 * 100,
            ask_qty=-50 * 100,
            venue=1,
            rate_to_usd=1,
            trading_session_year=2020,
            trading_session_month=6,
            trading_session_day=9,
        )

        order = Order(
            timestamp=1,
            order_book_id=self.instrument,
            symbol="A/B",
            account_id=1,
            order_qty=60 * 100,
            order_type="S",
            time_in_force="K",
            price=int(round(1.00003 * 1000000)),
            event_type="hedge",
        )

        trades = self.eng.match_order(event, order)

        expected_trades = [
            {
                "event_type": "hedge",
                "price": 1000030,
                "contract_qty": 5000,
                "order_qty": 6000,
            },
            {
                "event_type": "hedge",
                "price": 1000040,
                "contract_qty": 1000,
                "order_qty": 6000,
            },
        ]

        for trade, expected_trade in zip(trades, expected_trades):
            self.assertIsInstance(trade, Event)
            self.assertEqual(trade.order_book_id, self.instrument)
            self.assertEqual(trade.symbol, "A/B")
            self.assertEqual(trade.price, expected_trade["price"])
            self.assertEqual(trade.contract_qty, expected_trade["contract_qty"])
            self.assertEqual(trade.order_qty, expected_trade["order_qty"])
            self.assertEqual(trade.account_id, 1)
            self.assertEqual(trade.timestamp, 1)
            self.assertEqual(trade.event_type, "hedge")
            self.assertEqual(trade.venue, 1)

    def test_order_matched_for_short_SL_order_with_price_consumes_two_layer(self):

        event = Event(
            order_book_id=self.instrument,
            unit_price=10000,
            symbol="A/B",
            currency="USD",
            price_increment=0.00001,
            timestamp=1,
            counterparty_account_id=2,
            ask_price=1.00005 * 1000000,
            bid_price=1.00003 * 1000000,
            bid_qty=75 * 100,
            ask_qty=-75 * 100,
            venue=1,
            rate_to_usd=1,
            trading_session_year=2020,
            trading_session_month=6,
            trading_session_day=9,
        )

        order = Order(
            timestamp=1,
            order_book_id=self.instrument,
            symbol="A/B",
            account_id=1,
            order_qty=-195 * 100,
            order_type="S",
            time_in_force="K",
            price=int(round(1.00003 * 1000000)),
            event_type="hedge",
        )

        trades = self.eng.match_order(event, order)

        expected_trades = [
            {
                "event_type": "hedge",
                "price": 1000030,
                "contract_qty": -7500,
                "order_qty": -19500,
            },
            {
                "event_type": "hedge",
                "price": 1000020,
                "contract_qty": -9000,
                "order_qty": -19500,
            },
            {
                "event_type": "hedge",
                "price": 1000010,
                "contract_qty": -3000,
                "order_qty": -19500,
            },
        ]

        for trade, expected_trade in zip(trades, expected_trades):
            self.assertIsInstance(trade, Event)
            self.assertEqual(trade.order_book_id, self.instrument)
            self.assertEqual(trade.symbol, "A/B")
            self.assertEqual(trade.price, expected_trade["price"])
            self.assertEqual(trade.contract_qty, expected_trade["contract_qty"])
            self.assertEqual(trade.order_qty, expected_trade["order_qty"])
            self.assertEqual(trade.account_id, 1)
            self.assertEqual(trade.timestamp, 1)
            self.assertEqual(trade.event_type, "hedge")
            self.assertEqual(trade.venue, 1)

    def test_order_matched_for_long_SL_order_with_price_consume_all_layers(self):

        # order consumes all level of liqiudity and then on the last level fills with remaining unfilled quantity

        event = Event(
            order_book_id=self.instrument,
            unit_price=10000,
            symbol="A/B",
            currency="USD",
            price_increment=0.00001,
            timestamp=1,
            counterparty_account_id=2,
            ask_price=1.00005 * 1000000,
            bid_price=1.00004 * 1000000,
            bid_qty=50 * 100,
            ask_qty=-50 * 100,
            venue=1,
            rate_to_usd=1,
            trading_session_year=2020,
            trading_session_month=6,
            trading_session_day=9,
        )

        order = Order(
            timestamp=1,
            order_book_id=self.instrument,
            symbol="A/B",
            account_id=1,
            order_qty=550 * 100,
            order_type="S",
            time_in_force="K",
            price=int(round(1.00005 * 1000000)),
            event_type="hedge",
        )

        trades = self.eng.match_order(event, order)

        expected_trades = [
            {
                "event_type": "hedge",
                "price": 1000050,
                "contract_qty": 5000,
                "order_qty": 55000,
            },
            {
                "event_type": "hedge",
                "price": 1000060,
                "contract_qty": 6000,
                "order_qty": 55000,
            },
            {
                "event_type": "hedge",
                "price": 1000070,
                "contract_qty": 6500,
                "order_qty": 55000,
            },
            {
                "event_type": "hedge",
                "price": 1000080,
                "contract_qty": 7500,
                "order_qty": 55000,
            },
            {
                "event_type": "hedge",
                "price": 1000090,
                "contract_qty": 8500,
                "order_qty": 55000,
            },
            {
                "event_type": "hedge",
                "price": 1000100,
                "contract_qty": 7500,
                "order_qty": 55000,
            },
            {
                "event_type": "hedge",
                "price": 1000120,
                "contract_qty": 14000,
                "order_qty": 55000,
            },
        ]

        for trade, expected_trade in zip(trades, expected_trades):
            self.assertIsInstance(trade, Event)
            self.assertEqual(trade.order_book_id, self.instrument)
            self.assertEqual(trade.symbol, "A/B")
            self.assertEqual(trade.price, expected_trade["price"])
            self.assertEqual(trade.contract_qty, expected_trade["contract_qty"])
            self.assertEqual(trade.order_qty, expected_trade["order_qty"])
            self.assertEqual(trade.account_id, 1)
            self.assertEqual(trade.timestamp, 1)
            self.assertEqual(trade.event_type, "hedge")
            self.assertEqual(trade.venue, 1)

    def test_order_matched_for_long_SL_order_with_non_match_spread(self):

        event = Event(
            order_book_id=self.instrument,
            unit_price=10000,
            symbol="A/B",
            currency="USD",
            price_increment=0.00001,
            timestamp=1,
            counterparty_account_id=2,
            ask_price=1.00008 * 1000000,
            bid_price=1.00003 * 1000000,
            bid_qty=50 * 100,
            ask_qty=-50 * 100,
            venue=1,
            rate_to_usd=1,
            trading_session_year=2020,
            trading_session_month=6,
            trading_session_day=9,
        )

        order = Order(
            timestamp=1,
            order_book_id=self.instrument,
            symbol="A/B",
            account_id=1,
            order_qty=160 * 100,
            order_type="S",
            time_in_force="K",
            price=int(round(1.00008 * 1000000)),
            event_type="hedge",
        )

        trades = self.eng.match_order(event, order)

        expected_trades = [
            {
                "event_type": "hedge",
                "price": 1000080,
                "contract_qty": 5000,
                "order_qty": 16000,
            },
            {
                "event_type": "hedge",
                "price": 1000090,
                "contract_qty": 9600,
                "order_qty": 16000,
            },
            {
                "event_type": "hedge",
                "price": 1000100,
                "contract_qty": 1400,
                "order_qty": 16000,
            },
        ]

        for trade, expected_trade in zip(trades, expected_trades):
            self.assertIsInstance(trade, Event)
            self.assertEqual(trade.order_book_id, self.instrument)
            self.assertEqual(trade.symbol, "A/B")
            self.assertEqual(trade.price, expected_trade["price"])
            self.assertEqual(trade.contract_qty, expected_trade["contract_qty"])
            self.assertEqual(trade.order_qty, expected_trade["order_qty"])
            self.assertEqual(trade.account_id, 1)
            self.assertEqual(trade.timestamp, 1)
            self.assertEqual(trade.event_type, "hedge")
            self.assertEqual(trade.venue, 1)

    def test_order_matched_for_long_SL_order_with_non_match_spread_inverted_book(self):

        event = Event(
            order_book_id=self.instrument,
            unit_price=10000,
            symbol="A/B",
            currency="USD",
            price_increment=0.00001,
            timestamp=1,
            counterparty_account_id=2,
            ask_price=1.00002 * 1000000,
            bid_price=1.00005 * 1000000,
            bid_qty=50 * 100,
            ask_qty=-50 * 100,
            venue=1,
            rate_to_usd=1,
            trading_session_year=2020,
            trading_session_month=6,
            trading_session_day=9,
        )

        order = Order(
            timestamp=1,
            order_book_id=self.instrument,
            symbol="A/B",
            account_id=1,
            order_qty=160 * 100,
            order_type="S",
            time_in_force="K",
            price=int(round(1.00008 * 1000000)),
            event_type="hedge",
        )

        trades = self.eng.match_order(event, order)

        expected_trades = [
            {
                "event_type": "hedge",
                "price": 1000080,
                "contract_qty": 5000,
                "order_qty": 16000,
            },
            {
                "event_type": "hedge",
                "price": 1000090,
                "contract_qty": 6000,
                "order_qty": 16000,
            },
            {
                "event_type": "hedge",
                "price": 1000100,
                "contract_qty": 5000,
                "order_qty": 16000,
            },
        ]

        for trade, expected_trade in zip(trades, expected_trades):
            self.assertIsInstance(trade, Event)
            self.assertEqual(trade.order_book_id, self.instrument)
            self.assertEqual(trade.symbol, "A/B")
            self.assertEqual(trade.price, expected_trade["price"])
            self.assertEqual(trade.contract_qty, expected_trade["contract_qty"])
            self.assertEqual(trade.order_qty, expected_trade["order_qty"])
            self.assertEqual(trade.account_id, 1)
            self.assertEqual(trade.timestamp, 1)
            self.assertEqual(trade.event_type, "hedge")
            self.assertEqual(trade.venue, 1)


if __name__ == "__main__":
    unittest.main()
