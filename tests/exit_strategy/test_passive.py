import unittest

import pandas as pd

from risk_backtesting.exit_strategy.passive import Passive
from risk_backtesting.position import Position
from risk_backtesting.order import Order
from risk_backtesting.event import Event


class PassiveSignals(unittest.TestCase):
    def setUp(self):
        self.pas = Passive(skew_at="same_side", skew_by=1, passive_limit=None)

    def test_passive_follows_ask_on_short_position(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        events = pd.DataFrame(
            [
                [1, "A/B", "trade", 1.00001, 1.00003, -1, 0, 0],
                [
                    2,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00003,
                    0,
                ],  # move 1 inside ask
                [
                    3,
                    "A/B",
                    "market_data",
                    1.00001,
                    1.00003,
                    0,
                    1.00002,
                    1,
                ],  # move 1 inside ask
                [
                    4,
                    "A/B",
                    "market_data",
                    1.00000,
                    1.00002,
                    0,
                    1.00001,
                    2,
                ],  # move 1 inside ask
            ],
            columns=[
                "timestamp",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "order_price",
                "hold_time",
            ],
        )

        signals = [[], [], [], []]

        for idx, event in events.fillna(0).iterrows():
            if event.event_type == "trade":
                position.on_trade(
                    event.contract_qty * 100, int(event.bid * 1000000), rate_to_usd=1
                )
            else:
                total_opening_cost = sum(
                    [x.price * x.quantity for x in position.open_positions]
                )
                total_opening_quantity = sum(
                    [x.quantity for x in position.open_positions]
                )

                evt = Event(
                    event_type=event.event_type,
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    bid_price=int(round(event.bid * 1000000)),
                    ask_price=int(round(event.ask * 1000000)),
                    price_increment=0.00001,
                )

                exit_signal = self.pas.generate_exit_order_signal(
                    event=evt,
                    account=123,
                    avg_price=int(
                        round((total_opening_cost / total_opening_quantity), 0)
                    ),
                    tick_price=round(event.ask * 1000000),
                    position=position,
                )

                if exit_signal:
                    for idx2, order in enumerate(exit_signal):
                        self.assertEqual(order.price, signals[idx][idx2].price)
                        self.assertEqual(order.order_qty, signals[idx][idx2].order_qty)
                        self.assertEqual(order.signal, signals[idx][idx2].signal)

                self.assertEqual(
                    position.exit_attr["lastprice"],
                    int(round(event.order_price * 1000000)),
                )
                self.assertEqual(position.exit_attr["hold_time"], event.hold_time)
                self.assertEqual(position.exit_attr["start_time"], 2)

    def test_passive_follows_ask_on_short_position_then_matches(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        events = pd.DataFrame(
            [
                [1, "A/B", "trade", 1.00001, 1.00003, -1, 0, 0],
                [
                    2,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00003,
                    0,
                ],  # move 1 inside ask
                [
                    3,
                    "A/B",
                    "market_data",
                    1.00001,
                    1.00003,
                    0,
                    1.00002,
                    1,
                ],  # move 1 inside ask
                [
                    4,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00003,
                    2,
                ],  # move 1 inside ask
            ],
            columns=[
                "timestamp",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "order_price",
                "hold_time",
            ],
        )

        signals = [
            [],
            [],
            [],
            [
                Order(
                    4,
                    "A/B",
                    123,
                    100,
                    "N",
                    "K",
                    price=1000020,
                    signal="passive",
                    event_type="hedge",
                )
            ],
        ]

        for idx, event in events.fillna(0).iterrows():
            if event.event_type == "trade":
                position.on_trade(
                    event.contract_qty * 100, int(event.bid * 1000000), rate_to_usd=1
                )
            else:
                total_opening_cost = sum(
                    [x.price * x.quantity for x in position.open_positions]
                )
                total_opening_quantity = sum(
                    [x.quantity for x in position.open_positions]
                )

                evt = Event(
                    event_type=event.event_type,
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    bid_price=int(round(event.bid * 1000000)),
                    ask_price=int(round(event.ask * 1000000)),
                    price_increment=0.00001,
                )

                exit_signal = self.pas.generate_exit_order_signal(
                    event=evt,
                    account=123,
                    avg_price=int(
                        round((total_opening_cost / total_opening_quantity), 0)
                    ),
                    tick_price=round(event.ask * 1000000),
                    position=position,
                )

                if exit_signal:
                    for idx2, order in enumerate(exit_signal):
                        self.assertEqual(order.price, signals[idx][idx2].price)
                        self.assertEqual(order.order_qty, signals[idx][idx2].order_qty)
                        self.assertEqual(order.signal, signals[idx][idx2].signal)

                self.assertEqual(
                    int(round(event.order_price * 1000000)),
                    position.exit_attr["lastprice"],
                )
                self.assertEqual(position.exit_attr["hold_time"], event.hold_time)
                self.assertEqual(position.exit_attr["start_time"], 2)

    def test_passive_follows_bid_on_long_position(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        events = pd.DataFrame(
            [
                [1, "A/B", "trade", 1.00001, 1.00003, 1, 0],
                [
                    2,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00003,
                    0,
                ],  # move 1 inside ask
                [
                    3,
                    "A/B",
                    "market_data",
                    1.00003,
                    1.00005,
                    0,
                    1.00004,
                    1,
                ],  # move 1 inside ask
                [
                    4,
                    "A/B",
                    "market_data",
                    1.00004,
                    1.00006,
                    0,
                    1.00005,
                    2,
                ],  # move 1 inside ask
            ],
            columns=[
                "timestamp",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "order_price",
                "hold_time",
            ],
        )

        signals = [[], [], [], []]

        for idx, event in events.fillna(0).iterrows():
            if event.event_type == "trade":
                position.on_trade(
                    event.contract_qty * 100, int(event.ask * 1000000), rate_to_usd=1
                )
            else:
                total_opening_cost = sum(
                    [x.price * x.quantity for x in position.open_positions]
                )
                total_opening_quantity = sum(
                    [x.quantity for x in position.open_positions]
                )

                evt = Event(
                    event_type=event.event_type,
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    bid_price=int(round(event.bid * 1000000)),
                    ask_price=int(round(event.ask * 1000000)),
                    price_increment=0.00001,
                )

                exit_signal = self.pas.generate_exit_order_signal(
                    event=evt,
                    account=123,
                    avg_price=int(
                        round((total_opening_cost / total_opening_quantity), 0)
                    ),
                    tick_price=round(event.bid * 1000000),
                    position=position,
                )

                if exit_signal:
                    for idx2, order in enumerate(exit_signal):
                        self.assertEqual(order.price, signals[idx][idx2].price)
                        self.assertEqual(order.order_qty, signals[idx][idx2].order_qty)
                        self.assertEqual(order.signal, signals[idx][idx2].signal)

                self.assertEqual(
                    position.exit_attr["lastprice"],
                    int(round(event.order_price * 1000000)),
                )
                self.assertEqual(position.exit_attr["hold_time"], event.hold_time)
                self.assertEqual(position.exit_attr["start_time"], 2)

    def test_passive_follows_ask_on_long_position_then_matches(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        events = pd.DataFrame(
            [
                [1, "A/B", "trade", 1.00001, 1.00003, 1, 0, 0],
                [
                    2,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00003,
                    0,
                ],  # move 1 inside ask
                [
                    3,
                    "A/B",
                    "market_data",
                    1.00003,
                    1.00005,
                    0,
                    1.00004,
                    1,
                ],  # move 1 inside ask
                [
                    4,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00003,
                    2,
                ],  # move 1 inside ask
            ],
            columns=[
                "timestamp",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "order_price",
                "hold_time",
            ],
        )

        signals = [
            [],
            [],
            [],
            [
                Order(
                    4,
                    "A/B",
                    123,
                    -100,
                    "N",
                    "K",
                    price=1000040,
                    signal="passive",
                    event_type="hedge",
                )
            ],
        ]

        for idx, event in events.fillna(0).iterrows():
            if event.event_type == "trade":
                position.on_trade(
                    event.contract_qty * 100, int(event.ask * 1000000), rate_to_usd=1
                )
            else:
                total_opening_cost = sum(
                    [x.price * x.quantity for x in position.open_positions]
                )
                total_opening_quantity = sum(
                    [x.quantity for x in position.open_positions]
                )

                evt = Event(
                    event_type=event.event_type,
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    bid_price=int(round(event.bid * 1000000)),
                    ask_price=int(round(event.ask * 1000000)),
                    price_increment=0.00001,
                )

                exit_signal = self.pas.generate_exit_order_signal(
                    event=evt,
                    account=123,
                    avg_price=int(
                        round((total_opening_cost / total_opening_quantity), 0)
                    ),
                    tick_price=round(event.bid * 1000000),
                    position=position,
                )

                if exit_signal:
                    for idx2, order in enumerate(exit_signal):
                        self.assertEqual(order.price, signals[idx][idx2].price)
                        self.assertEqual(order.order_qty, signals[idx][idx2].order_qty)
                        self.assertEqual(order.signal, signals[idx][idx2].signal)

                self.assertEqual(
                    position.exit_attr["lastprice"],
                    int(round(event.order_price * 1000000)),
                )
                self.assertEqual(position.exit_attr["hold_time"], event.hold_time)
                self.assertEqual(position.exit_attr["start_time"], 2)


if __name__ == "__main__":
    unittest.main()
