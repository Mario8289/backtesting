import datetime as dt
import unittest

import pandas as pd

from risk_backtesting.exit_strategy.chaser import Chaser
from risk_backtesting.order import Order
from risk_backtesting.position import Position


class ChaserSignals(unittest.TestCase):
    def setUp(self):
        starttick = 0
        uptick = 1
        downtick = 2
        maxuptick = 2
        maxdowntick = 5
        self.chaser = Chaser(uptick, downtick, maxuptick, maxdowntick, starttick)

    def test_chaser_for_short_pos_downtick_triggers_passive_buy(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.chaser.starttick = 0
        self.chaser.uptick = 1
        self.chaser.downtick = 2
        self.chaser.maxuptick = 2
        self.chaser.maxdowntick = 5

        events = pd.DataFrame(
            [
                [1, 456, "A/B", "trade", 1.00001, 1.00003, -1, 1.00001, 0],
                [
                    2,
                    456,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00001,
                    1.00003,
                ],  # move towards ask 2 ticks
                [
                    3,
                    456,
                    "A/B",
                    "market_data",
                    1.00001,
                    1.00003,
                    0,
                    1.00001,
                    1.00002,
                ],  # move away ask 1 tick
                [
                    4,
                    456,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00001,
                    1.00004,
                ],  # move towards ask 2 ticks
            ],
            columns=[
                "timestamp",
                "order_book_id",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "starttick",
                "chaser_price",
            ],
        )

        signals = [
            [],
            [],
            [],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=1000040,
                    signal="chaser_price_meet",
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
                exit_signal = self.chaser.generate_exit_order_signal(
                    event=event,
                    account=123,
                    avg_price=int(
                        round((total_opening_cost / total_opening_quantity), 0)
                    ),
                    tick_price=event.ask * 1000000,
                    position=position,
                )

                if exit_signal:
                    for idx2, order in enumerate(exit_signal):
                        self.assertEqual(order.price, signals[idx][idx2].price)
                        self.assertEqual(order.order_qty, signals[idx][idx2].order_qty)
                        self.assertEqual(order.signal, signals[idx][idx2].signal)
                else:
                    self.assertEqual(signals[idx], exit_signal)
                self.assertEqual(
                    round(event.ask * 1000000), position.exit_attr["tick_price"]
                )
                self.assertEqual(
                    round(event.starttick * 1000000), position.exit_attr["starttick"]
                )
                self.assertEqual(
                    round(event.chaser_price * 1000000),
                    position.exit_attr["chaser_price"]
                    if position.exit_attr.get("chaser_price")
                    else 0,
                )

    def test_chaser_for_long_pos_downtick_triggers_passive_sell(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.chaser.starttick = 0
        self.chaser.uptick = 1
        self.chaser.downtick = 2
        self.chaser.maxuptick = 2
        self.chaser.maxdowntick = 5

        events = pd.DataFrame(
            [
                [1, 456, "A/B", "trade", 1.00001, 1.00003, 1, 1.00003, 0],
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00000,
                    1.00002,
                    0,
                    1.00003,
                    1.00001,
                ],  # move towards bid 2 ticks
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00001,
                    1.00003,
                    0,
                    1.00003,
                    1.00002,
                ],  # move away bid 1 tick
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00000,
                    1.00002,
                    0,
                    1.00003,
                    1.00000,
                ],  # move towards bid 2 ticks
            ],
            columns=[
                "timestamp",
                "order_book_id",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "starttick",
                "chaser_price",
            ],
        )

        signals = [
            [],
            [],
            [],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    -100,
                    "P",
                    "K",
                    price=1000000,
                    symbol="A/B",
                    signal="chaser_price_meet",
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
                exit_signal = self.chaser.generate_exit_order_signal(
                    event=event,
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
                else:
                    self.assertEqual(signals[idx], exit_signal)
                self.assertEqual(
                    round(event.bid * 1000000), position.exit_attr["tick_price"]
                )
                self.assertEqual(
                    round(event.starttick * 1000000), position.exit_attr["starttick"]
                )
                self.assertEqual(
                    round(event.chaser_price * 1000000),
                    position.exit_attr["chaser_price"]
                    if position.exit_attr.get("chaser_price")
                    else 0,
                )

    def test_chaser_for_short_pos_uptick_triggers_passive_buy(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.chaser.starttick = 0
        self.chaser.uptick = 1
        self.chaser.downtick = 2
        self.chaser.maxuptick = 2
        self.chaser.maxdowntick = 5

        events = pd.DataFrame(
            [
                [1, 456, "A/B", "trade", 1.00001, 1.00003, -1, 1.00001, 0],
                [
                    2,
                    456,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00001,
                    1.00003,
                ],  # move towards ask 2 ticks
                [
                    3,
                    456,
                    "A/B",
                    "market_data",
                    1.00001,
                    1.00003,
                    0,
                    1.00001,
                    1.00002,
                ],  # move away ask 1 tick
                [
                    4,
                    456,
                    "A/B",
                    "market_data",
                    0.99999,
                    1.00001,
                    0,
                    1.00001,
                    1.00001,
                ],  # move away ask 2 ticks
            ],
            columns=[
                "timestamp",
                "order_book_id",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "starttick",
                "chaser_price",
            ],
        )

        signals = [
            [],
            [],
            [],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    100,
                    "P",
                    "K",
                    price=1000010,
                    symbol="A/B",
                    signal="chaser_price_meet",
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
                exit_signal = self.chaser.generate_exit_order_signal(
                    event=event,
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
                else:
                    self.assertEqual(signals[idx], exit_signal)
                self.assertEqual(
                    round(event.ask * 1000000), position.exit_attr["tick_price"]
                )
                self.assertEqual(
                    round(event.starttick * 1000000), position.exit_attr["starttick"]
                )
                self.assertEqual(
                    round(event.chaser_price * 1000000),
                    position.exit_attr["chaser_price"]
                    if position.exit_attr.get("chaser_price")
                    else 0,
                )

    def test_chaser_for_long_pos_uptick_triggers_passive_sell(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.chaser.starttick = 0
        self.chaser.uptick = 1
        self.chaser.downtick = 2
        self.chaser.maxuptick = 2
        self.chaser.maxdowntick = 5

        events = pd.DataFrame(
            [
                [1, 456, "A/B", "trade", 1.00001, 1.00003, 1, 1.00003, 0],
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00000,
                    1.00002,
                    0,
                    1.00003,
                    1.00001,
                ],  # move towards bid 2 ticks
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00001,
                    1.00003,
                    0,
                    1.00003,
                    1.00002,
                ],  # move away bid 1 tick
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00003,
                    1.00005,
                    0,
                    1.00003,
                    1.00003,
                ],  # move away bid 2 ticks
            ],
            columns=[
                "timestamp",
                "order_book_id",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "starttick",
                "chaser_price",
            ],
        )

        signals = [
            [],
            [],
            [],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    -100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=1000030,
                    signal="chaser_price_meet",
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
                exit_signal = self.chaser.generate_exit_order_signal(
                    event=event,
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
                else:
                    self.assertEqual(signals[idx], exit_signal)
                self.assertEqual(
                    round(event.bid * 1000000), position.exit_attr["tick_price"]
                )
                self.assertEqual(
                    round(event.starttick * 1000000), position.exit_attr["starttick"]
                )
                self.assertEqual(
                    round(event.chaser_price * 1000000),
                    position.exit_attr["chaser_price"]
                    if position.exit_attr.get("chaser_price")
                    else 0,
                )

    def test_chaser_for_short_pos_maxuptick_triggers_aggressive_buy(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.chaser.starttick = 0
        self.chaser.uptick = 2
        self.chaser.downtick = 2
        self.chaser.maxuptick = 2
        self.chaser.maxdowntick = 2

        events = pd.DataFrame(
            [
                [1, 456, "A/B", "trade", 1.00001, 1.00003, -1, 1.00001, 0],
                [
                    2,
                    456,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00001,
                    1.00003,
                ],  # move towards ask 2 ticks
                [
                    3,
                    456,
                    "A/B",
                    "market_data",
                    1.00001,
                    1.00003,
                    0,
                    1.00001,
                    1.00001,
                ],  # move away ask 2 tick
                [
                    4,
                    456,
                    "A/B",
                    "market_data",
                    1.00000,
                    1.00002,
                    0,
                    1.00001,
                    1.00000,
                ],  # move away ask 1 ticks due to max tick
                [
                    4,
                    456,
                    "A/B",
                    "market_data",
                    0.99999,
                    1.00001,
                    0,
                    1.00001,
                    0.99999,
                ],  # move away ask 2 ticks
                [
                    4,
                    456,
                    "A/B",
                    "market_data",
                    1.00000,
                    1.00002,
                    0,
                    1.00001,
                    1.00001,
                ],  # move towards ask 2 ticks
                [
                    4,
                    456,
                    "A/B",
                    "market_data",
                    1.00001,
                    1.00003,
                    0,
                    1.00001,
                    1.00003,
                ],  # match at 1.00003
            ],
            columns=[
                "timestamp",
                "order_book_id",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "starttick",
                "chaser_price",
            ],
        )

        signals = [
            [],
            [],
            [],
            [],
            [],
            [],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=1000030,
                    signal="chaser_price_meet",
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
                exit_signal = self.chaser.generate_exit_order_signal(
                    event=event,
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
                else:
                    self.assertEqual(signals[idx], exit_signal)
                self.assertEqual(
                    round(event.ask * 1000000), position.exit_attr["tick_price"]
                )
                self.assertEqual(
                    round(event.starttick * 1000000), position.exit_attr["starttick"]
                )
                self.assertEqual(
                    round(event.chaser_price * 1000000),
                    position.exit_attr["chaser_price"]
                    if position.exit_attr.get("chaser_price")
                    else 0,
                )

    def test_chaser_for_long_pos_maxuptick_triggers_aggressive_sell(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.chaser.starttick = 0
        self.chaser.uptick = 2
        self.chaser.downtick = 2
        self.chaser.maxuptick = 3
        self.chaser.maxdowntick = 3

        events = pd.DataFrame(
            [
                [1, 456, "A/B", "trade", 1.00001, 1.00003, 1, 1.00003, 0],
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00000,
                    1.00002,
                    0,
                    1.00003,
                    1.00001,
                ],  # move towards bid 2 ticks
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00001,
                    1.00003,
                    0,
                    1.00003,
                    1.00003,
                ],  # move away bid 2 tick
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00003,
                    1.00005,
                ],  # move away bid 2 ticks
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00003,
                    1.00005,
                    0,
                    1.00003,
                    1.00006,
                ],  # move away bid 2 ticks
            ],
            columns=[
                "timestamp",
                "order_book_id",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "starttick",
                "chaser_price",
            ],
        )

        signals = [[], [], [], [], []]

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
                exit_signal = self.chaser.generate_exit_order_signal(
                    event=event,
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
                else:
                    self.assertEqual(signals[idx], exit_signal)
                self.assertEqual(
                    round(event.bid * 1000000), position.exit_attr["tick_price"]
                )
                self.assertEqual(
                    round(event.starttick * 1000000), position.exit_attr["starttick"]
                )
                self.assertEqual(
                    round(event.chaser_price * 1000000),
                    position.exit_attr["chaser_price"]
                    if position.exit_attr.get("chaser_price")
                    else 0,
                )

    def test_chaser_for_short_pos_maxdowntick_triggers_aggressive_buy(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.chaser.starttick = 0
        self.chaser.uptick = 2
        self.chaser.downtick = 2
        self.chaser.maxuptick = 3
        self.chaser.maxdowntick = 2

        events = pd.DataFrame(
            [
                [1, 456, "A/B", "trade", 1.00001, 1.00003, -1, 1.00001, 0],
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00002,
                    1.00004,
                    0,
                    1.00001,
                    1.00003,
                ],  # move towards ask 2 ticks
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00003,
                    1.00005,
                    0,
                    1.00001,
                    1.00005,
                ],  # move towards ask 2 tick
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00004,
                    1.00006,
                    0,
                    1.00001,
                    1.00007,
                ],  # move towards ask 2 tick
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00003,
                    1.00007,
                    0,
                    1.00001,
                    1.00009,
                ],  # move towards ask 2 tick
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00003,
                    1.00008,
                    0,
                    1.00001,
                    1.00010,
                ],  # move towards ask 1 tick due to maxdown
            ],
            columns=[
                "timestamp",
                "order_book_id",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "starttick",
                "chaser_price",
            ],
        )

        signals = [
            [],
            [],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=1000050,
                    signal="chaser_price_meet",
                    event_type="hedge",
                )
            ],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=1000060,
                    signal="chaser_price_meet",
                    event_type="hedge",
                )
            ],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=1000070,
                    signal="chaser_price_meet",
                    event_type="hedge",
                )
            ],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=1000080,
                    signal="chaser_price_meet",
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
                exit_signal = self.chaser.generate_exit_order_signal(
                    event=event,
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
                else:
                    self.assertEqual(signals[idx], exit_signal)
                self.assertEqual(
                    round(event.ask * 1000000), position.exit_attr["tick_price"]
                )
                self.assertEqual(
                    round(event.starttick * 1000000), position.exit_attr["starttick"]
                )
                self.assertEqual(
                    round(event.chaser_price * 1000000),
                    position.exit_attr["chaser_price"]
                    if position.exit_attr.get("chaser_price")
                    else 0,
                )

    def test_chaser_for_long_pos_maxdowntick_triggers_aggressive_sell(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.chaser.starttick = 0
        self.chaser.uptick = 2
        self.chaser.downtick = 3
        self.chaser.maxuptick = 3
        self.chaser.maxdowntick = 2

        events = pd.DataFrame(
            [
                [1, 456, "A/B", "trade", 1.00001, 1.00003, 1, 1.00003, 0],
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    1.00000,
                    1.00002,
                    0,
                    1.00003,
                    1.00000,
                ],  # move towards bid 3 ticks
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    0.99999,
                    1.00001,
                    0,
                    1.00003,
                    0.99997,
                ],  # move towards bid 3 tick
                [
                    1,
                    456,
                    "A/B",
                    "market_data",
                    0.99998,
                    1.00000,
                    0,
                    1.00003,
                    0.99996,
                ],  # move towards bid 3 tick
            ],
            columns=[
                "timestamp",
                "order_book_id",
                "symbol",
                "event_type",
                "bid",
                "ask",
                "contract_qty",
                "starttick",
                "chaser_price",
            ],
        )

        signals = [
            [],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    -100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=1000000,
                    signal="chaser_price_meet",
                    event_type="hedge",
                )
            ],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    -100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=999990,
                    signal="chaser_price_meet",
                    event_type="hedge",
                )
            ],
            [
                Order(
                    dt.datetime.utcnow(),
                    456,
                    123,
                    -100,
                    "P",
                    "K",
                    symbol="A/B",
                    price=999980,
                    signal="chaser_price_meet",
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
                exit_signal = self.chaser.generate_exit_order_signal(
                    event=event,
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
                else:
                    self.assertEqual(signals[idx], exit_signal)
                self.assertEqual(
                    round(event.bid * 1000000), position.exit_attr["tick_price"]
                )
                self.assertEqual(
                    round(event.starttick * 1000000), position.exit_attr["starttick"]
                )
                self.assertEqual(
                    round(event.chaser_price * 1000000),
                    position.exit_attr["chaser_price"]
                    if position.exit_attr.get("chaser_price")
                    else 0,
                )


if __name__ == "__main__":
    unittest.main()
