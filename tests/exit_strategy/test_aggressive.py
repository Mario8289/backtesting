import unittest
from typing import List

from risk_backtesting.exit_strategy.aggressive import Aggressive
from risk_backtesting.position import Position
from risk_backtesting.event import Event


class AggressiveSignals(unittest.TestCase):
    def setUp(self):
        self.agg = Aggressive(stoploss_limit=2, takeprofit_limit=3)

    def test1_aggressive_tp_for_position_long_not_triggered(self):

        self.agg.stoploss_limit = 2
        self.agg.takeprofit_limit = 3
        event = Event(order_book_id=123, timestamp=1)
        avg_price = 1.10001 * 1000000
        price_tick = 1.10003 * 1000000

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        position.net_position = 100

        exit_signal = self.agg.generate_exit_order_signal(
            event=event,
            account=1,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertEqual(exit_signal, [])

    def test2_aggressive_tp_for_position_long_triggered(self):

        self.agg.stoploss_limit = 2
        self.agg.takeprofit_limit = 3

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        position.net_position = 100
        event = Event(order_book_id=123, timestamp=1)
        avg_price = 1.10001 * 1000000
        price_tick = 1.10004 * 1000000
        account = 1

        exit_signal = self.agg.generate_exit_order_signal(
            event=event,
            account=account,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertIsInstance(exit_signal, List)
        self.assertEqual(exit_signal.__len__(), 1)

        order = exit_signal[0]
        self.assertEqual(order.timestamp, event.timestamp)
        self.assertEqual(order.order_book_id, event.order_book_id)
        self.assertEqual(order.account_id, account)
        self.assertEqual(order.time_in_force, "K")
        self.assertEqual(order.order_type, "R")
        self.assertEqual(order.signal, "TP_close_position")
        self.assertEqual(order.event_type, "hedge")
        self.assertEqual(order.order_qty, -100)
        self.assertEqual(order.unfilled_qty, -100)

    def test3_agg_tp_for_position_short_not_triggered(self):

        self.agg.stoploss_limit = 2
        self.agg.takeprofit_limit = 5

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = -100

        event = Event(order_book_id=123, timestamp=1)
        avg_price = 1.10003 * 1000000
        price_tick = 1.10001 * 1000000

        exit_signal = self.agg.generate_exit_order_signal(
            event=event,
            account=1,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertEqual(exit_signal, [])

    def test4_aggressive_tp_for_position_short_triggered(self):

        self.agg.stoploss_limit = 2
        self.agg.takeprofit_limit = 3

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        position.net_position = -100
        event = Event(order_book_id=123, timestamp=1)
        avg_price = 1.10003 * 1000000
        price_tick = 1.10000 * 1000000
        account = 1

        exit_signal = self.agg.generate_exit_order_signal(
            event=event,
            account=account,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertIsInstance(exit_signal, List)
        self.assertEqual(exit_signal.__len__(), 1)

        order = exit_signal[0]
        self.assertEqual(order.timestamp, event.timestamp)
        self.assertEqual(order.order_book_id, event.order_book_id)
        self.assertEqual(order.account_id, account)
        self.assertEqual(order.time_in_force, "K")
        self.assertEqual(order.order_type, "R")
        self.assertEqual(order.signal, "TP_close_position")
        self.assertEqual(order.event_type, "hedge")
        self.assertEqual(order.order_qty, 100)
        self.assertEqual(order.unfilled_qty, 100)

    def test5_aggressive_sl_for_position_long_not_triggered(self):

        self.agg.stoploss_limit = 2
        self.agg.takeprofit_limit = 3

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        position.net_position = 100
        event = Event(order_book_id=123, timestamp=1)
        avg_price = 1.10001 * 1000000
        price_tick = 1.10003 * 1000000

        exit_signal = self.agg.generate_exit_order_signal(
            event=event,
            account=1,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertEqual(exit_signal, [])

    def test6_aggressive_sl_for_position_long_triggered(self):

        self.agg.stoploss_limit = 2
        self.agg.takeprofit_limit = 3

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        position.net_position = 100
        event = Event(order_book_id=123, timestamp=1)
        avg_price = 1.10003 * 1000000
        price_tick = 1.10001 * 1000000
        account = 1

        exit_signal = self.agg.generate_exit_order_signal(
            event=event,
            account=account,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertIsInstance(exit_signal, List)
        self.assertEqual(exit_signal.__len__(), 1)

        order = exit_signal[0]
        self.assertEqual(order.timestamp, event.timestamp)
        self.assertEqual(order.order_book_id, event.order_book_id)
        self.assertEqual(order.account_id, account)
        self.assertEqual(order.time_in_force, "K")
        self.assertEqual(order.order_type, "S")
        self.assertEqual(order.signal, "SL_close_position")
        self.assertEqual(order.event_type, "hedge")
        self.assertEqual(order.order_qty, -100)
        self.assertEqual(order.unfilled_qty, -100)

    def test7_aggressive_sl_for_position_short_not_triggered(self):

        self.agg.stoploss_limit = 2
        self.agg.takeprofit_limit = 3

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        position.net_position = -100
        event = Event(order_book_id=123, timestamp=1)
        avg_price = 1.10001 * 1000000
        price_tick = 1.10002 * 1000000

        exit_signal = self.agg.generate_exit_order_signal(
            event=event,
            account=1,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertEqual(exit_signal, [])

    def test8_aggressive_sl_for_position_short_triggered(self):

        self.agg.stoploss_limit = 2
        self.agg.takeprofit_limit = 3

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = -100
        event = Event(order_book_id=123, timestamp=1)
        avg_price = 1.10001 * 1000000
        price_tick = 1.10003 * 1000000
        account = 1

        exit_signal = self.agg.generate_exit_order_signal(
            event=event,
            account=account,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertIsInstance(exit_signal, List)
        self.assertEqual(exit_signal.__len__(), 1)

        order = exit_signal[0]
        self.assertEqual(order.timestamp, event.timestamp)
        self.assertEqual(order.order_book_id, event.order_book_id)
        self.assertEqual(order.account_id, account)
        self.assertEqual(order.time_in_force, "K")
        self.assertEqual(order.order_type, "S")
        self.assertEqual(order.signal, "SL_close_position")
        self.assertEqual(order.event_type, "hedge")
        self.assertEqual(order.order_qty, 100)
        self.assertEqual(order.unfilled_qty, 100)


if __name__ == "__main__":
    unittest.main()
