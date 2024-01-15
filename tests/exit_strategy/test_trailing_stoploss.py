import unittest
from typing import List

from risk_backtesting.exit_strategy.trailing_stoploss import TrailingStopLoss
from risk_backtesting.position import Position
from risk_backtesting.event import Event
from risk_backtesting.order import Order


class TrailingStopLossSignals(unittest.TestCase):
    def setUp(self):
        stoploss_limit = 2
        self.tsl = TrailingStopLoss(stoploss_limit=stoploss_limit)

    def test_trailing_stoploss_set_first_trailing_when_moves_into_profit_long(self):
        account = 1

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = 100

        avg_price = 1.10001 * 1000000
        price_tick = 1.10002 * 1000000
        event = Event(order_book_id=123, symbol="EUR/MMA")

        self.assertFalse("trailing_stoplosprice" in position.exit_attr.keys())
        self.assertFalse("last_tick_peak" in position.exit_attr.keys())
        self.assertFalse("last_tick_price" in position.exit_attr.keys())

        orders = self.tsl.generate_exit_order_signal(
            event=event,
            account=account,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertEqual(position.exit_attr["trailing_stoploss"], 1100000)
        self.assertEqual(position.exit_attr["tick_peak"], 1100020)
        self.assertEqual(position.exit_attr["last_tick_price"], 1100020)
        self.assertEqual(orders, [])

    def test_trailing_stoploss_set_first_trailing_when_moves_into_profit_short(self):
        account = 1

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = -100

        avg_price = 1.10001 * 1000000
        price_tick = 1.10000 * 1000000
        event = Event(order_book_id=123, symbol="EUR/MMA")

        self.assertFalse(hasattr(position, "trailing_stoplosprice"))
        self.assertFalse(hasattr(position, "last_tick_peak"))
        self.assertFalse(hasattr(position, "last_tick_price"))

        orders = self.tsl.generate_exit_order_signal(
            event=event,
            account=account,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertEqual(position.exit_attr["trailing_stoploss"], 1100020)
        self.assertEqual(position.exit_attr["tick_peak"], 1100000)
        self.assertEqual(position.exit_attr["last_tick_price"], 1100000)
        self.assertEqual(orders, [])

    def test_trailing_stoploss_set_first_trailing_when_moves_into_loss_long(self):
        account = 1

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = 100

        avg_price = 1.10001 * 1000000
        price_tick = 1.10000 * 1000000
        event = Event(order_book_id=123, symbol="EUR/MMA")

        self.assertFalse(hasattr(position, "trailing_stoplosprice"))
        self.assertFalse(hasattr(position, "last_tick_peak"))
        self.assertFalse(hasattr(position, "last_tick_price"))

        exit_signal = self.tsl.generate_exit_order_signal(
            event=event,
            account=account,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertEqual(position.exit_attr["trailing_stoploss"], 1099990)
        self.assertEqual(position.exit_attr["tick_peak"], 1100010)
        self.assertEqual(position.exit_attr["last_tick_price"], 1100000)
        self.assertEqual(exit_signal, [])

    def test_trailing_stoploss_set_first_trailing_when_moves_into_loss_short(self):
        account = 1

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = -100

        avg_price = 1.10001 * 1000000
        price_tick = 1.10002 * 1000000
        event = Event(order_book_id=123, symbol="EUR/MMA")

        self.assertFalse("trailing_stoploss" in position.exit_attr.keys())
        self.assertFalse("last_tick_peak" in position.exit_attr.keys())
        self.assertFalse("last_tick_price" in position.exit_attr.keys())

        exit_signal = self.tsl.generate_exit_order_signal(
            event=event,
            account=account,
            avg_price=avg_price,
            tick_price=price_tick,
            position=position,
        )

        self.assertEqual(position.exit_attr["trailing_stoploss"], 1100030)
        self.assertEqual(position.exit_attr["tick_peak"], 1100010)
        self.assertEqual(position.exit_attr["last_tick_price"], 1100020)
        self.assertEqual(exit_signal, [])

    def test_trailing_long_stoploss_set_new_peak_set_and_trailing_stoploss(self):
        account = 1
        self.tsl.stoploss_limit = 4

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = 100

        avg_price = 1.10001 * 1000000
        price_tick = [
            x * 1000000 for x in [1.10002, 1.10001, 1.10000, 1.10001, 1.10002, 1.10003]
        ]
        array_of_peaks = [
            x * 1000000 for x in [1.10002, 1.10002, 1.10002, 1.10002, 1.10002, 1.10003]
        ]
        array_of_trailing_stoploss = [
            x * 1000000 for x in [1.09998, 1.09998, 1.09998, 1.09998, 1.09998, 1.09999]
        ]
        event = Event(order_book_id=123, symbol="EUR/MMA")

        for (tick, peak, sl) in zip(
                price_tick, array_of_peaks, array_of_trailing_stoploss
        ):

            orders = self.tsl.generate_exit_order_signal(
                event=event,
                account=account,
                avg_price=avg_price,
                tick_price=tick,
                position=position,
            )

            self.assertEqual(position.exit_attr["trailing_stoploss"], sl)
            self.assertEqual(position.exit_attr["tick_peak"], peak)
            self.assertEqual(orders, [])

    def test_trailing_short_stoploss_set_new_peak_set_and_trailing_stoploss(self):
        account = 1
        self.tsl.stoploss_limit = 4

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = -100

        avg_price = 1.10001 * 1000000
        price_tick = [
            x * 1000000 for x in [1.10002, 1.10003, 1.10004, 1.10002, 1.10001, 1.10000]
        ]
        array_of_peaks = [
            x * 1000000 for x in [1.10001, 1.10001, 1.10001, 1.10001, 1.10001, 1.10000]
        ]
        array_of_trailing_stoploss = [
            x * 1000000 for x in [1.10005, 1.10005, 1.10005, 1.10005, 1.10005, 1.10004]
        ]
        event = Event(order_book_id=123, symbol="EUR/MMA")

        for (tick, peak, sl) in zip(
                price_tick, array_of_peaks, array_of_trailing_stoploss
        ):

            exit_signal = self.tsl.generate_exit_order_signal(
                event=event,
                account=account,
                avg_price=avg_price,
                tick_price=tick,
                position=position,
            )

            self.assertEqual(position.exit_attr["trailing_stoploss"], sl)
            self.assertEqual(position.exit_attr["tick_peak"], peak)
            self.assertEqual(exit_signal, [])

    def test_trailing_long_stoploss_set_new_peak_set_and_trailing_stoploss_triggered(
            self,
    ):
        account = 1
        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = 100

        avg_price = 1.10001 * 1000000
        price_tick = [x * 1000000 for x in [1.10002, 1.10001, 1.10000]]
        array_of_peaks = [x * 1000000 for x in [1.10002, 1.10002, 1.10002]]
        array_of_trailing_stoploss = [x * 1000000 for x in [1.10000, 1.10000, 1.10000]]
        event = Event(timestamp=3, order_book_id=123, symbol="EUR/MMA")

        array_of_signals = [[]] * 2 + [
            Order(
                timestamp=3,
                order_book_id=123,
                account_id=account,
                time_in_force="K",
                event_type="hedge",
                order_type="S",
                order_qty=position.net_position * -1,
                signal="SL_close_position",
            )
        ]

        for (tick, peak, sl, signal) in zip(
                price_tick, array_of_peaks, array_of_trailing_stoploss, array_of_signals
        ):
            orders = self.tsl.generate_exit_order_signal(
                event=event,
                account=account,
                avg_price=avg_price,
                tick_price=tick,
                position=position,
            )

            self.assertEqual(position.exit_attr["trailing_stoploss"], sl)
            self.assertEqual(position.exit_attr["tick_peak"], peak)

            self.assertIsInstance(orders, List)
            if orders:

                self.assertEqual(orders.__len__(), 1)

                order = orders[0]
                order_check = signal

                self.assertEqual(order.timestamp, order_check.timestamp)
                self.assertEqual(order.order_book_id, order_check.order_book_id)
                self.assertEqual(order.account_id, order_check.account_id)
                self.assertEqual(order.time_in_force, order_check.time_in_force)
                self.assertEqual(order.order_type, order_check.order_type)
                self.assertEqual(order.signal, order_check.signal)
                self.assertEqual(order.event_type, order_check.event_type)
                self.assertEqual(order.order_qty, order_check.order_qty)
                self.assertEqual(order.unfilled_qty, order_check.unfilled_qty)
            else:
                self.assertEqual(orders, signal)

    def test_trailing_short_stoploss_set_new_peak_set_and_trailing_stoploss_triggered(
            self,
    ):
        account = 1

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )
        position.net_position = -100

        avg_price = 1.10004 * 1000000
        price_tick = [x * 1000000 for x in [1.10003, 1.10004, 1.10005]]
        array_of_peaks = [x * 1000000 for x in [1.10003, 1.10003, 1.10003]]
        array_of_trailing_stoploss = [x * 1000000 for x in [1.10005, 1.10005, 1.10005]]
        array_of_signals = [[]] * 2 + [
            Order(
                timestamp=3,
                order_book_id=123,
                account_id=account,
                time_in_force="K",
                event_type="hedge",
                order_type="S",
                order_qty=position.net_position * -1,
                signal="SL_close_position",
            )
        ]

        event = Event(timestamp=3, order_book_id=123, symbol="EUR/MMA")

        for (tick, peak, sl, signal) in zip(
                price_tick, array_of_peaks, array_of_trailing_stoploss, array_of_signals
        ):
            orders = self.tsl.generate_exit_order_signal(
                event=event,
                account=account,
                avg_price=avg_price,
                tick_price=tick,
                position=position,
            )

            self.assertEqual(position.exit_attr["trailing_stoploss"], sl)
            self.assertEqual(position.exit_attr["tick_peak"], peak)

            self.assertIsInstance(orders, List)
            if orders:

                self.assertEqual(orders.__len__(), 1)

                order = orders[0]
                order_check = signal

                self.assertEqual(order.timestamp, order_check.timestamp)
                self.assertEqual(order.order_book_id, order_check.order_book_id)
                self.assertEqual(order.account_id, order_check.account_id)
                self.assertEqual(order.time_in_force, order_check.time_in_force)
                self.assertEqual(order.order_type, order_check.order_type)
                self.assertEqual(order.signal, order_check.signal)
                self.assertEqual(order.event_type, order_check.event_type)
                self.assertEqual(order.order_qty, order_check.order_qty)
                self.assertEqual(order.unfilled_qty, order_check.unfilled_qty)
            else:
                self.assertEqual(orders, signal)


if __name__ == "__main__":
    unittest.main()
