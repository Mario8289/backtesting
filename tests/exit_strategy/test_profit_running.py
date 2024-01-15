import unittest
from typing import List

from risk_backtesting.exit_strategy.profit_running import ProfitRunning
from risk_backtesting.position import Position
from risk_backtesting.event import Event


class ProfitRunningSignals(unittest.TestCase):
    def setUp(self):
        min_trade_size = 10
        cut_ratio = 0.5
        takeprofit_limit = 3
        stoploss_limit = 2
        self.exit = ProfitRunning(
            cut_ratio=cut_ratio,
            min_trade_size=min_trade_size,
            stoploss_limit=stoploss_limit,
            takeprofit_limit=takeprofit_limit,
        )

    def test_aggressive_tp_for_position_long_not_triggered(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        trade_tick = 1.10001 * 1000000
        next_tick = 1.10003 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(100, trade_tick, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=next_tick, position=position
        )

        self.assertEqual(exit_signal, [])

    def test_aggressive_tp_for_position_long_triggered(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        avg_price = 1.10001 * 1000000
        price_tick = 1.10004 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(100, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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
        self.assertEqual(order.order_qty, -50)
        self.assertEqual(order.unfilled_qty, -50)

    def test_aggressive_tp_for_position_long_triggered_min_trade_size_breached_use_net(
            self,
    ):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.exit.min_trade_size = 20
        self.exit.cut_ratio = 0.5
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        avg_price = 1.10001 * 1000000
        price_tick = 1.10004 * 1000000

        position.on_trade(10, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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
        self.assertEqual(order.order_qty, -10)
        self.assertEqual(order.unfilled_qty, -10)

    def test_aggressive_tp_for_position_long_triggered_min_trade_size_breached_use_min_trade_size(
            self,
    ):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.exit.min_trade_size = 20
        self.exit.cut_ratio = 0.5

        avg_price = 1.10001 * 1000000
        price_tick = 1.10004 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(35, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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
        self.assertEqual(order.order_qty, -20)
        self.assertEqual(order.unfilled_qty, -20)

    def test_aggressive_tp_for_position_long_triggered_no_trades_under_1(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.exit.min_trade_size = 10
        self.exit.cut_ratio = 0.5

        avg_price = 1.10001 * 1000000
        price_tick = 1.10004 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(13, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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
        self.assertEqual(order.order_qty, -10)
        self.assertEqual(order.unfilled_qty, -10)

    def test_aggressive_tp_for_position_short_not_triggered(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.exit.takeprofit_limit = 5
        self.exit.stoploss_limit = 2
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        avg_price = 1.10003 * 1000000
        price_tick = 1.10001 * 1000000

        position.on_trade(-100, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
        )

        self.assertEqual(exit_signal, [])

    def test_aggressive_tp_for_position_short_triggered(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        avg_price = 1.10003 * 1000000
        price_tick = 1.10000 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(-100, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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
        self.assertEqual(order.order_qty, 50)
        self.assertEqual(order.unfilled_qty, 50)

    def test_aggressive_tp_for_position_short_triggered_min_trade_size_breached_use_net(
            self,
    ):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.exit.min_trade_size = 20
        self.exit.cut_ratio = 0.5
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        avg_price = 1.10004 * 1000000
        price_tick = 1.10001 * 1000000

        position.on_trade(-10, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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
        self.assertEqual(order.order_qty, 10)
        self.assertEqual(order.unfilled_qty, 10)

    def test_aggressive_tp_for_position_short_triggered_min_trade_size_breached_use_min_trade_size(
            self,
    ):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.exit.min_trade_size = 20
        self.exit.cut_ratio = 0.5

        avg_price = 1.10004 * 1000000
        price_tick = 1.10001 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(-35, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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
        self.assertEqual(order.order_qty, 20)
        self.assertEqual(order.unfilled_qty, 20)

    def test_aggressive_tp_for_position_short_triggered_no_trades_under_1(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        self.exit.min_trade_size = 10
        self.exit.cut_ratio = 0.5

        avg_price = 1.10004 * 1000000
        price_tick = 1.10001 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(-13, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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
        self.assertEqual(order.order_qty, 10)
        self.assertEqual(order.unfilled_qty, 10)

    def test_aggressive_sl_for_position_long_not_triggered(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        avg_price = 1.10001 * 1000000
        price_tick = 1.10003 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(100, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
        )

        self.assertEqual(exit_signal, [])

    def test_aggressive_sl_for_position_long_triggered(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        avg_price = 1.10003 * 1000000
        price_tick = 1.10001 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(100, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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

    def test_aggressive_sl_for_position_short_not_triggered(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        avg_price = 1.10001 * 1000000
        price_tick = 1.10002 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(-100, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
        )

        self.assertEqual(exit_signal, [])

    def test_aggressive_sl_for_position_short_triggered(self):

        position = Position(
            name="EUR/MMA",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
            currency="USD",
        )

        avg_price = 1.10001 * 1000000
        price_tick = 1.10003 * 1000000
        event = Event(order_book_id=123, timestamp=1, symbol="EUR/MMA")
        account = 1

        position.on_trade(-100, avg_price, rate_to_usd=1)

        exit_signal = self.exit.generate_exit_order_signal(
            event=event, account=account, tick_price=price_tick, position=position
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

    # test profit running scenarios accumulated PnL

    # def test_long_position_1_tp(self):
    #
    #     account = 1
    #
    #     position = Position(
    #         name='EUR/MMA',
    #         unit_price=10000,
    #         price_increment=0.00001,
    #         netting_engine='fifo',
    #         currency='USD')
    #
    #     events = pd.DataFrame([
    #         [1, 'trade',       123, 0.99999, 1.00001, 1, 1.00001,  0],
    #         [2, 'market_data', 123, 1.00000, 1.00002, 0, 1.00001,  0],
    #         [3, 'trade',       123, 1.00000, 1.00002, 1, 1.000015, 0],
    #         [4, 'trade',       123, 1.00001, 1.00003, 1, 1.00002,  0],
    #         [5, 'market_data', 123, 1.00003, 1.00005, 0, 1.00002,  0],
    #         [6, 'market_data', 123, 1.00005, 1.00007, 0, 1.00002,  0.555],  # TP reached
    #         [7, 'market_data', 123, 1.00007, 1.00009, 0, 1.00005,  0.555]
    #
    #     ], columns=['timestamp', 'type', 'order_book_id', 'bid', 'ask', 'contract_qty', 'running_price', 'rpnl'])
    #
    #     signals = [
    #         [], [], [], [], [],
    #         [Order(timestamp=6, order_book_id=123, order_qty=-150,
    #                order_type='R', time_in_force='K', account_id=account,
    #                event_type='hedge', signal='TP_close_position', price=100005)],
    #         []
    #     ]
    #
    #     for idx, evt in events.fillna(0).iterrows():
    #         event = Event(timestamp=evt.timestamp,
    #                       contract_qty=evt.contract_qty * 100,
    #                       bid_price=evt.bid * 1000000,
    #                       ask_price=evt.ask * 1000000,
    #                       order_book_id=evt.order_book_id,
    #                       unit_price=10000)
    #         if evt.type == 'trade':
    #             position.on_trade(event.contract_qty, event.ask_price, rate_to_usd=1)
    #         else:
    #             orders = self.exit.generate_exit_order_signal(
    #                 event=event,
    #                 account=account,
    #                 tick_price=event.bid_price,
    #                 position=position
    #             )
    #             self.assertIsInstance(orders, List)
    #             if orders:
    #                 self.assertEqual(orders.__len__(), 1)
    #
    #                 order = orders[0]
    #                 order_check = signals[idx][0]
    #
    #                 self.assertEqual(order.timestamp, order_check.timestamp)
    #                 self.assertEqual(order.order_book_id, order_check.order_book_id)
    #                 self.assertEqual(order.account_id, order_check.account_id)
    #                 self.assertEqual(order.time_in_force, order_check.time_in_force)
    #                 self.assertEqual(order.order_type, order_check.order_type)
    #                 self.assertEqual(order.signal, order_check.signal)
    #                 self.assertEqual(order.event_type, order_check.event_type)
    #                 self.assertEqual(order.order_qty, order_check.order_qty)
    #                 self.assertEqual(order.unfilled_qty, order_check.unfilled_qty)
    #             else:
    #                 self.assertEqual(signals[idx], orders)
    #
    #             self.assertEqual(position.exit_attr['running_price'], int(round(evt.running_price * 1000000)))
    #
    #             try:
    #                 if orders:
    #                     for order in orders:
    #                         position.on_trade(order.order_qty,
    #                                           event.bid_price,
    #                                           rate_to_usd=1)
    #             except KeyError:
    #                 pass
    #             # validate that the correct PnL was calculated
    #             self.assertEqual(position.realised_pnl, evt.rpnl)
    #
    # def test_long_position_2_tp(self):
    #     account = 1
    #
    #     position = Position(
    #         name='EUR/MMA',
    #         unit_price=10000,
    #         price_increment=0.00001,
    #         netting_engine='fifo',
    #         currency='USD')
    #
    #     events = pd.DataFrame([
    #         [1, 'trade',        123, 0.99999, 1.00001, 1, 1.00001,  0],
    #         [2, 'market_data', 123, 1.00000, 1.00002, 0, 1.00001,  0],
    #         [3, 'trade',       123, 1.00000, 1.00002, 1, 1.000015, 0],
    #         [4, 'trade',       123, 1.00001, 1.00003, 1, 1.00002,  0],
    #         [5, 'market_data', 123, 1.00003, 1.00005, 0, 1.00002,  0],
    #         [6, 'market_data', 123, 1.00005, 1.00007, 0, 1.00002,  0.555],  # TP reached
    #         [7, 'market_data', 123, 1.00007, 1.00009, 0, 1.00005,  0.555],
    #         [8, 'market_data', 123, 1.00008, 1.00010, 0, 1.00005,  0.985]
    #     ], columns=['timestamp', 'type', 'order_book_id', 'bid', 'ask', 'contract_qty', 'running_price', 'rpnl'])
    #
    #     signals = [
    #         [], [], [], [], [],
    #         [Order(timestamp=6, order_book_id=123, order_qty=-150,
    #                order_type='R', time_in_force='K', account_id=account,
    #                event_type='hedge', signal='TP_close_position', price=100005)],
    #         [],
    #         [Order(timestamp=8, order_book_id=123, order_qty=-75,
    #                order_type='R', time_in_force='K', account_id=account,
    #                event_type='hedge', signal='TP_close_position', price=100005)]
    #     ]
    #
    #     for idx, evt in events.fillna(0).iterrows():
    #         event = Event(timestamp=evt.timestamp,
    #                       contract_qty=evt.contract_qty * 100,
    #                       bid_price=evt.bid * 1000000,
    #                       ask_price=evt.ask * 1000000,
    #                       order_book_id=evt.order_book_id,
    #                       unit_price=10000)
    #
    #         if evt.type == 'trade':
    #             position.on_trade(event.contract_qty, event.ask_price, rate_to_usd=1)
    #         else:
    #             orders = self.exit.generate_exit_order_signal(
    #                 event=event,
    #                 account=account,
    #                 tick_price=event.bid_price,
    #                 position=position
    #             )
    #             self.assertIsInstance(orders, List)
    #             if orders:
    #                 self.assertEqual(orders.__len__(), 1)
    #
    #                 order = orders[0]
    #                 order_check = signals[idx][0]
    #
    #                 self.assertEqual(order.timestamp, order_check.timestamp)
    #                 self.assertEqual(order.order_book_id, order_check.order_book_id)
    #                 self.assertEqual(order.account_id, order_check.account_id)
    #                 self.assertEqual(order.time_in_force, order_check.time_in_force)
    #                 self.assertEqual(order.order_type, order_check.order_type)
    #                 self.assertEqual(order.signal, order_check.signal)
    #                 self.assertEqual(order.event_type, order_check.event_type)
    #                 self.assertEqual(order.order_qty, order_check.order_qty)
    #                 self.assertEqual(order.unfilled_qty, order_check.unfilled_qty)
    #
    #             else:
    #                 self.assertEqual(signals[idx], orders)
    #
    #             self.assertEqual(position.exit_attr['running_price'], int(round(evt.running_price * 1000000)))
    #
    #             try:
    #                 if orders:
    #                     for order in orders:
    #                         position.on_trade(order.order_qty,
    #                                           event.bid_price,
    #                                           rate_to_usd=1)
    #             except KeyError:
    #                 pass
    #             # validate that the correct PnL was calculated
    #             self.assertEqual(round(position.realised_pnl, 3), evt.rpnl)
    #
    # def test_long_position_2_tp_extend_position_in_between(self):
    #
    #     account = 1
    #
    #     position = Position(
    #         name='EUR/MMA',
    #         unit_price=10000,
    #         price_increment=0.00001,
    #         netting_engine='fifo',
    #         currency='USD')
    #
    #     events = pd.DataFrame([
    #         [1, 'trade',       0.99999, 1.00001, 1, 1.00001,  0],
    #         [2, 'market_data', 1.00000, 1.00002, 0, 1.00001,  0],
    #         [3, 'trade',       1.00000, 1.00002, 1, 1.000015, 0],
    #         [4, 'trade',       1.00001, 1.00003, 1, 1.00002,  0],
    #         [5, 'market_data', 1.00003, 1.00005, 0, 1.00002,  0],
    #         [6, 'market_data', 1.00005, 1.00007, 0, 1.00002,  0.55],  # TP reached
    #         ['market_data', 1.00006, 1.00008, 1, 1.00005, 0.55],
    #         ['trade',       1.00006, 1.00008, 1, 1.00005, 0.55],
    #         ['market_data', 1.00007, 1.00009, 0, 1.000062,  0.55],
    #         ['market_data', 1.00010, 1.00010, 0, 1.000062,  1.475]
    #
    #     ], columns=['type', 'bid', 'ask', 'contract_qty', 'running_price', 'rpnl'])
    #
    #     signals = [
    #         [], [], [], [], [],
    #         {'TP_close_position':
    #             {'name': 'TP_close_position',
    #              'expression': True,
    #              'order_type': 'market_order',
    #              'event_type': 'hedge',
    #              'quantity': -150}},
    #         [], [], [],
    #         {'TP_close_position':
    #              {'name': 'TP_close_position',
    #               'expression': True,
    #               'order_type': 'market_order',
    #               'event_type': 'hedge',
    #               'quantity': -125}}
    #     ]
    #
    #     for idx, event in events.fillna(0).iterrows():
    #         if event.type == 'trade':
    #             position.on_trade(event.contract_qty * 100, round(event.ask * 1000000), rate_to_usd=1)
    #         else:
    #             exit_signal = self.exit.generate_exit_order_signal(
    #                 tick_price=int(round(event.bid * 1000000)),
    #                 position=position
    #             )
    #
    #             self.assertEqual(signals[idx], exit_signal)
    #             self.assertEqual(position.running_price, int(round(event.running_price * 1000000)))
    #
    #             try:
    #                 if exit_signal['TP_close_position']['expression']:
    #                     position.on_trade(exit_signal['TP_close_position']['quantity'], round(event.bid * 1000000), rate_to_usd=1)
    #             except KeyError:
    #                 pass
    #
    #             # validate that the correct PnL was calculated
    #             self.assertAlmostEqual(position.realised_pnl, event.rpnl)
    #
    # def test_long_position_2_tp_closed_by_min_trade_size(self):
    #
    #     self.exit.min_trade_size = 150
    #
    #     position = Position(
    #         name='EUR/MMA',
    #         unit_price=10000,
    #         price_increment=0.00001,
    #         netting_engine='fifo',
    #         currency='USD')
    #
    #     events = pd.DataFrame([
    #         ['trade',       0.99999, 1.00001, 1, 1.00001,  0],
    #         ['market_data', 1.00000, 1.00002, 0, 1.00001,  0],
    #         ['trade',       1.00000, 1.00002, 1, 1.000015, 0],
    #         ['trade',       1.00001, 1.00003, 1, 1.00002,  0],
    #         ['market_data', 1.00003, 1.00005, 0, 1.00002,  0],
    #         ['market_data', 1.00005, 1.00007, 0, 1.00002,  0.55],  # TP reached
    #         ['market_data', 1.00007, 1.00009, 0, 1.00005,  0.55],
    #         ['market_data', 1.00008, 1.00010, 0, 1.00005,  1.35]
    #
    #     ], columns=['type', 'bid', 'ask', 'contract_qty', 'running_price', 'rpnl'])
    #
    #     signals = [
    #         [], [], [], [], [],
    #         {'TP_close_position':
    #             {'name': 'TP_close_position',
    #              'expression': True,
    #              'order_type': 'market_order',
    #              'event_type': 'hedge',
    #              'quantity': -150}},
    #         [],
    #         {'TP_close_position':
    #              {'name': 'TP_close_position',
    #               'expression': True,
    #               'order_type': 'market_order',
    #               'event_type': 'hedge',
    #               'quantity': -150}}
    #     ]
    #
    #     for idx, event in events.fillna(0).iterrows():
    #         if event.type == 'trade':
    #             position.on_trade(event.contract_qty * 100, round(event.ask * 1000000), rate_to_usd=1)
    #         else:
    #             exit_signal = self.exit.generate_exit_order_signal(
    #                 tick_price=int(round(event.bid * 1000000)),
    #                 position=position
    #             )
    #
    #             self.assertEqual(signals[idx], exit_signal)
    #             self.assertEqual(position.running_price, int(round(event.running_price * 1000000)))
    #
    #             try:
    #                 if exit_signal['TP_close_position']['expression']:
    #                     position.on_trade(exit_signal['TP_close_position']['quantity'], round(event.bid * 1000000), rate_to_usd=1)
    #             except KeyError:
    #                 pass
    #
    #             # validate that the correct PnL was calculated
    #             self.assertAlmostEqual(position.realised_pnl, event.rpnl)
    #
    # def test_long_position_1_sl(self):
    #
    #     position = Position(
    #         name='EUR/MMA',
    #         unit_price=10000,
    #         price_increment=0.00001,
    #         netting_engine='fifo',
    #         currency='USD')
    #
    #     events = pd.DataFrame([
    #         ['trade',       0.99999, 1.00001, 1, 1.00001,  0],
    #         ['market_data', 1.00000, 1.00002, 0, 1.00001,  0],
    #         ['trade',       1.00000, 1.00002, 1, 1.000015, 0],
    #         ['trade',       1.00001, 1.00003, 1, 1.00002,  0],
    #         ['market_data', 1.00001, 1.00003, 0, 1.00002,  0],
    #         ['market_data', 1.00000, 1.00002, 0, 1.00002,  -0.6],  # SL reached
    #         ['market_data', 1.00001, 1.00003, 0, 1.00002,  -0.6]
    #
    #     ], columns=['type', 'bid', 'ask', 'contract_qty', 'running_price', 'rpnl'])
    #
    #     signals = [
    #         [], [], [], [], [],
    #         {'SL_close_position':
    #             {'name': 'SL_close_position',
    #              'expression': True,
    #              'order_type': 'market_order',
    #              'event_type': 'hedge',
    #              'quantity': -300}},
    #         []
    #     ]
    #
    #     for idx, event in events.fillna(0).iterrows():
    #         if event.type == 'trade':
    #             position.on_trade(event.contract_qty * 100, round(event.ask * 1000000), rate_to_usd=1)
    #         else:
    #             exit_signal = self.exit.generate_exit_order_signal(
    #                 tick_price=int(round(event.bid * 1000000)),
    #                 position=position
    #             )
    #
    #             self.assertEqual(signals[idx], exit_signal)
    #             self.assertEqual(position.running_price, int(round(event.running_price * 1000000)))
    #
    #             try:
    #                 if exit_signal['SL_close_position']['expression']:
    #                     position.on_trade(exit_signal['SL_close_position']['quantity'], round(event.bid * 1000000), rate_to_usd=1)
    #             except KeyError:
    #                 pass
    #
    #             # validate that the correct PnL was calculated
    #             self.assertAlmostEqual(position.realised_pnl, event.rpnl)
    #
    # def test_long_position_1_tp_1_sl(self):
    #
    #     self.exit.min_trade_size = 1
    #
    #     position = Position(
    #         name='EUR/MMA',
    #         unit_price=10000,
    #         price_increment=0.00001,
    #         netting_engine='fifo',
    #         currency='USD')
    #
    #     events = pd.DataFrame([
    #         ['trade',       0.99999, 1.00001, 1, 1.00001,  0],
    #         ['market_data', 1.00000, 1.00002, 0, 1.00001,  0],
    #         ['trade',       1.00000, 1.00002, 1, 1.000015, 0],
    #         ['trade',       1.00001, 1.00003, 1, 1.00002,  0],
    #         ['market_data', 1.00003, 1.00005, 0, 1.00002,  0],
    #         ['market_data', 1.00005, 1.00007, 0, 1.00002,  0.55],  # TP reached
    #         ['market_data', 1.00004, 1.00006, 0, 1.00005,  0.55],
    #         ['market_data', 1.00003, 1.00005, 0, 1.00005,  0.6]
    #
    #     ], columns=['type', 'bid', 'ask', 'contract_qty', 'running_price', 'rpnl'])
    #
    #     signals = [
    #         [], [], [], [], [],
    #         {'TP_close_position':
    #             {'name': 'TP_close_position',
    #              'expression': True,
    #              'order_type': 'market_order',
    #              'event_type': 'hedge',
    #              'quantity': -150}},
    #         [],
    #         {'SL_close_position':
    #              {'name': 'SL_close_position',
    #               'expression': True,
    #               'order_type': 'market_order',
    #               'event_type': 'hedge',
    #               'quantity': -150}}
    #     ]
    #
    #     for idx, event in events.fillna(0).iterrows():
    #         if event.type == 'trade':
    #             position.on_trade(event.contract_qty * 100, round(event.ask * 1000000), rate_to_usd=1)
    #         else:
    #             exit_signal = self.exit.generate_exit_order_signal(
    #                 tick_price=int(round(event.bid * 1000000)),
    #                 position=position
    #             )
    #
    #             self.assertEqual(signals[idx], exit_signal)
    #             self.assertEqual(position.running_price, int(round(event.running_price * 1000000)))
    #
    #             for signal in ['TP_close_position', 'SL_close_position']:
    #                 try:
    #                     if exit_signal[signal]['expression']:
    #                         position.on_trade(exit_signal[signal]['quantity'], round(event.bid * 1000000), rate_to_usd=1)
    #
    #                 except KeyError:
    #                     pass
    #
    #             # validate that the correct PnL was calculated
    #             self.assertAlmostEqual(position.realised_pnl, event.rpnl)


if __name__ == "__main__":
    unittest.main()
