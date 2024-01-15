# import unittest
# import datetime
# import pandas as pd
# import pytz
# from risk_backtesting.client_profiling import ClientProfile
# from risk_backtesting.eventloader import EventLoader
# from risk_backtesting.client_profiling import Position, OpenPosition
#
# nytime = pytz.timezone('America/New_York')
# utctime = pytz.timezone('UTC')
#
#
# def generate_opening_positions(account, symbols, names, date, venue=1):
#     _date = datetime.datetime.strptime(date, '%Y-%m-%d')
#     _nyts = nytime.localize(datetime.datetime(_date.year, _date.month,
#                                            _date.day, 10))
#     _utcts = _nyts.astimezone(utctime)
#     contract_qty = 1 * 100
#     price = 1 * 1000000
#
#     open_positions = dict()
#     x = 10
#     for symbol, name in zip(symbols, names):
#         pos = Position(name, 10000, 0.00001)
#         open_positions[venue, symbol, account] = pos
#         new_position = OpenPosition(contract_qty, price, _utcts, x)
#         pos.open_positions.append(new_position)
#         x += 1
#     return open_positions
#
#
# class ClientProfiling(unittest.TestCase):
#
#     def setUp(self):
#         _accounts = [1, 2, 3, 4]
#         self.end_date = '2018-07-12'
#         self.symbols = [101, 102]
#         self.symbol_names = ['fx1', 'fx2']
#         self.start_date = '2018-07-11'
#
#         self.symbol_details = {
#             'order_book_id': [101, 102],
#             'unit_price': [10000] * 2,
#             'effective_price_increment': [0.00001] * 2,
#             'name': ['fx1', 'fx2']
#         }
#
#         op = generate_opening_positions(_accounts[2],
#                                         self.symbol_details['order_book_id'],
#                                         self.symbol_details['name'],
#                                         self.start_date)
#
#         self.cp = ClientProfile(_accounts, EventLoader,
#                                 self.start_date, self.end_date)
#
#         self.cp.client_portfolio.positions = op
#         self.venue = 1
#
#     def test_positions_only_stored_for_defined_accounts(self):
#         symbol = 101
#         trade_ticks = {
#             'execution_id': [1, 2, 3, 4],
#             'account_id': [1, 1, 4, 5],
#             'instrument_id': [symbol] * 4,
#             'order_id': [1, 2, 3, 4],
#             'price_decimal': [1.00000, 1.20000] * 2,
#             'quantity_decimal': [-1, 1] * 2,
#             'trade_date': ["2018-07-11"] * 4,
#             'time_stamp': [1531314000000000, 1531314060000000] * 2,
#             'mtf_price_decimal': [1.00000, 1.20000] * 2,
#             'mtf_execution_id': ["MTF1e", "MTF2e"] * 2,
#             'broker_id': [5] * 4,
#             'remote_venue': [None] * 4,
#             'mtf_order_id': ["MTF1", "MTF2"] * 2
#         }
#
#         trades = pd.DataFrame(data=trade_ticks)
#         order_book = pd.DataFrame(data=self.symbol_details)
#         trades = pd.merge(trades,
#                           order_book,
#                           left_on="instrument_id",
#                           right_on="order_book_id")
#         trades['timestamp'] = pd.to_datetime(trades['time_stamp'], unit='us',
#                                              utc=True)
#         trades.set_index('timestamp', inplace=True)
#
#         _events = self.cp.dl.generate_events(trades)
#
#         self.assertEqual(_events.shape[0], 3)
#
#     def test_duration_and_pnl_of_fully_closed_losing_trade_using_fifo(self):
#
#         symbol = 101
#
#         trade_ticks = {
#             'execution_id': [1, 2],
#             'account_id': [1] * 2,
#             'instrument_id': [symbol] * 2,
#             'order_id': [1, 2],
#             'price_decimal': [1.00000, 1.20000],
#             'quantity_decimal': [-1, 1],
#             'trade_date': ["2018-07-11", "2018-07-11"],
#             'time_stamp': [1531314000000000, 1531314060000000],
#             'mtf_price_decimal': [1.00000, 1.20000],
#             'mtf_execution_id': ["MTF1e", "MTF2e"],
#             'broker_id': [5, 5],
#             'remote_venue': [None] * 2,
#             'mtf_order_id': ["MTF1", "MTF2"]
#         }
#
#         trades = pd.DataFrame(data=trade_ticks)
#         order_book = pd.DataFrame(data=self.symbol_details)
#         trades = pd.merge(trades,
#                           order_book,
#                           left_on="instrument_id",
#                           right_on="order_book_id")
#         trades['timestamp'] = pd.to_datetime(trades['time_stamp'], unit='us',
#                                               utc=True)
#         trades.set_index('timestamp', inplace=True)
#
#         self.cp.run_day_simulation(self.start_date, trades, self.venue, symbol)
#
#         closed_positions = \
#             self.cp.client_portfolio.positions[1, symbol, 1].closed_positions
#         self.assertEqual(60.0, closed_positions[0].duration)
#         self.assertAlmostEqual(-2000, closed_positions[0].pnl)
#         self.assertAlmostEqual(-20000, closed_positions[0].pnl_pip)
#
#     def test_duration_and_pnl_of_fully_closed_winning_trade_using_fifo(self):
#         symbol = 101
#         trade_ticks = {
#             'execution_id': [1, 2],
#             'account_id': [1] * 2,
#             'instrument_id': [symbol] * 2,
#             'order_id': [1, 2],
#             'price_decimal': [1.00002, 1.00000],
#             'quantity_decimal': [-1, 1],
#             'trade_date': ["2018-07-11", "2018-07-11"],
#             'time_stamp': [1531314000000000, 1531314060000000],
#             'mtf_price_decimal': [1.00002, 1.00000],
#             'mtf_execution_id': ["MTF1e", "MTF2e"],
#             'broker_id': [5, 5],
#             'remote_venue': [None] * 2,
#             'mtf_order_id': ["MTF1", "MTF2"]
#         }
#
#         trades = pd.DataFrame(data=trade_ticks)
#         order_book = pd.DataFrame(data=self.symbol_details)
#         trades = pd.merge(trades,
#                           order_book,
#                           left_on="instrument_id",
#                           right_on="order_book_id")
#
#         trades['timestamp'] = pd.to_datetime(trades['time_stamp'], unit='us',
#                                              utc=True)
#         trades.set_index('timestamp', inplace=True)
#
#         self.cp.run_day_simulation(self.start_date, trades, self.venue, symbol)
#
#         closed_positions = \
#             self.cp.client_portfolio.positions[1, symbol, 1].closed_positions
#
#         self.assertEqual(60.0, closed_positions[0].duration)
#         self.assertAlmostEqual(.20, closed_positions[0].pnl)
#         self.assertAlmostEqual(2, closed_positions[0].pnl_pip)
#
#     def test_duration_and_pnl_of_trade_closed_by_n_trades_using_fifo(self):
#         symbol = 101
#         trade_ticks = {
#             'execution_id': [1, 2, 3, 4],
#             'account_id': [1, 2, 1, 1],
#             'instrument_id': [symbol] * 4,
#             'order_id': [1, 2, 3, 4],
#             'price_decimal': [1.00000, 1.10000, 1.20000, 1.40000],
#             'quantity_decimal': [-1, 1, 0.5, 0.5],
#             'trade_date': ["2018-07-11"] * 4,
#             'time_stamp': [1531314000000000, 1531314060000000,
#                           1531314120000000, 1531314180000000],
#             'mtf_price_decimal': [1.00000, 1.10000, 1.20000, 1.40000],
#             'mtf_execution_id': ["MTF" + str(i) + "e" for i in range(1, 5, 1)],
#             'broker_id': [5] * 4,
#             'remote_venue': [None] * 4,
#             'mtf_order_id': ["MTF" + str(i) for i in range(1, 5, 1)]
#
#         }
#
#         trades = pd.DataFrame(data=trade_ticks)
#         order_book = pd.DataFrame(data=self.symbol_details)
#         trades = pd.merge(trades,
#                           order_book,
#                           left_on="instrument_id",
#                           right_on="order_book_id")
#
#         trades['timestamp'] = pd.to_datetime(trades['time_stamp'], unit='us',
#                                              utc=True)
#         trades.set_index('timestamp', inplace=True)
#
#         self.cp.run_day_simulation(self.start_date, trades, self.venue, symbol)
#
#         closed_positions = \
#             self.cp.client_portfolio.positions[1, 101, 1].closed_positions
#
#         self.assertEqual(180.0, closed_positions[0].duration)
#         self.assertAlmostEqual(-3000, closed_positions[0].pnl)
#         self.assertAlmostEqual(-40000, closed_positions[0].pnl_pip)
#
#     def test_volitile_pnl_of_trade_closed_by_n_trades_using_fifo(self):
#         # close by 1 profit trade +50 1 lost trade - 200.
#         symbol = 101
#         trade_ticks = {
#             'execution_id': [1, 2, 3, 4],
#             'account_id': [1, 2, 1, 1],
#             'instrument_id': [symbol] * 4,
#             'order_id': [1, 2, 3, 4],
#             'price_decimal': [1, 1.1, 0.9, 1.4],
#             'quantity_decimal': [-1, 1, 0.5, 0.5],
#             'trade_date': ["2018-07-11"] * 4,
#             'time_stamp': [1531314000000000, 1531314060000000,
#                           1531314120000000, 1531314180000000],
#             'mtf_price_decimal': [1, 1.1, 0.9, 1.4],
#             'mtf_execution_id': ["MTF" + str(i) + "e" for i in range(1, 5, 1)],
#             'broker_id': [5] * 4,
#             'remote_venue': [None] * 4,
#             'mtf_order_id': ["MTF" + str(i) for i in range(1, 5, 1)]
#         }
#
#         trades = pd.DataFrame(data=trade_ticks)
#         order_book = pd.DataFrame(data=self.symbol_details)
#         trades = pd.merge(trades,
#                           order_book,
#                           left_on="instrument_id",
#                           right_on="order_book_id")
#
#         trades['timestamp'] = pd.to_datetime(trades['time_stamp'], unit='us',
#                                              utc=True)
#         trades.set_index('timestamp', inplace=True)
#
#         self.cp.run_day_simulation(self.start_date, trades, self.venue, symbol)
#
#         closed_positions = \
#             self.cp.client_portfolio.positions[1, symbol, 1].closed_positions
#
#         self.assertEqual(180.0, closed_positions[0].duration)
#         self.assertAlmostEqual(-1500, closed_positions[0].pnl)
#         self.assertAlmostEqual(-40000, closed_positions[0].pnl_pip)
#
#     def test_duration_and_pnl_of_trade_closing_opening_position(self):
#         symbol = 101
#         trade_ticks = {
#             'execution_id': [1],
#             'account_id': [3],
#             'instrument_id': [symbol],
#             'order_id': [1],
#             'price_decimal': [1.2],
#             'quantity_decimal': [-1],
#             'trade_date': ["2018-07-11"],
#             'time_stamp': [1531334000000000],
#             'mtf_price_decimal': [1.2],
#             'mtf_execution_id': ["MTF1e"],
#             'broker_id': [5],
#             'remote_venue': [None],
#             'mtf_order_id': ["MTF1"]
#         }
#
#
#         # trades_ticks = {
#         #     'mtf_trade_id': [1],
#         #     'timestamp': [1531334000000000],
#         #     'order_book_id': [self.symbols[0]] * 1,
#         #     'aggressive_account_id': [3],
#         #     'passive_account_id': [5],
#         #     'contract_quantity': [-1],
#         #     'trade_price': [1.20000],
#         #     'action': ['trade'],
#         #     'name': ['fx1'],
#         #     'unit_price': [10000],
#         #     'effective_price_increment': [0.00001]
#         # }
#
#         trades = pd.DataFrame(data=trade_ticks)
#         order_book = pd.DataFrame(data=self.symbol_details)
#         trades = pd.merge(trades,
#                           order_book,
#                           left_on="instrument_id",
#                           right_on="order_book_id")
#
#         trades['timestamp'] = pd.to_datetime(trades['time_stamp'], unit='us',
#                                              utc=True)
#         trades.set_index('timestamp', inplace=True)
#
#         self.cp.run_day_simulation(self.start_date, trades, self.venue, symbol)
#
#         # trades = pd.DataFrame(data=trades_ticks)
#         # trades['timestamp'] = pd.to_datetime(trades['timestamp'], unit='us', utc=True)
#         # trades.set_index('timestamp', inplace=True)
#         #
#         # self.cp.run_day_simulation(self.start_date, trades, 1)
#
#         closed_positions = \
#             self.cp.client_portfolio.positions[1, 101, 3].closed_positions
#         self.assertEqual(16400.0, closed_positions[0].duration)
#         self.assertAlmostEqual(2000, closed_positions[0].pnl)
#         self.assertAlmostEqual(20000, closed_positions[0].pnl_pip)
#
#
# if __name__ == "__main__":
#     unittest.main()
