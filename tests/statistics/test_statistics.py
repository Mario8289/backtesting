import unittest

import pandas as dt
import pandas as pd
import pytz

from risk_backtesting.portfolio import Portfolio
from risk_backtesting.statistics.statistics import Stats


class TestSimulatorPool_internalisation(unittest.TestCase):
    def setUp(self):
        self.stats = Stats(portfolio=Portfolio())

    def tearDown(self):
        pass

    # inventory metrics
    def test_add_bound_rows_for_trading_session(self):
        from pandas.core.resample import TimeGrouper

        grouper = TimeGrouper(key="trading_session", freq="1D")
        df = pd.DataFrame(
            data=[
                [1, "A/USD", dt.datetime(2019, 12, 5), 10, 2],
                [1, "A/USD", dt.datetime(2019, 12, 5), 40, 30],
                [1, "A/USD", dt.datetime(2019, 12, 6), 20, -20],
            ],
            columns=[
                "instrument_id",
                "symbol",
                "trading_session",
                "net_qty",
                "trade_qty",
            ],
            index=pd.DatetimeIndex(
                [
                    dt.datetime(2019, 12, 4, 23),
                    dt.datetime(2019, 12, 5, 2),
                    dt.datetime(2019, 12, 6, 8),
                ],
                tz=pytz.utc,
                name="timestamp",
            ),
        )
        results = self.stats.add_bounding_rows(
            df=df, grouper=grouper, event_features=["instrument_id"]
        )
        self.assertEqual(
            [
                dt.datetime(2019, 12, 4, 17, 0),
                dt.datetime(2019, 12, 4, 23, 0),
                dt.datetime(2019, 12, 5, 2, 0),
                dt.datetime(2019, 12, 5, 16, 59, 59, 999999),
                dt.datetime(2019, 12, 5, 17, 0),
                dt.datetime(2019, 12, 6, 8, 0),
                dt.datetime(2019, 12, 6, 17, 0),
            ],
            [dt.datetime.fromtimestamp(x.value / 1e9) for x in results.index.tolist()],
        )
        self.assertEqual([8, 10, 40, 40, 40, 20, 20], results.net_qty.values.tolist())

    def test_add_bound_rows_for_timestamp_hour(self):
        from pandas.core.resample import TimeGrouper

        grouper = TimeGrouper(key="timestamp", freq="1H")
        df = pd.DataFrame(
            data=[
                [1, "A/USD", dt.datetime(2019, 12, 5), 10, 2],
                [1, "A/USD", dt.datetime(2019, 12, 5), 40, 30],
                [1, "A/USD", dt.datetime(2019, 12, 5), 20, -20],
            ],
            columns=[
                "instrument_id",
                "symbol",
                "trading_session",
                "net_qty",
                "trade_qty",
            ],
            index=pd.DatetimeIndex(
                [
                    dt.datetime(2019, 12, 5, 12, 30),
                    dt.datetime(2019, 12, 5, 12, 59),
                    dt.datetime(2019, 12, 5, 13, 20),
                ],
                tz=pytz.utc,
                name="timestamp",
            ),
        )
        results = self.stats.add_bounding_rows(
            df=df, grouper=grouper, event_features=["instrument_id"]
        )
        self.assertEqual(
            [
                dt.datetime(2019, 12, 5, 12, 0, 0),
                dt.datetime(2019, 12, 5, 12, 30, 0),
                dt.datetime(2019, 12, 5, 12, 59, 0),
                dt.datetime(2019, 12, 5, 12, 59, 59, 999999),
                dt.datetime(2019, 12, 5, 13, 0, 0),
                dt.datetime(2019, 12, 5, 13, 20, 0),
                dt.datetime(2019, 12, 5, 13, 59, 59, 999999),
            ],
            [dt.datetime.fromtimestamp(x.value / 1e9) for x in results.index.tolist()],
        )
        self.assertEqual([8, 10, 40, 40, 40, 20, 20], results.net_qty.values.tolist())

    def test_compute_net_weighted_pos_for_trading_session(self):
        from pandas.core.resample import TimeGrouper

        grouper = TimeGrouper(key="trading_session", freq="1D")
        event_features = ["instrument_id", grouper]
        df = pd.DataFrame(
            data=[
                [1, "A/USD", dt.datetime(2019, 12, 5), 10, 2],
                [1, "A/USD", dt.datetime(2019, 12, 5), 40, 30],
                [1, "A/USD", dt.datetime(2019, 12, 6), 20, -20],
            ],
            columns=[
                "instrument_id",
                "symbol",
                "trading_session",
                "net_qty",
                "trade_qty",
            ],
            index=pd.DatetimeIndex(
                [
                    dt.datetime(2019, 12, 4, 23),
                    dt.datetime(2019, 12, 5, 2),
                    dt.datetime(2019, 12, 6, 8),
                ],
                tz=pytz.utc,
                name="timestamp",
            ),
        )
        results = self.stats.compute_net_weighted_pos(
            df=df, event_features=event_features
        )
        self.assertEqual([28.249999999864006, 32.5], results.values.tolist())

    def test_compute_net_weighted_pos_for_timestamp_hr(self):
        from pandas.core.resample import TimeGrouper

        grouper = TimeGrouper(key="timestamp", freq="1H")
        event_features = ["instrument_id", grouper]
        df = pd.DataFrame(
            data=[
                [1, "A/USD", dt.datetime(2019, 12, 5), 10, 2],
                [1, "A/USD", dt.datetime(2019, 12, 5), 40, 30],
                [1, "A/USD", dt.datetime(2019, 12, 5), 20, -20],
            ],
            columns=[
                "instrument_id",
                "symbol",
                "trading_session",
                "net_qty",
                "trade_qty",
            ],
            index=pd.DatetimeIndex(
                [
                    dt.datetime(2019, 12, 5, 12, 1),
                    dt.datetime(2019, 12, 5, 12, 30),
                    dt.datetime(2019, 12, 5, 13, 10),
                ],
                tz=pytz.utc,
                name="timestamp",
            ),
        )
        results = self.stats.compute_net_weighted_pos(
            df=df, event_features=event_features
        )
        self.assertEqual(
            [24.96666666249074, 23.333333334259258], results.values.tolist()
        )

    def test_aggregate_return_resample_1D_metric_performance_overview(self):

        df = pd.DataFrame(
            data={
                "rpnl_cum": [2, 5],
                "rpnl": [2, 3],
                "net_rpnl_cum": [1, 4],
                "net_rpnl": [1, 3],
                "tighten_cost": [1, 0],
                "tob_price": [1.11, 1.22],
                "notional_traded": [10000, 30000],
                "notional_rejected": [0, 0],
                "hash": ["asekjbjs12398"] * 2,
                "trade_qty": [1, 2],
            },
            index=["2019-07-08T21:23:24.907000000", "2019-07-08T21:38:20.897000000"],
        )

        event_features = ["hash"]

        df.index = pd.to_datetime(df.index)
        df.index.name = "timestamp"
        df_resample = self.stats.aggregate_returns(
            df, "1D", event_features=event_features, metrics=["performance_overview"]
        )

        # performance_overview
        self.assertEqual(df_resample["hash"].values, ["asekjbjs12398"])
        self.assertEqual(df_resample["rpnl"].values, [5])
        self.assertEqual(df_resample["net_rpnl"].values, [4])
        self.assertEqual(df_resample["notional_traded"].values, [40000])
        self.assertEqual(df_resample["notional_rejected"].values, [0])
        self.assertEqual(df_resample["tighten_cost"].values, [1])
        self.assertEqual(df_resample["trade_qty"].values, [3])
        self.assertEqual(df_resample["trade_cnt"].values, [2])

    # def test_aggregate_return_resample_1D(self):
    #
    #     columns = ['venue', 'symbol', 'currency', 'order_book_id', 'price', 'net_qty',
    #                'inventory', 'trade_qty', 'is_long', 'upnl', 'rpnl_cum', 'notional_traded_cum', 'account_id',
    #                'action', 'type', 'portfolio', 'trading_session_year',
    #                'trading_session_month', 'trading_session_day', 'rpnl', 'notional_traded', 'sim',
    #                'strategy', 'takeprofit_limit', 'stoploss_limit', 'max_pos_qty']
    #
    #     index = ['2019-07-08T21:23:24.907000000', '2019-07-08T21:38:20.897000000',
    #              '2019-07-08T21:53:28.999000000', '2019-07-08T22:04:51.459000000',
    #              '2019-07-08T23:01:04.117000000', '2019-07-08T23:01:05.217000000']
    #
    #     trades = [
    #         [1, 'CAD/JPY', 'JPY', 100793, 83.024, 0.3, 0.3, 0.3, 1, 0.0, 0.0, 249072,
    #          1463064262, 'open_long_pos', 'trade', 'lmax', 2019.0, 7.0, 9.0,
    #          0.0, 249072, 'sim_1', 'internalisation', 120, 80, 400.0],
    #         [1, 'CAD/JPY', 'JPY', 100793, 82.99, -3.7, -3.7, -4.0, 0,
    #          7.817626400000001, -0.3031512, 3568672, 1463064262, 'close_long_pos',
    #          'trade', 'lmax', 2019.0, 7.0, 9.0, -0.3031512, 3319600, 'sim_1',
    #          'internalisation', 120, 80, 400.0],
    #         [1, 'CAD/JPY', 'JPY', 100793, 83.014, -4.7, -4.7, -1.0, 0,
    #          0.9461992000000001, -0.3031512, 4398812, 1463064262, 'open_short_pos',
    #          'trade', 'lmax', 2019.0, 7.0, 9.0, 0.0, 830140, 'sim_1',
    #          'internalisation', 120, 80, 400.0],
    #         [1, 'CAD/JPY', 'JPY', 100793, 82.999, -5.2, -5.2, -0.5, 0,
    #          7.7441352000000006, -0.3031512, 4813807, 1463064262, 'open_short_pos',
    #          'trade', 'lmax', 2019.0, 7.0, 9.0, 0.0, 414995, 'sim_1',
    #          'internalisation', 120, 80, 400.0],
    #         [1, 'CAD/JPY', 'JPY', 100793, 83.014, -5.4, -5.4, -0.2, 0,
    #          0.6706072000000001, -0.3031512, 4979835, 1463064262, 'open_short_pos',
    #          'trade', 'lmax', 2019.0, 7.0, 9.0, 0.0, 166028, 'sim_1',
    #          'internalisation', 120, 80, 400.0],
    #         [1, 'CAD/JPY', 'JPY', 100793, 83.000, 0, 0, 5.4, 1,
    #          0, 0.0968488, 9959670, 1463064262, 'TP_close_position',
    #          'hedge', 'lmax', 2019.0, 7.0, 9.0, 0.4, 4979835, 'sim_1',
    #          'internalisation', 120, 80, 400.0]
    #     ]
    #
    #     df = pd.DataFrame(columns=columns, data=trades, index=index)
    #     event_features = ['sim', 'strategy', 'takeprofit_limit', 'stoploss_limit', 'max_pos_qty']
    #
    #     df.index = pd.to_datetime(df.index)
    #     df.index.name = 'timestamp'
    #     df_resample = self.stats.aggregate_returns(
    #         df,
    #         '1D',
    #         event_features=event_features,
    #         metrics=['performance_overview', 'trading_actions_breakdown', 'inventory_overview']) #'trading_drawdowns'
    #
    #     # performance_overview
    #     self.assertEqual(df_resample['sim'].values, ['sim_1'])
    #     self.assertEqual(df_resample['strategy'].values, ['internalisation'])
    #     self.assertEqual(df_resample['takeprofit_limit'].values, [120])
    #     self.assertEqual(df_resample['stoploss_limit'].values, [80])
    #     self.assertEqual(df_resample['max_pos_qty'].values, [400])
    #     [self.assertAlmostEqual(r, e) for (r, e) in zip(df_resample['rpnl_cum'].values, [0.0968488])]
    #     [self.assertAlmostEqual(r, e) for (r, e) in zip(df_resample['rpnl'].values, [0.0968488])]
    #     self.assertEqual(df_resample['notional_traded'].values, [9959670])
    #     self.assertEqual(df_resample['trade_qty'].values, [11.4])
    #     self.assertEqual(df_resample['trade_cnt'].values, [6])
    #
    #     # trading_actions_breakdown
    #     self.assertEqual(df_resample['rpnl_count_close_long_pos'].values, [1])
    #     self.assertEqual(df_resample['rpnl_count_open_long_pos'].values, [1])
    #     self.assertEqual(df_resample['rpnl_count_open_short_pos'].values, [3])
    #     self.assertEqual(df_resample['rpnl_count_TP_close_position'].values, [1])
    #     self.assertEqual(df_resample['rpnl_sum_close_long_pos'].values, [-0.3031512])
    #     self.assertEqual(df_resample['rpnl_sum_open_long_pos'].values, [0])
    #     self.assertEqual(df_resample['rpnl_sum_open_short_pos'].values, [0])
    #     self.assertEqual(df_resample['rpnl_sum_TP_close_position'].values, [0.4])
    #     self.assertEqual(df_resample['notional_traded_sum_close_long_pos'].values, [3319600])
    #     self.assertEqual(df_resample['notional_traded_sum_open_long_pos'].values, [249072])
    #     self.assertEqual(df_resample['notional_traded_sum_open_short_pos'].values, [1411163])
    #     self.assertEqual(df_resample['rpnl_count_hedge'].values, [1])
    #     self.assertEqual(df_resample['rpnl_count_trade'].values, [5])
    #     self.assertEqual(df_resample['rpnl_sum_hedge'].values, [0.4])
    #     self.assertEqual(df_resample['rpnl_sum_trade'].values, [-0.3031512])
    #     self.assertEqual(df_resample['notional_traded_sum_hedge'].values, [4979835])
    #     self.assertEqual(df_resample['notional_traded_sum_trade'].values, [4979835])
    #     self.assertEqual(df_resample['notional_hedge_ratio'].values, [0.5])
    #     self.assertEqual(df_resample['notional_trade_ratio'].values, [0.5])
    #
    #     # inventory_overview
    #     [self.assertAlmostEqual(r, e) for (r, e) in zip(df_resample['wt_net_qty'].values, [-4.06847146])]

    def test_aggregate_return_resample_1H(self):
        pass

    def test_aggregate_return_resample_summary(self):
        pass


if __name__ == "__main__":
    unittest.main()
