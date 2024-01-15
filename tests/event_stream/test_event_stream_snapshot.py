import datetime as dt
import unittest

import pandas as pd
import pytz

from risk_backtesting.event import Event
from risk_backtesting.event_stream.event_stream_snapshot import EventStreamSnapshot

eastern_tz = pytz.timezone("US/Eastern")


class TestEventStreamSnapshot(unittest.TestCase):
    def setUp(self):
        pass

    def test_generate_events_with_tob(self):

        evt_stream = EventStreamSnapshot(sample_rate="1S", excl_period=None)

        date = dt.datetime(2018, 9, 6, 0, 0, 0, 0)
        trade_ticks = {
            "datasource": [4] * 3,
            "execution_id": [1, 2, 3],
            "account_id": [1, 1, 1],
            "symbol": ["EUR/USD", "EUR/USD", "NZD/JPY"],
            "order_book_id": [111] * 2 + [222],
            "order_id": [101, 102, 103],
            "price": [1.10002, 1.10003, 69.68],
            "contract_qty": [1, -1, -1],
            "contract_qty_std": [100, -100, -100],
            "order_qty": [1, -1, -1],
            "order_qty_std": [100, -100, -100],
            "event_type": ["trade"] * 3,
            "trade_date": [date.strftime("%Y-%m-%d")] * 3,
            "time_stamp": [1536235200000, 1536235260000, 1536235320000],
            "unit_price": [10000] * 3,
            "rate_to_usd": [1, 1, 0.0092],
            "price_increment": [0.00001, 0.00001, 0.0001],
            "currency": ["USD"] * 3,
            "shard": ["ldprof"] * 3,
            "trading_session": ["2019-09-06"] * 3,
        }
        trades = pd.DataFrame(trade_ticks)
        trades["timestamp"] = pd.to_datetime(trades["time_stamp"], unit="ms", utc=True)
        trades.set_index("timestamp", inplace=True)

        tob_ticks = {
            "datasource": [4] * 6,
            "ask_price": [1.10003, 69.70, 1.10003, 1.10002, 69.70, 69.71],
            "ask_qty": [100, 100, 100, 100, 100, 100],
            "bid_price": [1.10001, 69.66, 1.10001, 1.00000, 69.66, 69.67],
            "bid_qty": [100, 100, 100, 100, 100, 100],
            "timestamp_micros": [
                1536181200000,
                1536181200000,
                1536235000000,
                1536235240000,
                1536235200000,
                1536235330000,
            ],
            "order_book_id": [111, 222, 111, 111, 222, 222],
            "shard": ["ldprof"] * 6,
            "trading_session": ["2019-09-06"] * 6,
            "event_type": "market_data",
        }
        tob = pd.DataFrame(tob_ticks)
        tob["ask_price"] = tob["ask_price"] * 1000000
        tob["bid_price"] = tob["bid_price"] * 1000000
        tob["timestamp"] = pd.to_datetime(tob["timestamp_micros"], unit="ms", utc=True)
        tob.set_index("timestamp", inplace=True)
        tob.sort_index(inplace=True)

        tob_sample = evt_stream.sample(tob, date)

        events = evt_stream.generate_events(
            date=date.date(), trades=trades, tob=tob_sample
        )

        self.assertEqual(events[events.event_type == "trade"].__len__(), 3)
        self.assertEqual(
            events[
                (events.event_type == "market_data") & (events.order_book_id == 111)
                ].__len__(),
            86400,
        )
        self.assertEqual(
            events[
                (events.event_type == "market_data") & (events.order_book_id == 222)
                ].__len__(),
            86400,
        )

        for field in ["ask_price", "bid_price", "rate_to_usd"]:
            self.assertFalse(any(events[field].isnull()))

        for order_book, rate in zip([111, 222], [1, 0.00920]):
            events_for_order_book = events[events.order_book_id == order_book].copy()
            event_rate = events_for_order_book["rate_to_usd"].unique()
            self.assertEqual(len(event_rate), 1)
            self.assertAlmostEqual(event_rate[0], rate)

    def test_generate_events_without_tob(self):
        evt_stream = EventStreamSnapshot(sample_rate="1S", excl_period=None)

        event = Event()
        date = dt.datetime(2018, 9, 6, 0, 0, 0, 0)
        trade_ticks = {
            "datasource": [4] * 3,
            "execution_id": [1, 2, 3],
            "account_id": [1, 1, 1],
            "symbol": ["EUR/USD", "EUR/USD", "NZD/JPY"],
            "order_book_id": [111] * 2 + [222],
            "order_id": [101, 102, 103],
            "price": [1.10002, 1.10003, 69.68],
            "contract_qty": [1, -1, -1],
            "contract_qty_std": [100, -100, -100],
            "order_qty": [1, -1, -1],
            "order_qty_std": [100, -100, -100],
            "event_type": ["trade"] * 3,
            "trade_date": [date.strftime("%Y-%m-%d")] * 3,
            "time_stamp": [1536235200000, 1536235260000, 1536235320000],
            "unit_price": [10000] * 3,
            "rate_to_usd": [1, 1, 0.0092],
            "price_increment": [0.00001, 0.00001, 0.0001],
        }
        trades = pd.DataFrame(trade_ticks)
        trades["timestamp"] = pd.to_datetime(trades["time_stamp"], unit="ms", utc=True)
        trades.set_index("timestamp", inplace=True)

        events = evt_stream.generate_events(date=date.date(), trades=trades)

        self.assertListEqual(
            sorted(events.columns.tolist()), sorted(list(event.__dict__.keys()))
        )
        self.assertEqual(events.shape, tuple([3, 48]))
        self.assertListEqual(["trade"], list(events.event_type.unique()))

    def test_generate_events_with_no_trade_no_tob(self):
        evt_stream = EventStreamSnapshot(sample_rate="1S", excl_period=None)

        date = dt.datetime(2019, 12, 25, 0, 0, 0, 0)

        tob = pd.DataFrame(
            columns=[
                "datasource",
                "order_book_id",
                "timestamp_micros",
                "tier",
                "bid_price",
                "bid_qty",
                "ask_price",
                "ask_qty",
                "sequence",
                "shard",
                "trading_session",
                "gfd",
                "gfw",
            ],
            index=pd.to_datetime([]),
        )
        tob.index.name = "timestamp"

        trades = pd.DataFrame(
            columns=[
                "datasource",
                "shard",
                "timestamp_micros",
                "time_stamp",
                "trade_date",
                "trading_session",
                "execution_id",
                "mtf_execution_id",
                "order_id",
                "immediate_order",
                "order_type",
                "time_in_force",
                "order_book_id",
                "instrument_id",
                "symbol",
                "bid_adjustment",
                "ask_adjustment",
                "tob_snapshot_bid_price",
                "tob_snapshot_ask_price",
                "price",
                "notional_value",
                "notional_value_usd",
                "unit_quantity",
                "rate_to_usd",
                "contract_qty",
                "order_qty",
                "unit_price",
                "currency",
                "broker_id",
                "account_id",
                "price_increment",
                "event_type",
            ],
            index=pd.to_datetime([]),
        )
        trades.index.name = "timestamp"

        events = evt_stream.generate_events(trades=trades, tob=tob, date=date.date())

        self.assertEqual(events.shape, tuple([0, 48]))

    def test_generate_events_with_tob_and_exclusion_period(self):

        evt_stream = EventStreamSnapshot(
            sample_rate="1S", excl_period=[[16, 30], [18, 30]]
        )
        event = Event()
        date = dt.datetime(2018, 9, 6, 0, 0, 0, 0)

        trade_ticks = {
            "datasource": [4],
            "execution_id": [1],
            "account_id": [1],
            "symbol": ["EUR/USD"],
            "order_book_id": [111],
            "order_id": [101],
            "price": [1.10002],
            "contract_qty": [1],
            "contract_qty_std": [100],
            "order_qty": [1],
            "order_qty_std": [100],
            "event_type": ["trade"],
            "trade_date": [date.strftime("%Y-%m-%d")],
            "time_stamp": [1536235200000],
            "unit_price": [10000],
            "rate_to_usd": [1],
            "price_increment": [0.00001],
            "currency": ["USD"],
            "shard": ["ldprof"],
            "trading_session": ["2019-09-06"],
        }

        trades = pd.DataFrame(trade_ticks)
        trades["timestamp"] = pd.to_datetime(trades["time_stamp"], unit="ms", utc=True)
        trades.set_index("timestamp", inplace=True)

        tob_ticks = {
            "datasource": [4],
            "ask_price": [1.10003],
            "ask_qty": [100],
            "bid_price": [1.10001],
            "bid_qty": [100],
            "timestamp_micros": [1536181200000],
            "order_book_id": [111],
            "shard": ["ldprof"],
            "trading_session": ["2019-09-06"],
            "event_type": "market_data",
        }
        tob = pd.DataFrame(tob_ticks)
        tob["ask_price"] = tob["ask_price"] * 1000000
        tob["bid_price"] = tob["bid_price"] * 1000000
        tob["timestamp"] = pd.to_datetime(tob["timestamp_micros"], unit="ms", utc=True)
        tob.set_index("timestamp", inplace=True)

        tob_sample = evt_stream.sample(tob, date)

        events = evt_stream.generate_events(date=date, trades=trades, tob=tob_sample)

        self.assertListEqual(
            sorted(events.columns.tolist()), sorted(list(event.__dict__.keys()))
        )

        self.assertEqual(events.shape, tuple([86401, 48]))

        start_exclusion_time = (
            eastern_tz.localize(date - dt.timedelta(days=1))
            .replace(hour=18, minute=30)
            .astimezone(pytz.UTC)
        )
        end_exclusion_time = (
            eastern_tz.localize(date).replace(hour=16, minute=30).astimezone(pytz.UTC)
        )

        self.assertListEqual(
            events.loc[:start_exclusion_time, "untrusted"].unique().tolist(), [1]
        )
        self.assertListEqual(
            events.loc[end_exclusion_time:, "untrusted"].unique().tolist(), [1]
        )
        self.assertListEqual(
            events.loc[
            start_exclusion_time
            + dt.timedelta(seconds=1) : end_exclusion_time
                                        - dt.timedelta(seconds=1),
            "untrusted",
            ]
            .unique()
            .tolist(),
            [0],
            )

    def test_generate_events_with_gfd_lifespan_events(self):
        evt_stream = EventStreamSnapshot(
            sample_rate="1S", excl_period=[[16, 30], [18, 30]]
        )
        event = Event()
        date = dt.datetime(2018, 9, 6, 0, 0, 0, 0)

        trade_ticks = {
            "datasource": [4],
            "execution_id": [1],
            "account_id": [1],
            "symbol": ["EUR/USD"],
            "order_book_id": [111],
            "order_id": [101],
            "price": [1.10002],
            "contract_qty": [1],
            "contract_qty_std": [100],
            "order_qty": [1],
            "order_qty_std": [100],
            "event_type": ["trade"],
            "trade_date": [date.strftime("%Y-%m-%d")],
            "time_stamp": [1536235200000],
            "unit_price": [10000],
            "rate_to_usd": [1],
            "price_increment": [0.00001],
            "currency": ["USD"],
            "shard": ["ldprof"],
            "trading_session": ["2019-09-06"],
        }

        trades = pd.DataFrame(trade_ticks)
        trades["timestamp"] = pd.to_datetime(trades["time_stamp"], unit="ms", utc=True)
        trades.set_index("timestamp", inplace=True)

        tob_ticks = {
            "datasource": [4],
            "ask_price": [1.10003],
            "ask_qty": [100],
            "bid_price": [1.10001],
            "bid_qty": [100],
            "timestamp_micros": [1536181200000],
            "order_book_id": [111],
            "shard": ["ldprof"],
            "trading_session": ["2019-09-06"],
            "event_type": "market_data",
        }
        tob = pd.DataFrame(tob_ticks)
        tob["ask_price"] = tob["ask_price"] * 1000000
        tob["bid_price"] = tob["bid_price"] * 1000000
        tob["timestamp"] = pd.to_datetime(tob["timestamp_micros"], unit="ms", utc=True)
        tob.set_index("timestamp", inplace=True)

        tob_sample = evt_stream.sample(tob, date)

        events = evt_stream.generate_events(date=date, trades=trades, tob=tob_sample)

        self.assertListEqual(
            sorted(events.columns.tolist()), sorted(list(event.__dict__.keys()))
        )
        self.assertEqual(events.shape, tuple([86401, 48]))

        last_five_mins = (
                eastern_tz.localize(date).replace(hour=17) - dt.timedelta(minutes=5)
        ).astimezone(pytz.utc)

        self.assertListEqual(
            events.loc[: last_five_mins - dt.timedelta(seconds=1), "gfd"]
            .unique()
            .tolist(),
            [0],
            )
        self.assertListEqual(events.loc[last_five_mins:, "gfd"].unique().tolist(), [1])
        self.assertListEqual(events["gfw"].unique().tolist(), [0])

    def test_generate_events_with_gfw_lifespan_events(self):
        evt_stream = EventStreamSnapshot(
            sample_rate="1S", excl_period=[[16, 30], [18, 30]]
        )
        event = Event()
        date = dt.datetime(2018, 9, 7, 0, 0, 0, 0)  # Friday

        trade_ticks = {
            "datasource": [4],
            "execution_id": [1],
            "account_id": [1],
            "symbol": ["EUR/USD"],
            "order_book_id": [111],
            "order_id": [101],
            "price": [1.10002],
            "contract_qty": [1],
            "contract_qty_std": [100],
            "order_qty": [1],
            "order_qty_std": [100],
            "event_type": ["trade"],
            "trade_date": [date.strftime("%Y-%m-%d")],
            "time_stamp": [1536235200000],
            "unit_price": [10000],
            "rate_to_usd": [1],
            "price_increment": [0.00001],
            "currency": ["USD"],
            "shard": ["ldprof"],
            "trading_session": ["2019-09-06"],
        }

        trades = pd.DataFrame(trade_ticks)
        trades["timestamp"] = pd.to_datetime(trades["time_stamp"], unit="ms", utc=True)
        trades.set_index("timestamp", inplace=True)

        tob_ticks = {
            "datasource": [4],
            "ask_price": [1.10003],
            "ask_qty": [100],
            "bid_price": [1.10001],
            "bid_qty": [100],
            "timestamp_micros": [1536181200000],
            "order_book_id": [111],
            "shard": ["ldprof"],
            "trading_session": ["2019-09-06"],
            "event_type": "market_data",
        }
        tob = pd.DataFrame(tob_ticks)
        tob["ask_price"] = tob["ask_price"] * 1000000
        tob["bid_price"] = tob["bid_price"] * 1000000
        tob["timestamp"] = pd.to_datetime(tob["timestamp_micros"], unit="ms", utc=True)
        tob.set_index("timestamp", inplace=True)

        tob_sample = evt_stream.sample(tob, date)

        events = evt_stream.generate_events(date=date, trades=trades, tob=tob_sample)

        self.assertListEqual(
            sorted(events.columns.tolist()), sorted(list(event.__dict__.keys()))
        )
        self.assertEqual(events.shape, tuple([86401, 48]))

        last_five_mins = (
                eastern_tz.localize(date).replace(hour=17) - dt.timedelta(minutes=5)
        ).astimezone(pytz.utc)

        self.assertListEqual(
            events.loc[: last_five_mins - dt.timedelta(seconds=1), "gfd"]
            .unique()
            .tolist(),
            [0],
            )
        self.assertListEqual(events.loc[last_five_mins:, "gfd"].unique().tolist(), [1])
        self.assertListEqual(
            events.loc[: last_five_mins - dt.timedelta(seconds=1), "gfw"]
            .unique()
            .tolist(),
            [0],
            )
        self.assertListEqual(events.loc[last_five_mins:, "gfw"].unique().tolist(), [1])


if __name__ == "__main__":
    unittest.main()
