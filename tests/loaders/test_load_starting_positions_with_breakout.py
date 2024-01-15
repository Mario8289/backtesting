import datetime as dt
import unittest

import pandas as pd

from risk_backtesting.loaders.load_starting_positions import load_open_positions
from risk_backtesting.loaders.load_starting_positions_with_breakout import (
    StartingPositionsParquetLoaderBreakOut,
)
from risk_backtesting.loaders.load_starting_positions_with_breakout import (
    get_delta_trades,
    load_delta_trades,
    load_starting_positions,
)
from risk_backtesting.portfolio import Portfolio
from risk_backtesting.position import OpenPosition

GENERIC_TRADES = pd.DataFrame(
    {
        "datasource": [4] * 5,
        "time_stamp": [
            1598800547000,
            1598886108000,
            1598889708000,
            1598968908000,
            1598972508000,
        ],
        "execution_id": range(1, 6, 1),
        "shard": ["ldprof"] * 5,
        "price": [1.0, 1.1, 1.2, 1.3, 1.4],
        "symbol": ["A/B"] * 5,
        "trade_date": [
            dt.date(2020, 8, 30),
            dt.date(2020, 8, 31),
            dt.date(2020, 8, 31),
            dt.date(2020, 9, 1),
            dt.date(2020, 9, 1),
        ],
        "unit_price": [10000] * 5,
        "price_increment": [0.00001] * 5,
        "account_id": [1] * 5,
        "counterparty_account_id": [None] * 5,
        "instrument_id": [123] * 5,
        "contract_qty": [0.2, -1, -1, -0.4, -0.2],
        "rate_to_usd": [1] * 5,
        "currency": ["USD"] * 5,
    }
)

GENERIC_STARTING_POSITIONS = pd.DataFrame(
    data={
        "datasource": [4] * 20,
        "shard": ["ldprof"] * 20,
        "symbol": ["A/B"] * 20,
        "currency": ["USD"] * 20,
        "instrument_id": [123] * 20,
        "unit_price": [10000] * 20,
        "price_increment": [0.00001] * 20,
        "next_trading_day": [
            "2020-08-13",
            "2020-08-14",
            "2020-08-15",
            "2020-08-16",
            "2020-08-17",
            "2020-08-18",
            "2020-08-19",
            "2020-08-20",
            "2020-08-22",
            "2020-08-23",
            "2020-08-24",
            "2020-08-25",
            "2020-08-26",
            "2020-08-27",
            "2020-08-28",
            "2020-08-29",
            "2020-08-30",
            "2020-08-31",
            "2020-09-01",
            "2020-09-02",
        ],
        "account_id": [1] * 20,
        "position": [-0.2] * 17 + [0, -2, -2.6],
        "open_cost": [(1 * 10000 * 0.2)] * 18
                     + [
                         ((1.1 * 10000 * 1) + (1.2 * 10000 * 1)),
                         (
                                 (1.1 * 10000 * 1)
                                 + (1.2 * 10000 * 1)
                                 + (1.3 * 10000 * 0.1)
                                 + (1.4 * 10000 * 0.2)
                         ),
                     ],
    },
    index=[
        "2020-08-13T00:00:00.000Z",
        "2020-08-14T00:00:00.000Z",
        "2020-08-15T00:00:00.000Z",
        "2020-08-16T00:00:00.000Z",
        "2020-08-17T00:00:00.000Z",
        "2020-08-18T00:00:00.000Z",
        "2020-08-19T00:00:00.000Z",
        "2020-08-20T00:00:00.000Z",
        "2020-08-22T00:00:00.000Z",
        "2020-08-23T00:00:00.000Z",
        "2020-08-24T00:00:00.000Z",
        "2020-08-25T00:00:00.000Z",
        "2020-08-26T00:00:00.000Z",
        "2020-08-27T00:00:00.000Z",
        "2020-08-28T00:00:00.000Z",
        "2020-08-29T00:00:00.000Z",
        "2020-08-30T00:00:00.000Z",
        "2020-08-31T00:00:00.000Z",
        "2020-09-01T00:00:00.000Z",
        "2020-09-02T00:00:00.000Z",
    ],
)


class LoaderStub:
    def __init__(self, trades, starting_positions):
        self.trades = trades
        self.starting_positions = starting_positions

    def load_broker_trades(
            self, datasource_label, start_date, end_date, instrument, account, schema=None
    ):
        trades = self.trades[
            (self.trades.shard == datasource_label)
            & (self.trades.instrument_id.isin(instrument))
            & (self.trades.trade_date >= start_date)
            & (self.trades.trade_date <= end_date)
            & (self.trades.account_id.isin(account))
            ].copy()
        return trades

    def load_opening_positions(
            self, datasource_label, start_date, end_date, instrument, account, schema=None
    ):
        try:
            starting_positions = self.starting_positions[
                (
                        self.starting_positions.next_trading_day
                        == start_date.strftime("%Y-%m-%d")
                )
                & (self.starting_positions.shard == datasource_label)
                & (self.starting_positions.instrument_id.isin(instrument))
                & (self.starting_positions.account_id.isin(account))
                ].copy()
        except Exception as e:
            raise e

        return starting_positions


class TestGetOpeningPositionsFifo(unittest.TestCase):
    def setUp(self):
        self.loader = StartingPositionsParquetLoaderBreakOut(
            loader=LoaderStub(
                trades=GENERIC_TRADES, starting_positions=GENERIC_STARTING_POSITIONS
            )
        )

    def tearDown(self) -> None:
        pass

    # get_delta_trades

    def test_load_delta_trades_single_instrument_single_account(self):
        starting_positions = pd.DataFrame(
            data={
                "name": ["GBP/AUD"],
                "currency": ["USD"],
                "instrument_id": [123],
                "unit_price": [10000],
                "price_increment": [0.00001],
                "next_trading_day": ["2020-08-03"],
                "account_id": [1],
                "position": [-2],
            },
            index=pd.DatetimeIndex(["2020-08-03T00:00:00.000Z"], name="timestamp"),
        )

        trades = pd.DataFrame(
            {
                "timestamp": [
                    "2020-07-30 20:13:04.434",
                    "2020-07-30 20:13:05.434",
                    "2020-07-30 20:13:06.434",
                    "2020-07-30 20:13:07.434",
                ],
                "execution_id": [1, 2, 3, 4],
                "account_id": [1] * 4,
                "instrument_id": [123] * 4,
                "contract_qty": [1, 1, -1, -1],
            }
        )

        trades = get_delta_trades(starting_positions, trades)

        self.assertEqual([123, 123], trades.instrument_id.tolist())
        self.assertEqual([-1, -1], trades.contract_qty.tolist())
        self.assertEqual([1, 0], trades.start_of_position.tolist())
        self.assertEqual([1, 1], trades.account_id.tolist())

    def test_load_delta_trades_single_instrument_two_account(self):
        starting_positions = pd.DataFrame(
            data={
                "name": ["GBP/AUD"] * 2,
                "currency": ["USD"] * 2,
                "instrument_id": [123] * 2,
                "unit_price": [10000] * 2,
                "price_increment": [0.00001] * 2,
                "next_trading_day": ["2020-08-03T00:00:00.000Z"] * 2,
                "account_id": [2, 1],
                "position": [2, -2],
            },
            index=pd.DatetimeIndex(["2020-08-03T00:00:00.000Z"] * 2, name="timestamp"),
        )

        trades = pd.DataFrame(
            {
                "timestamp": [
                    "2020-07-30 20:13:04.434",
                    "2020-07-30 20:13:05.434",
                    "2020-07-30 20:13:06.434",
                    "2020-07-30 20:13:07.434",
                    "2020-07-30 20:13:08.434",
                    "2020-07-30 20:13:09.434",
                    "2020-07-30 20:13:10.434",
                    "2020-07-30 20:13:11.434",
                ],
                "execution_id": range(1, 9, 1),
                "account_id": [1, 2] * 4,
                "instrument_id": [123] * 8,
                "contract_qty": [1, -1, 1, -1, -1, 1, -1, 1],
            }
        )

        trades = get_delta_trades(starting_positions, trades)

        self.assertEqual([123] * 4, trades.instrument_id.tolist())
        self.assertEqual([1, 1, 2, 2], trades.account_id.tolist())
        self.assertEqual([-1, -1, 1, 1], trades.contract_qty.tolist())
        self.assertEqual([1, 0] * 2, trades.start_of_position.tolist())

    def test_load_delta_trades_two_instrument_single_account(self):
        starting_positions = pd.DataFrame(
            data={
                "name": ["CHF/JPY", "GBP/AUD"],
                "instrument_id": [123, 456],
                "currency": ["USD"] * 2,
                "unit_price": [10000] * 2,
                "price_increment": [0.00100, 0.00001],
                "next_trading_day": ["2020-08-03T00:00:00.000Z"] * 2,
                "account_id": [1] * 2,
                "position": [-2, 0.1],
            },
            index=pd.DatetimeIndex(["2020-08-03T00:00:00.000Z"] * 2, name="timestamp"),
        )

        trades = pd.DataFrame(
            {
                "timestamp": [
                    "2020-07-30 20:13:04.434",
                    "2020-07-30 20:13:05.434",
                    "2020-07-30 20:13:06.434",
                    "2020-07-30 20:13:07.434",
                    "2020-07-30 20:13:07.434",
                ],
                "execution_id": [1, 2, 3, 4, 5],
                "account_id": [1] * 5,
                "instrument_id": [123, 123, 123, 123, 456],
                "contract_qty": [1, 1, -1, -1, 0.1],
            }
        )

        trades = get_delta_trades(starting_positions, trades)

        self.assertEqual([123, 123, 456], trades.instrument_id.tolist())
        self.assertEqual([-1, -1, 0.1], trades.contract_qty.tolist())
        self.assertEqual([1, 0, 1], trades.start_of_position.tolist())
        self.assertEqual([1, 1, 1], trades.account_id.tolist())

    def test_load_delta_trades_two_instrument_two_account(self):
        starting_positions = pd.DataFrame(
            data={
                "name": ["CHF/JPY", "GBP/AUD", "GBP/AUD"],
                "instrument_id": [123, 456, 456],
                "currency": ["USD"] * 3,
                "unit_price": [10000] * 3,
                "price_increment": [0.00100, 0.00001, 0.00001],
                "next_trading_day": ["2020-08-03"] * 3,
                "account_id": [1, 1, 2],
                "position": [-2, 0.1, 0.2],
            },
            index=pd.DatetimeIndex(["2020-08-03T00:00:00.000Z"] * 3, name="timestamp"),
        )

        trades = pd.DataFrame(
            {
                "timestamp": [
                    "2020-07-30 20:13:04.434",
                    "2020-07-30 20:13:05.434",
                    "2020-07-30 20:13:06.434",
                    "2020-07-30 20:13:07.434",
                    "2020-07-30 20:13:08.434",
                    "2020-07-30 20:13:09.434",
                    "2020-07-30 20:13:10.434",
                ],
                "execution_id": range(1, 8, 1),
                "account_id": [1, 1, 1, 1, 1, 2, 2],
                "instrument_id": [123, 123, 123, 123, 456, 456, 456],
                "contract_qty": [1, 1, -1, -1, 0.1, 0.1, 0.1],
            }
        )

        trades = get_delta_trades(starting_positions, trades)

        self.assertEqual([123, 123, 456, 456, 456], trades.instrument_id.tolist())
        self.assertEqual([-1, -1, 0.1, 0.1, 0.1], trades.contract_qty.tolist())
        self.assertEqual([1, 0, 1, 1, 0], trades.start_of_position.tolist())
        self.assertEqual([1, 1, 1, 2, 2], trades.account_id.tolist())

    def test_load_delta_trades_no_position_inversions(self):
        # load trades that make up a position but position did not start in this time window so there are no inversions

        starting_positions = pd.DataFrame(
            data={
                "name": ["GBP/AUD"],
                "instrument_id": [123],
                "currency": ["USD"],
                "unit_price": [10000],
                "price_increment": [0.00001],
                "next_trading_day": ["2020-08-03T00:00:00.000Z"],
                "account_id": [1],
                "position": [-2],
            },
            index=pd.DatetimeIndex(["2020-08-03T00:00:00.000Z"], name="timestamp"),
        )

        trades = pd.DataFrame(
            {
                "timestamp": ["2020-07-30 20:13:04.434", "2020-07-30 20:13:05.434"],
                "execution_id": [1, 2],
                "account_id": [1] * 2,
                "instrument_id": [123] * 2,
                "contract_qty": [-0.5, -0.5],
            }
        )

        trades = get_delta_trades(starting_positions, trades)

        self.assertEqual([123, 123], trades.instrument_id.tolist())
        self.assertEqual([-0.5, -0.5], trades.contract_qty.tolist())
        self.assertEqual([0, 0], trades.start_of_position.tolist())
        self.assertEqual([1, 1], trades.account_id.tolist())

    # load_delta_trades
    def test_load_delta_trades_for_past_one_day(self):
        # load trades that make up a position but position did not start in this time window so there are no inversions

        starting_positions = pd.DataFrame(
            data={
                "name": ["GBP/AUD"],
                "instrument_id": [123],
                "currency": ["USD"],
                "unit_price": [10000],
                "price_increment": [0.00001],
                "next_trading_day": ["2020-07-31T00:00:00.000Z"],
                "account_id": [1],
                "position": [-2],
            },
            index=pd.DatetimeIndex(["2020-07-31T00:00:00.000Z"], name="timestamp"),
        )

        trades = pd.DataFrame(
            {
                "shard": ["ldprof"] * 2,
                "time_stamp": [1596139984000, 1596139984000],
                "execution_id": [1, 2],
                "account_id": [1] * 2,
                "instrument_id": [123] * 2,
                "trade_date": [dt.date(2020, 7, 30)] * 2,
                "contract_qty": [-1.5, -0.5],
            }
        )

        loader = LoaderStub(
            trades=GENERIC_TRADES, starting_positions=GENERIC_STARTING_POSITIONS
        )
        loader.trades = trades

        trades = load_delta_trades(
            loader=loader,
            datasource_label="ldprof",
            date=dt.date(2020, 7, 31),
            starting_positions=starting_positions,
        )

        self.assertEqual([123, 123], trades.instrument_id.tolist())
        self.assertEqual([-1.5, -0.5], trades.contract_qty.tolist())
        self.assertEqual([1, 0], trades.start_of_position.tolist())
        self.assertEqual([1, 1], trades.account_id.tolist())

    def test_load_delta_trades_for_multiple_days(self):
        # load trades that make up a position but position did not start in this time window so there are no inversions

        starting_positions = pd.DataFrame(
            data={
                "shard": ["ldprof"] * 2,
                "name": ["GBP/AUD"] * 2,
                "currency": ["USD"] * 2,
                "instrument_id": [123] * 2,
                "unit_price": [10000] * 2,
                "price_increment": [0.00001] * 2,
                "next_trading_day": ["2020-07-30", "2020-07-31",],
                "account_id": [1] * 2,
                "position": [-1, -2],
            },
            index=pd.DatetimeIndex(
                ["2020-07-30T00:00:00.000Z", "2020-07-31T00:00:00.000Z",],
                name="timestamp",
            ),
        )
        starting_positions["timestamp"] = pd.to_datetime(
            starting_positions["next_trading_day"]
        )
        starting_positions.set_index("timestamp", inplace=True)

        # test has 2 trades from tdate -1 and 1 from tdate -2
        trades = pd.DataFrame(
            {
                "shard": ["ldprof"] * 3,
                "time_stamp": [1596053585000, 1596139984000, 1596139985000],
                "execution_id": [1, 2, 3],
                "account_id": [1] * 3,
                "instrument_id": [123] * 3,
                "trade_date": [
                    dt.date(2020, 7, 29),
                    dt.date(2020, 7, 30),
                    dt.date(2020, 7, 30),
                ],
                "contract_qty": [-1, -0.5, -0.5],
            }
        )

        loader = LoaderStub(
            trades=GENERIC_TRADES, starting_positions=GENERIC_STARTING_POSITIONS
        )
        loader.trades = trades
        loader.starting_positions = starting_positions

        trades = load_delta_trades(
            loader=loader,
            datasource_label="ldprof",
            date=dt.date(2020, 7, 31),
            starting_positions=starting_positions.loc["2020-07-31":"2020-07-31", :],
        )

        self.assertEqual([123, 123, 123], trades.instrument_id.tolist())
        self.assertEqual([-1, -0.5, -0.5], trades.contract_qty.tolist())
        self.assertEqual([1, 0, 0], trades.start_of_position.tolist())
        self.assertEqual([1, 1, 1], trades.account_id.tolist())

    def test_load_delta_trades_exceeds_max_days_search_for_trades(self):
        # when the max days is exceeded then set counterparty to -1

        starting_positions = pd.DataFrame(
            data={
                "datasource": [4] * 3,
                "shard": ["ldprof"] * 3,
                "currency": ["USD"] * 3,
                "symbol": ["GBP/AUD"] * 3,
                "instrument_id": [123] * 3,
                "unit_price": [10000] * 3,
                "price_increment": [0.00001] * 3,
                "next_trading_day": ["2020-07-29", "2020-07-30", "2020-07-31",],
                "account_id": [1] * 3,
                "position": [-1, -1, -2],
                "open_cost": [-1 * 1.10 * 10000, -1 * 1.11 * 10000, -2 * 1.12 * 10000],
            },
            index=pd.DatetimeIndex(
                [
                    "2020-07-29T00:00:00.000Z",
                    "2020-07-30T00:00:00.000Z",
                    "2020-07-31T00:00:00.000Z",
                ],
                name="timestamp",
            ),
        )
        starting_positions["timestamp"] = pd.to_datetime(
            starting_positions["next_trading_day"]
        )
        starting_positions.set_index("timestamp", inplace=True)

        # test has 2 trades from tdate -1 and 1 from tdate -2
        trades = pd.DataFrame(
            {
                "shard": ["ldprof"] * 2,
                "time_stamp": [1596139984000, 1596139985000],
                "execution_id": [1, 2],
                "account_id": [1] * 2,
                "instrument_id": [123] * 2,
                "trade_date": [dt.date(2020, 7, 30), dt.date(2020, 7, 30)],
                "contract_qty": [-0.5, -0.5],
                "rate_to_usd": [1] * 2,
                "price": [1] * 2,
                "symbol": ["A/B"] * 2,
                "unit_price": [10000] * 2,
                "currency": ["USD"] * 2,
            }
        )

        loader = LoaderStub(
            trades=GENERIC_TRADES, starting_positions=GENERIC_STARTING_POSITIONS
        )
        loader.trades = trades
        loader.starting_positions = starting_positions

        trades = load_delta_trades(
            loader=loader,
            datasource_label="ldprof",
            date=dt.date(2020, 7, 31),
            starting_positions=starting_positions.loc["2020-07-31":"2020-07-31", :],
            max_days=2,
        )

        self.assertEqual([123, 123, 123], trades.instrument_id.tolist())
        self.assertEqual([-1, -0.5, -0.5], trades.contract_qty.tolist())
        self.assertEqual([1, 1, 1], trades.account_id.tolist())

    def test_load_delta_trades_for_multiple_days_intermediate_date_has_no_trades(self):
        # test load trades when middle day has no trades so you have go back a further day

        starting_positions = pd.DataFrame(
            data={
                "shard": ["ldprof"] * 3,
                "currency": ["USD"] * 3,
                "name": ["GBP/AUD"] * 3,
                "instrument_id": [123] * 3,
                "unit_price": [10000] * 3,
                "price_increment": [0.00001] * 3,
                "next_trading_day": ["2020-07-29", "2020-07-30", "2020-07-31",],
                "account_id": [1] * 3,
                "position": [-1, -1, -2],
            },
            index=[
                "2020-07-29T00:00:00.000Z",
                "2020-07-30T00:00:00.000Z",
                "2020-07-31T00:00:00.000Z",
            ],
        )
        starting_positions["timestamp"] = pd.to_datetime(
            starting_positions["next_trading_day"]
        )
        starting_positions.set_index("timestamp", inplace=True)

        # test has 2 trades from tdate -1 and 1 from tdate -2
        trades = pd.DataFrame(
            {
                "shard": ["ldprof"] * 3,
                "time_stamp": [1595967185000, 1596139984000, 1596139985000],
                "execution_id": [1, 2, 3],
                "account_id": [1] * 3,
                "instrument_id": [123] * 3,
                "trade_date": [
                    dt.date(2020, 7, 28),
                    dt.date(2020, 7, 30),
                    dt.date(2020, 7, 30),
                ],
                "contract_qty": [-1, -0.5, -0.5],
            }
        )

        loader = LoaderStub(
            trades=GENERIC_TRADES, starting_positions=GENERIC_STARTING_POSITIONS
        )
        loader.trades = trades
        loader.starting_positions = starting_positions

        trades = load_delta_trades(
            loader=loader,
            datasource_label="ldprof",
            date=dt.date(2020, 7, 31),
            starting_positions=starting_positions.loc["2020-07-31":"2020-07-31", :],
        )

        self.assertEqual([123, 123, 123], trades.instrument_id.tolist())
        self.assertEqual([-1, -0.5, -0.5], trades.contract_qty.tolist())
        self.assertEqual([1, 0, 0], trades.start_of_position.tolist())
        self.assertEqual([1, 1, 1], trades.account_id.tolist())

    def test_load_delta_trades_for_multiple_days_prev_date_weekend_no_trades_or_starting_positions(
            self,
    ):
        # load trades that make up a position but position did not start in this time window so there are no inversions

        starting_positions = pd.DataFrame(
            data={
                "shard": ["ldprof"] * 2,
                "currency": ["USD"] * 2,
                "name": ["GBP/AUD"] * 2,
                "instrument_id": [123] * 2,
                "unit_price": [10000] * 2,
                "price_increment": [0.00001] * 2,
                "next_trading_day": [
                    "2020-07-29T00:00:00.000Z",
                    "2020-07-31T00:00:00.000Z",
                ],
                "account_id": [1] * 2,
                "position": [-1, -2],
            },
            index=["2020-07-29T00:00:00.000Z", "2020-07-31T00:00:00.000Z",],
        )
        starting_positions["timestamp"] = pd.to_datetime(
            starting_positions["next_trading_day"]
        )
        starting_positions.set_index("timestamp", inplace=True)

        # test has 2 trades from tdate -1 and 1 from tdate -2
        trades = pd.DataFrame(
            data={
                "shard": ["ldprof"] * 2,
                "time_stamp": [1596053584000, 1596053585000],
                "execution_id": [1, 2],
                "account_id": [1] * 2,
                "instrument_id": [123] * 2,
                "trade_date": [dt.date(2020, 7, 29), dt.date(2020, 7, 29),],
                "contract_qty": [-1, -1],
            }
        )

        loader = LoaderStub(
            trades=GENERIC_TRADES, starting_positions=GENERIC_STARTING_POSITIONS
        )
        loader.trades = trades
        loader.starting_positions = starting_positions

        trades = load_delta_trades(
            loader=loader,
            datasource_label="ldprof",
            date=dt.date(2020, 7, 31),
            starting_positions=starting_positions.loc["2020-07-31":"2020-07-31", :],
        )

        self.assertEqual([123, 123], trades.instrument_id.tolist())
        self.assertEqual([-1, -1], trades.contract_qty.tolist())
        self.assertEqual([1, 0], trades.start_of_position.tolist())
        self.assertEqual([1, 1], trades.account_id.tolist())

    # load_starting_positions

    def test_load_starting_positions_for_one_account_one_instrument(self):
        trades = pd.DataFrame(
            {
                "datasource": [4] * 4,
                "time_stamp": [
                    1596139984000,
                    1596139985000,
                    1596139986000,
                    1596139987000,
                ],
                "execution_id": [1, 2, 3, 4],
                "price": [1.1, 1.2, 1.3, 1.4],
                "symbol": ["A/B"] * 4,
                "unit_price": [10000] * 4,
                "price_increment": [0.00001] * 4,
                "account_id": [1] * 4,
                "counterparty_account_id": [None] * 4,
                "instrument_id": [123] * 4,
                "contract_qty": [1, 1, -0.1, -0.5],
                "rate_to_usd": [1] * 4,
                "currency": ["USD"] * 4,
            }
        )

        positions, portfolio_net = load_starting_positions(
            portfolio=Portfolio(), trades=trades
        )

        self.assertEqual(140, positions[None, 123, 1].net_position)
        self.assertEqual(2, positions[None, 123, 1].open_positions.__len__())
        self.assertEqual(
            positions[None, 123, 1].open_positions[0].__dict__,
            OpenPosition(quantity=40, price=1100000).__dict__,
        )
        self.assertEqual(
            positions[None, 123, 1].open_positions[1].__dict__,
            OpenPosition(quantity=100, price=1200000).__dict__,
        )

    # load starting positions into a portfolio

    def test_load_starting_position_into_portfolio_across_one_day(self):
        loader = StartingPositionsParquetLoaderBreakOut(
            loader=LoaderStub(
                trades=GENERIC_TRADES, starting_positions=GENERIC_STARTING_POSITIONS
            )
        )

        (positions, total_net_position,) = load_open_positions(
            loader=loader,
            datasource_label="ldprof",
            start_date=dt.date(2020, 9, 1),
            end_date=dt.date(2020, 9, 1),
            account=[1],
            instrument=[123],
            netting_engine="fifo",
        )

        self.assertEqual(total_net_position, -200)
        self.assertEqual(
            len([x.quantity for x in positions[None, 123, 1].open_positions]), 2
        )
        self.assertEqual(
            sum([x.quantity for x in positions[None, 123, 1].open_positions]), -200
        )

    def test_load_starting_position_into_portfolio_across_two_days(self):
        loader = StartingPositionsParquetLoaderBreakOut(
            loader=LoaderStub(
                trades=GENERIC_TRADES, starting_positions=GENERIC_STARTING_POSITIONS
            )
        )
        loader.portfolio = Portfolio()
        (positions, total_net_position,) = load_open_positions(
            loader=loader,
            datasource_label="ldprof",
            start_date=dt.date(2020, 9, 2),
            end_date=dt.date(2020, 9, 2),
            account=[1],
            instrument=[123],
            netting_engine="fifo",
        )

        self.assertEqual(total_net_position, -260)
        self.assertEqual(
            len([x.quantity for x in positions[None, 123, 1].open_positions]), 4
        )
        self.assertEqual(
            sum([x.quantity for x in positions[None, 123, 1].open_positions]), -260
        )

    def test_load_starting_position_into_portfolio_no_trades_found(self):
        loader = StartingPositionsParquetLoaderBreakOut(
            loader=LoaderStub(
                trades=GENERIC_TRADES, starting_positions=GENERIC_STARTING_POSITIONS
            )
        )

        loader.portfolio = Portfolio()
        (positions, total_net_position,) = load_open_positions(
            loader=loader,
            datasource_label="ldprof",
            start_date=dt.date(2020, 8, 30),
            end_date=dt.date(2020, 8, 30),
            account=[1],
            instrument=[123],
            netting_engine="fifo",
        )

        self.assertEqual(-20, total_net_position)
        self.assertEqual(
            1, len([x.quantity for x in positions[None, 123, 1].open_positions])
        )
        self.assertEqual(
            -20, sum([x.quantity for x in positions[None, 123, 1].open_positions])
        )

    def test_load_open_positions_for_multiple_accounts_on_same_instrument_when_they_start_on_different_days(
            self,
    ):
        trades = pd.DataFrame(
            {
                "datasource": [4] * 4,
                "time_stamp": [
                    dt.datetime(2020, 8, 31, 23).timestamp() * 1000,
                    dt.datetime(2020, 9, 1, 23).timestamp() * 1000,
                    dt.datetime(2020, 9, 2, 23).timestamp() * 1000,
                    dt.datetime(2020, 9, 3, 23).timestamp() * 1000,
                    ],
                "execution_id": [1, 2, 3, 4],
                "shard": ["ldprof"] * 4,
                "price": [0.9, 1.0, 1.1, 1.2],
                "symbol": ["A/B"] * 4,
                "trade_date": [
                    dt.date(2020, 8, 31),
                    dt.date(2020, 9, 1),
                    dt.date(2020, 9, 2),
                    dt.date(2020, 9, 3),
                ],
                "unit_price": [10000] * 4,
                "price_increment": [0.00001] * 4,
                "account_id": [2, 1, 1, 1],
                "counterparty_account_id": [None] * 4,
                "instrument_id": [123] * 4,
                "contract_qty": [-5, -0.5, -1, -1],
                "rate_to_usd": [1] * 4,
                "currency": ["USD"] * 4,
            }
        )

        starting_positions = pd.DataFrame(
            data={
                "datasource": [4] * 8,
                "shard": ["ldprof"] * 8,
                "symbol": ["A/B"] * 8,
                "currency": ["USD"] * 8,
                "instrument_id": [123] * 8,
                "unit_price": [10000] * 8,
                "price_increment": [0.00001] * 8,
                "next_trading_day": [
                                        "2020-09-01",
                                        "2020-09-02",
                                        "2020-09-03",
                                        "2020-09-04",
                                    ]
                                    * 2,
                "account_id": [2] * 4 + [1] * 4,
                "position": [-5, -5, -5, -5, 0, -0.5, -1.5, -2.5],
                "open_cost": [
                    (0.9 * 10000 * -5),  # account 2
                    (0.9 * 10000 * -5),
                    (0.9 * 10000 * -5),
                    (0.9 * 10000 * -5),
                    0,  # account 1
                    (1.0 * 10000 * -0.5),
                    (1.0 * 10000 * -0.5) + (1.1 * 10000 * -1),
                    (1.0 * 10000 * -0.5) + (1.1 * 10000 * -1) + (1.2 * 10000 * -1),
                    ],
            },
            index=[
                      "2020-09-01T00:00:00.000Z",
                      "2020-09-02T00:00:00.000Z",
                      "2020-09-03T00:00:00.000Z",
                      "2020-09-04T00:00:00.000Z",
                  ]
                  * 2,
        )
        loader = StartingPositionsParquetLoaderBreakOut(
            loader=LoaderStub(trades=trades, starting_positions=starting_positions)
        )

        loader.portfolio = Portfolio()
        (positions, total_net_position,) = load_open_positions(
            loader=loader,
            datasource_label="ldprof",
            start_date=dt.date(2020, 9, 4),
            end_date=dt.date(2020, 9, 4),
            account=[1, 2],
            instrument=[123],
            netting_engine="fifo",
        )

        self.assertEqual(-750, total_net_position)

        self.assertEqual(
            3, len([x.quantity for x in positions[None, 123, 1].open_positions])
        )

        self.assertEqual(
            1, len([x.quantity for x in positions[None, 123, 2].open_positions])
        )

    def test_load_open_positions_for_multiple_accounts_on_same_instrument_when_cant_find_the_trades_for_one_account(
            self,
    ):
        # accounts 2 has no trades

        trades = pd.DataFrame(
            {
                "datasource": [4] * 3,
                "time_stamp": [
                    dt.datetime(2020, 9, 1, 23).timestamp() * 1000,
                    dt.datetime(2020, 9, 2, 23).timestamp() * 1000,
                    dt.datetime(2020, 9, 3, 23).timestamp() * 1000,
                    ],
                "execution_id": [1, 2, 3],
                "shard": ["ldprof"] * 3,
                "price": [1.0, 1.1, 1.2],
                "symbol": ["A/B"] * 3,
                "trade_date": [
                    dt.date(2020, 9, 1),
                    dt.date(2020, 9, 2),
                    dt.date(2020, 9, 3),
                ],
                "unit_price": [10000] * 3,
                "price_increment": [0.00001] * 3,
                "account_id": [1, 1, 1],
                "counterparty_account_id": [None] * 3,
                "instrument_id": [123] * 3,
                "contract_qty": [-0.5, -1, -1],
                "rate_to_usd": [1] * 3,
                "currency": ["USD"] * 3,
            }
        )

        starting_positions = pd.DataFrame(
            data={
                "datasource": [4] * 8,
                "shard": ["ldprof"] * 8,
                "symbol": ["A/B"] * 8,
                "currency": ["USD"] * 8,
                "instrument_id": [123] * 8,
                "unit_price": [10000] * 8,
                "price_increment": [0.00001] * 8,
                "next_trading_day": [
                                        "2020-09-01",
                                        "2020-09-02",
                                        "2020-09-03",
                                        "2020-09-04",
                                    ]
                                    * 2,
                "account_id": [2] * 4 + [1] * 4,
                "position": [1, 1, 1, 1, 0, -0.5, -1.5, -2.5],
                "open_cost": [
                    (0.9 * 10000 * 1),  # account 2
                    (0.9 * 10000 * 1),
                    (0.9 * 10000 * 1),
                    (0.9 * 10000 * 1),
                    0,  # account 1
                    (1.0 * 10000 * -0.5),
                    (1.0 * 10000 * -0.5) + (1.1 * 10000 * -1),
                    (1.0 * 10000 * -0.5) + (1.1 * 10000 * -1) + (1.2 * 10000 * -1),
                    ],
            },
            index=[
                      "2020-09-01T00:00:00.000Z",
                      "2020-09-02T00:00:00.000Z",
                      "2020-09-03T00:00:00.000Z",
                      "2020-09-04T00:00:00.000Z",
                  ]
                  * 2,
        )
        loader = StartingPositionsParquetLoaderBreakOut(
            loader=LoaderStub(trades=trades, starting_positions=starting_positions),
            max_days=3,
        )

        loader.portfolio = Portfolio()
        (positions, total_net_position,) = load_open_positions(
            loader=loader,
            datasource_label="ldprof",
            start_date=dt.date(2020, 9, 4),
            end_date=dt.date(2020, 9, 4),
            account=[1, 2],
            instrument=[123],
            netting_engine="fifo",
        )

        self.assertEqual(-150, total_net_position)

        self.assertEqual(
            3, len([x.quantity for x in positions[None, 123, 1].open_positions])
        )

        self.assertEqual(
            1, len([x.quantity for x in positions[None, 123, 2].open_positions])
        )

    def test_load_open_positions_for_single_account_where_starting_position_is_made_up_with_lots_of_small_trades(
            self,
    ):
        trades = pd.DataFrame(
            {
                "datasource": [4] * 12,
                "time_stamp": [
                    dt.datetime(2020, 10, 22, 22).timestamp() * 1000,
                    dt.datetime(2020, 10, 22, 22, 1).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23, 1).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23, 2).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23, 3).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23, 4).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23, 5).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23, 6).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23, 7).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23, 8).timestamp() * 1000,
                    dt.datetime(2020, 10, 23, 23, 9).timestamp() * 1000,
                    ],
                "execution_id": range(1, 13, 1),
                "shard": ["ldprof"] * 12,
                "price": [0.9, 1.0, 1.1, 1.1, 1.1, 1.1, 1.1, 1.2, 1.2, 1.2, 1.2, 1.2],
                "symbol": ["A/B"] * 12,
                "trade_date": [
                    dt.date(2020, 10, 22),
                    dt.date(2020, 10, 22),
                    dt.date(2020, 10, 23),
                    dt.date(2020, 10, 23),
                    dt.date(2020, 10, 23),
                    dt.date(2020, 10, 23),
                    dt.date(2020, 10, 23),
                    dt.date(2020, 10, 23),
                    dt.date(2020, 10, 23),
                    dt.date(2020, 10, 23),
                    dt.date(2020, 10, 23),
                    dt.date(2020, 10, 23),
                ],
                "unit_price": [10000] * 12,
                "price_increment": [0.00001] * 12,
                "account_id": [1] * 12,
                "counterparty_account_id": [None] * 12,
                "instrument_id": [123] * 12,
                "contract_qty": [
                    -5,
                    0.5,
                    0.1,
                    0.1,
                    0.1,
                    0.1,
                    0.1,
                    0.1,
                    0.1,
                    0.1,
                    0.1,
                    0.1,
                ],
                "rate_to_usd": [1] * 12,
                "currency": ["USD"] * 12,
            }
        )

        starting_positions = pd.DataFrame(
            data={
                "datasource": [4] * 2,
                "shard": ["ldprof"] * 2,
                "symbol": ["A/B"] * 2,
                "currency": ["USD"] * 2,
                "instrument_id": [123] * 2,
                "unit_price": [10000] * 2,
                "price_increment": [0.00001] * 2,
                "next_trading_day": ["2020-10-23", "2020-10-26"],
                "account_id": [1] * 2,
                "position": [0, 1],
                "open_cost": [
                    0,
                    (1.1 * 10000 * 0.25)
                    + (1.2 * 10000 * 0.25)
                    + (1.3 * 10000 * 0.25)
                    + (1.4 * 10000 * 0.25),
                    ],
            },
            index=["2020-10-23T00:00:00.000Z", "2020-10-26T00:00:00.000Z",],
        )
        loader = StartingPositionsParquetLoaderBreakOut(
            loader=LoaderStub(trades=trades, starting_positions=starting_positions)
        )

        loader.portfolio = Portfolio()
        (positions, total_net_position,) = load_open_positions(
            loader=loader,
            datasource_label="ldprof",
            start_date=dt.date(2020, 10, 26),
            end_date=dt.date(2020, 10, 26),
            account=[1],
            instrument=[123],
            netting_engine="fifo",
        )

        self.assertEqual(100, total_net_position)

        self.assertEqual(
            10, len([x.quantity for x in positions[None, 123, 1].open_positions])
        )


if __name__ == "__main__":
    unittest.main()
