import datetime as dt
import unittest
from typing import List

# import numpy as np
import pandas as pd

from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.event_stream import create_event_stream, EventStream
from risk_backtesting.loaders.load_price_slippage_model import PriceSlippageLoader
from risk_backtesting.loaders.load_profiling import ProfilingLoader
from risk_backtesting.loaders.load_snapshot import SnapshotLoader, parse_snapshot
from risk_backtesting.loaders.load_starting_positions import StartingPositionsLoader

# from risk_backtesting.loaders.load_starting_positions StartingPositionsLoader
from risk_backtesting.loaders.load_tob import TobLoader
from risk_backtesting.loaders.load_trades import TradesLoader
from risk_backtesting.loaders.dataserver import DataServerLoader
from risk_backtesting.matching_engine import (
    create_matching_engine,
    AbstractMatchingEngine,
)
from risk_backtesting.risk_backtesting_result import (
    BackTestingResults,
    DataFrameAccumulatingBackTestingResults,
)

# from risk_backtesting.simulator.simulation_plan import SimulationPlan
from risk_backtesting.simulator.simulator_pool import SimulatorPool
from risk_backtesting.writers import Writer, create_writer


class TradesDummyLoader(TradesLoader):
    def __init__(self, trades: pd.DataFrame = None):
        super().__init__(None)
        self.trades: pd.DataFrame = trades

    def get_broker_trades(
            self,
            datasource_label: str,
            start_date: dt.date,
            end_date: dt.date,
            account: List,
            instrument: List,
    ):
        trades = self.trades[
            (self.trades.trade_date >= start_date)
            & (self.trades.trade_date <= end_date)
            ].copy()
        trades["timestamp"] = pd.to_datetime(
            trades["timestamp_micros"], unit="us", utc=True
        )
        trades.set_index("timestamp", inplace=True)
        trades.sort_index(inplace=True)
        return trades


class TobDummyLoader(TobLoader):
    def __init__(self):
        super().__init__(None)

    def get_tob(
            self,
            datasource_label: str,
            order_book: List,
            start_date: dt.date,
            end_date: dt.date,
            tier: List,
    ):
        dates = pd.date_range(start_date, end_date)
        time = dt.time(10, 0, 0)
        order_book_details = [
            {
                "ob": order_book[0],
                "ob_name": "A/USD",
                "open_bid_price": 1.10001,
                "open_ask_price": 1.10004,
                "price_inc": 0.00030,
                "unit_price": 10000,
                "rate_to_usd": 1,
            }
        ]

        date_timestamps = [
            int(dt.datetime.timestamp(dt.datetime.combine(x, time)) * 1000)
            for x in dates
        ]
        tob = pd.DataFrame()
        for date, first_tob_ts in zip(dates, date_timestamps):
            for row in order_book_details:
                tob2 = {
                    "datasource": [4] * 3,
                    "order_book_id": [row["ob"]] * 3,
                    "timestamp_micros": [
                        x * 1000
                        for x in [
                            first_tob_ts,
                            first_tob_ts + 1000,
                            first_tob_ts + 2000,
                            ]
                    ],
                    "tier": tier * 3,
                    "bid_price": [
                        row["open_bid_price"],
                        row["open_bid_price"] + row["price_inc"],
                        round(row["open_bid_price"] + (row["price_inc"] * 2), 5),
                        ],
                    "bid_quantity": [10] * 3,
                    "ask_price": [
                        row["open_ask_price"],
                        row["open_ask_price"] + row["price_inc"],
                        round(row["open_ask_price"] + (row["price_inc"] * 2), 5),
                        ],
                    "ask_quantity": [10] * 3,
                    "sequence": range(1, 4, 1),
                    "shard": [datasource_label] * 3,
                    "trading_session": [date] * 3,
                }
                tob = pd.concat([tob, pd.DataFrame(tob2)])
        tob["timestamp"] = pd.to_datetime(tob["timestamp_micros"], unit="us", utc=True)
        tob.set_index("timestamp", inplace=True)
        tob.sort_index(inplace=True)
        return tob

    def get_tob_minute(
            self,
            datasource_label: str,
            order_book: List,
            start_date: dt.date,
            end_date: dt.date,
            tier: List,
            datetimes: List[dt.datetime] = None,
    ):
        dates = pd.date_range(start_date, end_date)
        time = dt.time(10, 0, 0)
        order_book_details = [
            {
                "ob": order_book[0],
                "ob_name": "A/USD",
                "open_bid_price": 1.10001,
                "open_ask_price": 1.10004,
                "price_inc": 0.00030,
                "unit_price": 10000,
                "currency": "USD",
                "rate_to_usd": 1,
            }
        ]

        date_timestamps = [
            int(dt.datetime.timestamp(dt.datetime.combine(x, time)) * 1000)
            for x in dates
        ]
        tob = pd.DataFrame()
        for date, first_tob_ts in zip(dates, date_timestamps):
            for row in order_book_details:
                tob2 = {
                    "datasource": [4] * 3,
                    "order_book_id": [row["ob"]] * 3,
                    "timestamp_micros": [
                        x * 1000
                        for x in [
                            first_tob_ts,
                            first_tob_ts + 1000,
                            first_tob_ts + 2000,
                            ]
                    ],
                    "tier": tier * 3,
                    "bid_price": [
                        row["open_bid_price"],
                        row["open_bid_price"] + row["price_inc"],
                        round(row["open_bid_price"] + (row["price_inc"] * 2), 5),
                        ],
                    "ask_price": [
                        row["open_ask_price"],
                        row["open_ask_price"] + row["price_inc"],
                        round(row["open_ask_price"] + (row["price_inc"] * 2), 5),
                        ],
                    "sequence": range(1, 4, 1),
                    "shard": [datasource_label] * 3,
                    # these are extra columns that need to come from order_book
                    "price_increment": [row["price_inc"]] * 3,
                    "currency": [row["currency"]] * 3,
                    "symbol": [row["ob_name"]] * 3,
                    "unit_price": [row["unit_price"]] * 3,
                    # these are extra columns that need tp come from exchange rates
                    "rate_to_usd": [1] * 3,
                }
                tob = pd.concat([tob, pd.DataFrame(tob2)])
        tob["timestamp"] = pd.to_datetime(tob["timestamp_micros"], unit="us", utc=True)
        tob.set_index("timestamp", inplace=True)
        tob.sort_index(inplace=True)
        return tob


class SnapshotDummyLoader(SnapshotLoader):
    def __init__(self, snapshot: pd.DataFrame = None):
        self.snapshot: pd.DataFrame = snapshot

    def get_liquidity_profile_snapshot(
            self,
            datasource_label: str,
            start_date: dt.date,
            end_date: dt.date,
            instruments: list = None,
            internalisation_account_id: int = None,
            restrict_to_bbooked: bool = False,
    ):
        snapshot = self.snapshot.copy()

        if internalisation_account_id:
            snapshot = snapshot[
                snapshot.internalisation_account_id == internalisation_account_id
                ]

        if instruments:
            snapshot = snapshot[snapshot.instrument_id.isin(instruments)]

        snapshot = parse_snapshot(snapshot)

        return snapshot


class ProfilingDummyLoader(ProfilingLoader):
    def __init__(self, profiles: pd.DataFrame = None):
        self.profiles = profiles

    def load_closed_positions(
            self,
            datasource_label: str,
            start_date: dt.date,
            end_date: dt.date,
            target_accounts: pd.DataFrame,
    ):
        closed_positions = self.profiles

        if not target_accounts.empty:
            closed_positions = closed_positions.join(
                target_accounts.set_index("account_id"), how="inner"
            )

        return closed_positions


class DataServerDummyLoader:
    def __init__(self, closing_prices: pd.DataFrame = pd.DataFrame()):
        self.closing_prices: pd.DataFrame = closing_prices

    @staticmethod
    def get_order_book_details(shard: str, instruments: List[int]):
        return pd.DataFrame(
            {"order_book_id": [12345], "contract_unit_of_measure": ["A"]}
        )

    @staticmethod
    def get_opening_positions(
            datasource_label, start_date, end_date, instruments, accounts
    ):
        pass

    def get_closing_prices(self, shard, start_date, end_date, instruments):
        return self.closing_prices


class StartingPositionsDummyLoader(StartingPositionsLoader):
    def __init__(self, starting_positions: pd.DataFrame):
        self.starting_positions = starting_positions

    def get_opening_positions(
            self,
            datasource_label: str,
            start_date: dt.date,
            end_date: dt.date,
            instruments: List,
            accounts: List,
            schema=None,
    ):
        return self.starting_positions


class TestSimulatorPool(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def run_simulator(
            self,
            config: BackTestingConfig,
            trades: pd.DataFrame = pd.DataFrame(),
            snapshot: pd.DataFrame = pd.DataFrame(),
            profiles: pd.DataFrame = pd.DataFrame(),
            closing_prices: pd.DataFrame = pd.DataFrame(),
            starting_positions: pd.DataFrame = pd.DataFrame(),
    ) -> pd.DataFrame:

        event_stream: EventStream = create_event_stream(config.event_stream_params)
        matching_engine: AbstractMatchingEngine = create_matching_engine(
            config.matching_engine_params, config.matching_method
        )
        # noinspection PyTypeChecker
        writer: Writer = create_writer(config.output.filesystem_type, None)

        simulator: SimulatorPool = SimulatorPool(
            tob_loader=TobDummyLoader(),
            trades_loader=TradesDummyLoader(trades=trades),
            starting_positions_loader=StartingPositionsDummyLoader(
                starting_positions=starting_positions
            ),
            price_slippage_loader=PriceSlippageLoader(),
            event_stream=event_stream,
            dataserver=DataServerDummyLoader(closing_prices=closing_prices),
            snapshot_loader=SnapshotDummyLoader(snapshot=snapshot),
            profiling_loader=ProfilingDummyLoader(profiles=profiles),
        )

        results: BackTestingResults = DataFrameAccumulatingBackTestingResults()

        # noinspection PyTypeChecker
        simulator.start_simulator(
            config,
            results_cache=None,
            writer=writer,
            matching_engine=matching_engine,
            simulation_configs=(config.simulation_configs),
            results=results,
        )

        return results.df


class TestSimulatorPoolForInternalisationStrategy(TestSimulatorPool):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single_day_simulations_with_eod_snapshot(self):
        # Load Data
        date_str: str = "2019-12-05"

        # Setup Config
        pipeline_config = {
            "lmax_account": 12345678,
            "load_starting_positions": False,
            "load_client_starting_positions": False,
            "level": "trades_only",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": True,
            "shard": "ldprof",
            "process_client_portfolio": True,
            "store_client_trade_snapshot": False,
            "process_lmax_portfolio": True,
            "store_lmax_trade_snapshot": True,
            "store_lmax_eod_snapshot": True,
            "event_stream_parameters": {
                "event_stream_type": "event_stream_snapshot",
                "sample_rate": "s",
                "excl_period": [[21, 30], [23, 30]],
                "include_eod_snapshot": True,
            },
            "matching_engine_parameters": {
                "matching_engine_type": "matching_engine_default"
            },
        }
        simulations_config = {
            "sim_1": {
                "instruments": [12345],
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty": 300,
                    "allow_partial_fills": True,
                    "max_pos_qty_type": "contracts",
                    "max_pos_qty_rebalance_rate": None,
                    "max_pos_qty_buffer": 1,
                    "position_lifespan": None,
                },
                "exit_parameters": {"exit_type": "exit_default",},
                "constructor": "zip",
            },
        }
        output_config = {
            "resample_rule": None,
            "to_csv": True,
            "save": False,
            "by": None,
            "filesystem": "local",
            "freq": None,
            "directory": "/home/jovyan/work/outputs",
            "event_features": ["symbol", "order_book_id", "account_id"],
            "filename": None,
        }

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1, 2,]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        # create trades
        dates = pd.date_range(date_str, date_str)
        time = dt.time(10, 0, 0)
        date_timestamps = [
            int(dt.datetime.timestamp(dt.datetime.combine(x, time)) * 1000)
            for x in dates
        ]
        trades = pd.DataFrame()
        for date, first_tob_ts in zip(dates, date_timestamps):
            date = date.date()
            trades_times = [first_tob_ts + 500]
            trades_for_ob = {
                "datasource": [4],
                "shard": ["ldprof"],
                "timestamp_micros": [x * 1000 for x in trades_times],
                "time_stamp": trades_times,
                "trade_date": [date],
                "trading_session": [date],
                "execution_id": [1],
                "order_id": [1],
                "immediate_order": [1],
                "order_type": ["M"],
                "time_in_force": ["I"],
                "order_book_id": [12345],
                "instrument_id": [12345],
                "symbol": ["A/USD"],
                "price": [1.10004],
                "notional_value": [1.10004 * 1e6],
                "notional_value_usd": [1.10004 * 1e6],
                "unit_quantity": [1 * 10000],
                "rate_to_usd": [1],
                "contract_qty": [1],
                "order_qty": [1],
                "unit_price": [10000],
                "currency": ["USD"],
                "contract_unit_of_measure": ["A"],
                "broker_id": [10],
                "account_id": [1],
                "price_increment": [1e-05],
                "event_type": ["trade"],
            }
            trades = pd.concat([trades, pd.DataFrame(trades_for_ob)])

        closing_prices: pd.DataFrame = pd.DataFrame(
            {
                "trading_session": [date],
                "order_book_id": [12345],
                "symbol": ["A/USD"],
                "unit_price": [10000],
                "price": [1.10006],
                "rate_to_usd": [1],
                "currency": ["USD"],
                "contract_unit_of_measure": ["A"],
            }
        )

        closing_prices = DataServerLoader.parse_closing_prices(closing_prices)

        results: pd.DataFrame = self.run_simulator(
            config, trades=trades, closing_prices=closing_prices
        )

        self.assertEqual(results["venue"].values.tolist(), [1] * 2)
        self.assertEqual(results["symbol"].values.tolist(), ["A/USD"] * 2)
        self.assertEqual(results["currency"].values.tolist(), ["USD"] * 2)
        self.assertEqual(results["order_book_id"].values.tolist(), [12345] * 2)
        # self.assertEqual([results["trade_price"].values.tolist()], [1.10004, np.nan])
        self.assertEqual(results["net_qty"].values.tolist(), [-1.0, -1.0])
        self.assertEqual(results["inventory_contracts"].values.tolist(), [-1.0, 0.0])
        self.assertEqual(results["inventory_dollars"].values.tolist(), [-11000.4, 0.0])
        self.assertEqual(results["trade_qty"].values.tolist(), [-1.0, 0.0])
        self.assertEqual(results["is_long"].values.tolist(), [0, -1])
        self.assertEqual(results["upnl"].values.tolist(), [0.0, -0.2])
        self.assertEqual(results["rpnl_cum"].values.tolist(), [0.0, 0.0])
        # self.assertEqual(
        #     results["notional_traded"].values.tolist(), [11000.4, None]
        # )
        # self.assertEqual(
        #     results["notional_traded_cum"].values.tolist(), [11000.4, None]
        # )
        self.assertEqual(results["account_id"].values.tolist(), [12345678] * 2)
        # self.assertEqual(results["counterparty_account_id"].values.tolist(), [1, None])
        # self.assertEqual(
        #     results["action"].values.tolist(),
        #     ["client_long_open_short", None],
        # )
        self.assertEqual(results["type"].values.tolist(), ["internal", "closing_price"])
        self.assertEqual(results["portfolio"].values.tolist(), ["lmax"] * 2)
        self.assertEqual(
            results["trading_session"].values.tolist(),
            [
                int(
                    dt.datetime.strptime("2019-12-05", "%Y-%m-%d").timestamp()
                    * 1000000000
                )
            ]
            * 2,
            )
        self.assertEqual(results["rpnl"].values.tolist(), [0.0, 0.0])
        self.assertEqual(results["simulation"].values.tolist(), ["sim_1", "sim_1"])
        self.assertEqual(
            results["strategy_name"].values.tolist(), ["internalisation"] * 2
        )
        self.assertEqual(results["strategy_max_pos_qty"].values.tolist(), [300, 300])
        self.assertEqual(results["strategy_max_pos_qty_buffer"].values.tolist(), [1, 1])
        self.assertEqual(
            results["strategy_position_lifespan"].values.tolist(), [-1] * 2
        )
        self.assertEqual(results["exit_name"].values.tolist(), ["exit_default"] * 2)
        self.assertEqual(results["risk_name"].values.tolist(), ["no_risk"] * 2)

    # # run simulation
    #
    # def test_evaluation_string_for_simulation_excludes_account(self):
    #
    #     # Load Data
    #     date_str: str = "2020-08-05"
    #     date: dt.date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    #
    #     # tob
    #     start = dt.datetime(2020, 8, 5, 21, 0, 0, 0).timestamp() * 1000000
    #     end = dt.datetime(2020, 8, 6, 21, 0, 0, 0).timestamp() * 1000000
    #     microseconds = range(int(start), int(end), 1000000)
    #     rows = len(microseconds)
    #     bid_price = [np.random.uniform(1, 1.5) for i in range(rows)]
    #     ask_price = [x + 0.00001 for x in bid_price]
    #
    #     tob = {
    #         "datasource": [4] * rows,
    #         "order_book_id": [123] * rows,
    #         "timestamp_micros": microseconds,
    #         "tier": [1] * rows,
    #         "bid_price": bid_price,
    #         "bid_qty": [np.random.randint(10, 100) for i in range(rows)],
    #         "ask_price": ask_price,
    #         "ask_qty": [np.random.randint(10, 100) for i in range(rows)],
    #         "shard": ["ldprof"] * rows,
    #         "trading_session": [date] * rows,
    #         "event_type": ["market_data"] * rows,
    #     }
    #
    #     tob = pd.DataFrame(tob)
    #     tob["timestamp"] = pd.to_datetime(tob["timestamp_micros"], unit="us", utc=True)
    #     tob.set_index("timestamp", inplace=True)
    #     tob.sort_index(inplace=True)
    #
    #     # trades
    #     no_of_trades = 400
    #     trade_indexes = np.linspace(2, rows - 1, num=no_of_trades, dtype=int)
    #     contract_qty = [np.random.randint(-10, 10) for i in range(no_of_trades)]
    #     contract_qty = [x if x != 0 else 1 for x in contract_qty]
    #     timestamp_millis = [(microseconds[x] / 1000) + 1 for x in trade_indexes]
    #     price = [
    #         ask_price[t] if np.sign(c) == 1 else bid_price[t]
    #         for (t, c) in zip(trade_indexes, contract_qty)
    #     ]
    #     notional_value = [p * c * 10000 for (p, c) in zip(price, contract_qty)]
    #     account_id = [np.random.randint(1, 10) for i in range(no_of_trades)]
    #
    #     trades_for_ob = {
    #         "datasource": [4] * no_of_trades,
    #         "shard": ["ldprof"] * no_of_trades,
    #         "time_stamp": timestamp_millis,
    #         "timestamp_micros": timestamp_millis,
    #         "trading_session": [date] * no_of_trades,
    #         "trade_date": [date] * no_of_trades,
    #         "execution_id": range(1, no_of_trades + 1, 1),
    #         "order_id": range(1, no_of_trades + 1, 1),
    #         "immediate_order": [1] * no_of_trades,
    #         "order_type": ["M"] * no_of_trades,
    #         "time_in_force": ["I"] * no_of_trades,
    #         "order_book_id": [123] * no_of_trades,
    #         "instrument_id": [123] * no_of_trades,
    #         "symbol": ["A/B"] * no_of_trades,
    #         "tob_snapshot_bid_price": [bid_price[x] + 1000 for x in trade_indexes],
    #         "tob_snapshot_ask_price": [ask_price[x] + 1000 for x in trade_indexes],
    #         "bid_adjustment": [0] * no_of_trades,
    #         "ask_adjustment": [0] * no_of_trades,
    #         "price": price,
    #         "notional_value": notional_value,
    #         "notional_value_usd": notional_value,
    #         "unit_quantity": [x * 10000 for x in contract_qty],
    #         "rate_to_usd": [1] * no_of_trades,
    #         "contract_qty": contract_qty,
    #         "order_qty": contract_qty,
    #         "unit_price": [10000] * no_of_trades,
    #         "currency": ["USD"] * no_of_trades,
    #         "account_id": account_id,
    #         "price_increment": [1e-05] * no_of_trades,
    #         "event_type": ["trade"] * no_of_trades,
    #     }
    #     trades = pd.DataFrame(trades_for_ob)
    #
    #     # Setup Config
    #     pipeline_config = {
    #         "lmax_account": 1463064262,
    #         "load_starting_positions": False,
    #         "level": "mark_to_market",
    #         "netting_engine": "fifo",
    #         "matching_method": "side_of_book",
    #         "simulator": "simulator_pool",
    #         "calculate_cumulative_daily_pnl": False,
    #         "shard": "ldprof",
    #         "process_client_portfolio": False,
    #         "store_client_trade_snapshot": False,
    #         "process_lmax_portfolio": True,
    #         "store_lmax_trade_snapshot": True,
    #         "event_stream_parameters": {
    #             "event_stream_type": "event_stream_snapshot",
    #             "sample_rate": "s",
    #             "excl_period": [[21, 30], [23, 30]],
    #         },
    #         "matching_engine_parameters": {
    #             "matching_engine_type": "matching_engine_default"
    #         },
    #         "simulation_parameters": {
    #             "sim_1": {
    #                 "instruments": [123],
    #                 "event_filter_string": "account_id != 9",
    #                 "strategy_parameters": {
    #                     "strategy_type": "internalisation",
    #                     "max_pos_qty_buffer": 1.25,
    #                     "max_pos_qty": 100,
    #                     "position_lifespan": "gfw",
    #                     "position_lifespan_exit_parameters": {
    #                         "exit_type": "exit_default"
    #                     },
    #                 },
    #                 "exit_parameters": {
    #                     "exit_type": "aggressive",
    #                     "takeprofit_limit": 50,
    #                     "stoploss_limit": 70,
    #                 },
    #                 "risk_parameters": {"risk_type": "no_risk"},
    #             },
    #         },
    #     }
    #     simulations_config = {
    #         "sim_1": {
    #             "instruments": [123],
    #             "event_filter_string": "account_id != 9",
    #             "strategy_parameters": {
    #                 "strategy_type": "internalisation",
    #                 "max_pos_qty_buffer": 1.25,
    #                 "max_pos_qty": 100,
    #                 "position_lifespan": "gfw",
    #                 "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
    #             },
    #             "exit_parameters": {
    #                 "exit_type": "aggressive",
    #                 "takeprofit_limit": 50,
    #                 "stoploss_limit": 70,
    #             },
    #             "risk_parameters": {"risk_type": "no_risk"},
    #         },
    #     }
    #     output_config = {
    #         "resample_rule": None,
    #         "to_csv": True,
    #         "save": False,
    #         "by": None,
    #         "filesystem": "local",
    #         "freq": None,
    #         "directory": "/home/jovyan/work/outputs",
    #         "event_features": ["symbol", "order_book_id", "trading_session",],
    #         "filename": None,
    #     }
    #
    #     # noinspection PyTypeChecker
    #     config = BackTestingConfig(
    #         auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
    #     )
    #     config.optionally_override_running_config_parameters(
    #         start_date=date_str, end_date=date_str
    #     )
    #     config.optionally_set_target_accounts(
    #         target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
    #     )
    #     config.build_simulations_config(simulations_config, [])
    #     config.validate()
    #
    #     simulator: SimulatorPool = SimulatorPool(
    #         tob_loader=TobDummyLoader(),
    #         trades_loader=TradesDummyLoader(trades),
    #         starting_positions_loader=StartingPositionsLoader(),
    #         price_slippage_loader=PriceSlippageLoader(),
    #         event_stream=create_event_stream(config.event_stream_params),
    #     )
    #
    #     plan: SimulationPlan = simulator.create_simulation_plans(
    #         config,
    #         create_matching_engine(
    #             config.matching_engine_params, config.matching_method
    #         ),
    #         config.simulation_configs,
    #     )[0]
    #
    #     simulator.tob = tob
    #     results = simulator.run_simulation(plan).payload
    #
    #     # run it again with target accounts that dont include account 9 and no event_filter_string so we can check the results match above
    #     del simulations_config["sim_1"]["event_filter_string"]
    #
    #     # noinspection PyTypeChecker
    #     config = BackTestingConfig(
    #         auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
    #     )
    #     config.optionally_override_running_config_parameters(
    #         start_date=date_str, end_date=date_str
    #     )
    #     config.optionally_set_target_accounts(
    #         target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8]})
    #     )
    #     config.build_simulations_config(simulations_config, [])
    #     config.validate()
    #
    #     simulator: SimulatorPool = SimulatorPool(
    #         tob_loader=TobDummyLoader(),
    #         trades_loader=TradesDummyLoader(trades),
    #         starting_positions_loader=StartingPositionsLoader(),
    #         price_slippage_loader=PriceSlippageLoader(),
    #         event_stream=create_event_stream(config.event_stream_params),
    #     )
    #
    #     plan = simulator.create_simulation_plans(
    #         config,
    #         create_matching_engine(
    #             config.matching_engine_params, config.matching_method
    #         ),
    #         config.simulation_configs,
    #     )[0]
    #     simulator.tob = tob
    #     results2 = simulator.run_simulation(plan).payload
    #
    #     self.assertEqual(results2.rpnl.sum(), results.rpnl.sum())
    #     self.assertTrue(9 not in results.counterparty_account_id.unique())
    #     self.assertTrue(9 not in results2.counterparty_account_id.unique())
    #
    # # tests for start_simulator
    #
    # def test_single_day_simulations_on_order_book_store_lmax_snapshot(self):
    #     # Load Data
    #     date_str: str = "2019-12-05"
    #
    #     # Setup Config
    #     pipeline_config = {
    #         "lmax_account": 1463064262,
    #         "load_starting_positions": False,
    #         "level": "mark_to_market",
    #         "netting_engine": "fifo",
    #         "matching_method": "side_of_book",
    #         "simulator": "simulator_pool",
    #         "calculate_cumulative_daily_pnl": False,
    #         "shard": "ldprof",
    #         "process_client_portfolio": False,
    #         "store_client_trade_snapshot": False,
    #         "process_lmax_portfolio": True,
    #         "store_lmax_trade_snapshot": True,
    #         "event_stream_parameters": {
    #             "event_stream_type": "event_stream_snapshot",
    #             "sample_rate": "s",
    #             "excl_period": [[21, 30], [23, 30]],
    #         },
    #         "matching_engine_parameters": {
    #             "matching_engine_type": "matching_engine_default"
    #         },
    #     }
    #     simulations_config = {
    #         "sim_1": {
    #             "instruments": [12345],
    #             "strategy_parameters": {
    #                 "strategy_type": "internalisation",
    #                 "max_pos_qty_buffer": 1.25,
    #                 "max_pos_qty": 100,
    #                 "position_lifespan": "gfw",
    #                 "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
    #             },
    #             "exit_parameters": {
    #                 "exit_type": "aggressive",
    #                 "takeprofit_limit": 50,
    #                 "stoploss_limit": 70,
    #             },
    #             "risk_parameters": {"risk_type": "no_risk"},
    #         }
    #     }
    #     output_config = {
    #         "resample_rule": None,
    #         "to_csv": True,
    #         "save": False,
    #         "by": None,
    #         "filesystem": "local",
    #         "freq": None,
    #         "directory": "/home/jovyan/work/outputs",
    #         "event_features": ["symbol", "order_book_id", "trading_session",],
    #         "filename": None,
    #     }
    #
    #     # noinspection PyTypeChecker
    #     config = BackTestingConfig(
    #         auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
    #     )
    #     config.optionally_override_running_config_parameters(
    #         start_date=date_str, end_date=date_str
    #     )
    #     config.optionally_set_target_accounts(
    #         target_accounts=pd.DataFrame(
    #             {
    #                 "account_id": [
    #                     1,
    #                     2,
    #                     3,
    #                     4,
    #                     5,
    #                     6,
    #                     7,
    #                     8,
    #                     9,
    #                     10,
    #                     11,
    #                     12,
    #                     13,
    #                     14,
    #                     15,
    #                     16,
    #                     17,
    #                     18,
    #                     19,
    #                     20,
    #                 ]
    #             }
    #         )
    #     )
    #     config.build_simulations_config(simulations_config, [])
    #     config.validate()
    #
    #     # create trades
    #     dates = pd.date_range(date_str, date_str)
    #     time = dt.time(10, 0, 0)
    #     date_timestamps = [
    #         int(dt.datetime.timestamp(dt.datetime.combine(x, time)) * 1000)
    #         for x in dates
    #     ]
    #     trades = pd.DataFrame()
    #     for date, first_tob_ts in zip(dates, date_timestamps):
    #         date = date.date()
    #         trades_times = [
    #             first_tob_ts + 500,
    #             first_tob_ts + 1500,
    #             first_tob_ts + 1600,
    #         ]
    #         trades_for_ob = {
    #             "datasource": [4] * 3,
    #             "shard": ["ldprof"] * 3,
    #             "timestamp_micros": [x * 1000 for x in trades_times],
    #             "time_stamp": trades_times,
    #             "trade_date": [date] * 3,
    #             "trading_session": [date] * 3,
    #             "execution_id": range(1, 4, 1),
    #             "mtf_execution_id": ["a", "b", "c"],
    #             "order_id": range(1, 4, 1),
    #             "immediate_order": [1] * 3,
    #             "order_type": ["M"] * 3,
    #             "time_in_force": ["I"] * 3,
    #             "order_book_id": [12345] * 3,
    #             "instrument_id": [12345] * 3,
    #             "symbol": ["A/USD"] * 3,
    #             "tob_snapshot_bid_price": [
    #                 1.10004,
    #                 1.10001 + (0.00030 * 1),
    #                 1.10001 + (0.00030 * 1),
    #             ],
    #             "tob_snapshot_ask_price": [
    #                 1.10004,
    #                 1.10001 + (0.00030 * 1),
    #                 1.10001 + (0.00030 * 1),
    #             ],
    #             "bid_adjustment": [0] * 3,
    #             "ask_adjustment": [0] * 3,
    #             "price": [1.10004, 1.10001 + (0.00030 * 1), 1.10001 + (0.00030 * 1),],
    #             "notional_value": [
    #                 x * 10000
    #                 for x in [
    #                     1.10004,
    #                     1.10001 + (0.00030 * 1),
    #                     1.10001 + (0.00030 * 1),
    #                 ]
    #             ],
    #             "notional_value_usd": [
    #                 x * 10000 * 1
    #                 for x in [
    #                     1.10004,
    #                     1.10001 + (0.00030 * 1),
    #                     1.10001 + (0.00030 * 1),
    #                 ]
    #             ],
    #             "unit_quantity": [x * 10000 for x in [1, -1, -1]],
    #             "rate_to_usd": [1] * 3,
    #             "contract_qty": [1, -1, -1],
    #             "order_qty": [1, -1, -1],
    #             "unit_price": [10000] * 3,
    #             "currency": ["USD"] * 3,
    #             "broker_id": [10] * 3,
    #             "account_id": [1, 1, 100],
    #             "price_increment": [1e-05] * 3,
    #             "event_type": ["trade"] * 3,
    #         }
    #         trades = pd.concat([trades, pd.DataFrame(trades_for_ob)])
    #
    #     results: pd.DataFrame = self.run_simulator(config, trades)
    #
    #     self.assertEqual(results["venue"].values.tolist(), [1] * 2)
    #     self.assertEqual(results["symbol"].values.tolist(), ["A/USD"] * 2)
    #     self.assertEqual(results["currency"].values.tolist(), ["USD"] * 2)
    #     self.assertEqual(results["order_book_id"].values.tolist(), [12345] * 2)
    #     self.assertEqual(results["price"].values.tolist(), [1.10001, 1.10031])
    #     self.assertEqual(results["net_qty"].values.tolist(), [-1.0, 0.0])
    #     self.assertEqual(results["inventory"].values.tolist(), [-1.0, 0.0])
    #     self.assertEqual(results["trade_qty"].values.tolist(), [-1.0, 1.0])
    #     self.assertEqual(results["is_long"].values.tolist(), [0, 1])
    #     self.assertEqual(results["upnl"].values.tolist(), [0.0] * 2)
    #     self.assertEqual(results["rpnl_cum"].values.tolist(), [0.0, -2.7])
    #     self.assertEqual(
    #         results["notional_traded_cum"].values.tolist(), [11000.4, 22003.5]
    #     )
    #     self.assertEqual(results["account_id"].values.tolist(), [1463064262] * 2)
    #     self.assertEqual(results["counterparty_account_id"].values.tolist(), [1] * 2)
    #     self.assertEqual(
    #         results["action"].values.tolist(),
    #         ["client_long_open_short", "client_short_open_long"],
    #     )
    #     self.assertEqual(results["type"].values.tolist(), ["internal"] * 2)
    #     self.assertEqual(results["portfolio"].values.tolist(), ["lmax"] * 2)
    #     self.assertEqual(
    #         results["trading_session"].values.tolist(),
    #         [
    #             int(
    #                 dt.datetime.strptime("2019-12-05", "%Y-%m-%d").timestamp()
    #                 * 1000000000
    #             )
    #         ]
    #         * 2,
    #     )
    #     self.assertEqual(results["rpnl"].values.tolist(), [0.0, -2.7])
    #     self.assertEqual(results["notional_traded"].values.tolist(), [11000.4, 11003.1])
    #     self.assertEqual(results["simulation"].values.tolist(), ["sim_1", "sim_1"])
    #     self.assertEqual(
    #         results["strategy_name"].values.tolist(), ["internalisation"] * 2
    #     )
    #     self.assertEqual(results["strategy_max_pos_qty"].values.tolist(), [100, 100])
    #     self.assertEqual(
    #         results["strategy_max_pos_qty_buffer"].values.tolist(), [1.25, 1.25]
    #     )
    #     self.assertEqual(
    #         results["strategy_position_lifespan"].values.tolist(), ["gfw"] * 2
    #     )
    #     self.assertEqual(results["exit_name"].values.tolist(), ["aggressive"] * 2)
    #     self.assertEqual(results["exit_stoploss_limit"].values.tolist(), [70] * 2)
    #     self.assertEqual(results["exit_takeprofit_limit"].values.tolist(), [50] * 2)
    #     self.assertEqual(results["risk_name"].values.tolist(), ["no_risk"] * 2)
    #
    # def test_single_day_multiple_simulations_on_single_order_book_store_client_lmax_snapshot(
    #     self,
    # ):
    #     # Load Data
    #     date_str: str = "2019-12-05"
    #
    #     # Setup Config
    #     pipeline_config = {
    #         "strategy": "internalisation",
    #         "lmax_account": 1463064262,
    #         "load_starting_positions": False,
    #         "level": "mark_to_market",
    #         "netting_engine": "fifo",
    #         "matching_method": "side_of_book",
    #         "simulator": "simulator_pool",
    #         "calculate_cumulative_daily_pnl": False,
    #         "shard": "ldprof",
    #         "process_client_portfolio": False,
    #         "store_client_trade_snapshot": False,
    #         "process_lmax_portfolio": True,
    #         "store_lmax_trade_snapshot": True,
    #         "event_stream_parameters": {
    #             "event_stream_type": "event_stream_snapshot",
    #             "sample_rate": "s",
    #             "excl_period": [[21, 30], [23, 30]],
    #         },
    #         "matching_engine_parameters": {
    #             "matching_engine_type": "matching_engine_default"
    #         },
    #     }
    #     simulations_config = {
    #         "sim_1": {
    #             "instruments": [12345],
    #             "strategy_parameters": {
    #                 "strategy_type": "internalisation",
    #                 "max_pos_qty_buffer": 1.25,
    #                 "max_pos_qty": [100, 200],
    #                 "position_lifespan": "gfw",
    #                 "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
    #             },
    #             "exit_parameters": {
    #                 "exit_type": "aggressive",
    #                 "takeprofit_limit": 50,
    #                 "stoploss_limit": 70,
    #             },
    #             "risk_parameters": {"risk_type": "no_risk"},
    #             "constructor": "zip",
    #         }
    #     }
    #     output_config = {
    #         "resample_rule": None,
    #         "to_csv": True,
    #         "save": False,
    #         "by": None,
    #         "csv_per_simulation": False,
    #         "filesystem": "local",
    #         "directory": "/home/jovyan/work/outputs",
    #         "event_features": ["symbol", "order_book_id", "trading_session",],
    #         "filename": None,
    #     }
    #
    #     # create trades
    #     dates = pd.date_range(date_str, date_str)
    #     time = dt.time(10, 0, 0)
    #     date_timestamps = [
    #         int(dt.datetime.timestamp(dt.datetime.combine(x, time)) * 1000)
    #         for x in dates
    #     ]
    #     trades = pd.DataFrame()
    #     for date, first_tob_ts in zip(dates, date_timestamps):
    #         date = date.date()
    #         trades_times = [
    #             first_tob_ts + 500,
    #             first_tob_ts + 1500,
    #             first_tob_ts + 1600,
    #         ]
    #         trades_for_ob = {
    #             "datasource": [4] * 3,
    #             "shard": ["ldprof"] * 3,
    #             "timestamp_micros": [x * 1000 for x in trades_times],
    #             "time_stamp": trades_times,
    #             "trade_date": [date] * 3,
    #             "trading_session": [date] * 3,
    #             "execution_id": range(1, 4, 1),
    #             "mtf_execution_id": ["a", "b", "c"],
    #             "order_id": range(1, 4, 1),
    #             "immediate_order": [1] * 3,
    #             "order_type": ["M"] * 3,
    #             "time_in_force": ["I"] * 3,
    #             "order_book_id": [12345] * 3,
    #             "instrument_id": [12345] * 3,
    #             "symbol": ["A/USD"] * 3,
    #             "tob_snapshot_bid_price": [
    #                 1.10004,
    #                 1.10001 + (0.00030 * 1),
    #                 1.10001 + (0.00030 * 1),
    #             ],
    #             "tob_snapshot_ask_price": [
    #                 1.10004,
    #                 1.10001 + (0.00030 * 1),
    #                 1.10001 + (0.00030 * 1),
    #             ],
    #             "bid_adjustment": [0] * 3,
    #             "ask_adjustment": [0] * 3,
    #             "price": [1.10004, 1.10001 + (0.00030 * 1), 1.10001 + (0.00030 * 1),],
    #             "notional_value": [
    #                 x * 10000
    #                 for x in [
    #                     1.10004,
    #                     1.10001 + (0.00030 * 1),
    #                     1.10001 + (0.00030 * 1),
    #                 ]
    #             ],
    #             "notional_value_usd": [
    #                 x * 10000 * 1
    #                 for x in [
    #                     1.10004,
    #                     1.10001 + (0.00030 * 1),
    #                     1.10001 + (0.00030 * 1),
    #                 ]
    #             ],
    #             "unit_quantity": [x * 10000 for x in [1, -1, -1]],
    #             "rate_to_usd": [1] * 3,
    #             "contract_qty": [1, -1, -1],
    #             "order_qty": [1, -1, -1],
    #             "unit_price": [10000] * 3,
    #             "currency": ["USD"] * 3,
    #             "broker_id": [10] * 3,
    #             "account_id": [1, 1, 100],
    #             "price_increment": [1e-05] * 3,
    #             "event_type": ["trade"] * 3,
    #         }
    #         trades = pd.concat([trades, pd.DataFrame(trades_for_ob)])
    #
    #     # noinspection PyTypeChecker
    #     config = BackTestingConfig(
    #         auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
    #     )
    #     config.optionally_override_running_config_parameters(
    #         start_date=date_str, end_date=date_str
    #     )
    #     config.optionally_set_target_accounts(
    #         target_accounts=pd.DataFrame(
    #             {
    #                 "account_id": [
    #                     1,
    #                     2,
    #                     3,
    #                     4,
    #                     5,
    #                     6,
    #                     7,
    #                     8,
    #                     9,
    #                     10,
    #                     11,
    #                     12,
    #                     13,
    #                     14,
    #                     15,
    #                     16,
    #                     17,
    #                     18,
    #                     19,
    #                     20,
    #                 ]
    #             }
    #         )
    #     )
    #     config.build_simulations_config(simulations_config, [])
    #     config.validate()
    #
    #     results: pd.DataFrame = self.run_simulator(config, trades)
    #
    #     self.assertEqual(results["venue"].values.tolist(), [1] * 4)
    #     self.assertEqual(results["symbol"].values.tolist(), ["A/USD"] * 4)
    #     self.assertEqual(results["currency"].values.tolist(), ["USD"] * 4)
    #     self.assertEqual(results["order_book_id"].values.tolist(), [12345] * 4)
    #     self.assertEqual(results["price"].values.tolist(), [1.10001, 1.10031] * 2)
    #     self.assertEqual(results["net_qty"].values.tolist(), [-1.0, 0.0] * 2)
    #     self.assertEqual(results["inventory"].values.tolist(), [-1.0, 0.0] * 2)
    #     self.assertEqual(results["trade_qty"].values.tolist(), [-1.0, 1.0] * 2)
    #     self.assertEqual(results["is_long"].values.tolist(), [0, 1] * 2)
    #     self.assertEqual(results["upnl"].values.tolist(), [0.0] * 4)
    #     self.assertEqual(results["rpnl_cum"].values.tolist(), [0.0, -2.7] * 2)
    #     self.assertEqual(
    #         results["notional_traded_cum"].values.tolist(), [11000.4, 22003.5] * 2
    #     )
    #     self.assertEqual(results["account_id"].values.tolist(), [1463064262] * 4)
    #     self.assertEqual(
    #         results["action"].values.tolist(),
    #         ["client_long_open_short", "client_short_open_long"] * 2,
    #     )
    #     self.assertEqual(results["type"].values.tolist(), ["internal"] * 4)
    #     self.assertEqual(results["portfolio"].values.tolist(), ["lmax"] * 4)
    #     self.assertEqual(
    #         results["trading_session"].values.tolist(),
    #         [
    #             int(
    #                 dt.datetime.strptime("2019-12-05", "%Y-%m-%d").timestamp()
    #                 * 1000000000
    #             )
    #         ]
    #         * 4,
    #     )
    #     self.assertEqual(results["rpnl"].values.tolist(), [0.0, -2.7] * 2)
    #     self.assertEqual(
    #         results["notional_traded"].values.tolist(), [11000.4, 11003.1] * 2
    #     )
    #     self.assertEqual(results["simulation"].values.tolist(), ["sim_1", "sim_1"] * 2)
    #     self.assertEqual(
    #         results["strategy_name"].values.tolist(), ["internalisation"] * 4
    #     )
    #     self.assertEqual(
    #         results["strategy_max_pos_qty"].values.tolist(), [100, 100, 200, 200]
    #     )
    #     self.assertEqual(
    #         results["strategy_max_pos_qty_buffer"].values.tolist(), [1.25, 1.25] * 2
    #     )
    #     self.assertEqual(
    #         results["strategy_position_lifespan"].values.tolist(), ["gfw"] * 4
    #     )
    #     self.assertEqual(results["exit_name"].values.tolist(), ["aggressive"] * 4)
    #     self.assertEqual(results["exit_stoploss_limit"].values.tolist(), [70] * 4)
    #     self.assertEqual(results["exit_takeprofit_limit"].values.tolist(), [50] * 4)
    #     self.assertEqual(results["risk_name"].values.tolist(), ["no_risk"] * 4)
    #
    # def test_multi_day_multiple_simulations_on_single_order_book_not_rolling_pnl_lmax_snapshot(
    #     self,
    # ):
    #     # Load Data
    #     start_date_str: str = "2019-12-05"
    #     end_date_str: str = "2019-12-06"
    #
    #     # Setup Config
    #     pipeline_config = {
    #         "strategy": "internalisation",
    #         "lmax_account": 1463064262,
    #         "load_starting_positions": False,
    #         "level": "mark_to_market",
    #         "netting_engine": "fifo",
    #         "matching_method": "side_of_book",
    #         "simulator": "simulator_pool",
    #         "calculate_cumulative_daily_pnl": False,
    #         "shard": "ldprof",
    #         "process_client_portfolio": False,
    #         "store_client_trade_snapshot": False,
    #         "process_lmax_portfolio": True,
    #         "store_lmax_trade_snapshot": True,
    #         "event_stream_parameters": {
    #             "event_stream_type": "event_stream_snapshot",
    #             "sample_rate": "s",
    #             "excl_period": [[16, 30], [18, 30]],
    #         },
    #         "matching_engine_parameters": {
    #             "matching_engine_type": "matching_engine_default"
    #         },
    #     }
    #     simulations_config = {
    #         "sim_1": {
    #             "instruments": [12345],
    #             "strategy_parameters": {
    #                 "strategy_type": "internalisation",
    #                 "max_pos_qty_buffer": 1.25,
    #                 "max_pos_qty": [100, 200, 300],
    #                 "position_lifespan": "gfw",
    #                 "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
    #             },
    #             "exit_parameters": {
    #                 "exit_type": "aggressive",
    #                 "takeprofit_limit": 50,
    #                 "stoploss_limit": 70,
    #             },
    #             "risk_parameters": {"risk_type": "no_risk"},
    #             "constructor": "zip",
    #         },
    #     }
    #     output_config = {
    #         "resample_rule": None,
    #         "to_csv": True,
    #         "save": False,
    #         "by": None,
    #         "csv_per_simulation": False,
    #         "filesystem": "local",
    #         "directory": "/home/jovyan/work/outputs",
    #         "event_features": ["symbol", "order_book_id", "trading_session",],
    #         "filename": None,
    #     }
    #     # create trades
    #     dates = pd.date_range(start_date_str, end_date_str)
    #     time = dt.time(10, 0, 0)
    #     date_timestamps = [
    #         int(dt.datetime.timestamp(dt.datetime.combine(x, time)) * 1000)
    #         for x in dates
    #     ]
    #     trades = pd.DataFrame()
    #     for date, first_tob_ts in zip(dates, date_timestamps):
    #         date = date.date()
    #         trades_times = [
    #             first_tob_ts + 500,
    #             first_tob_ts + 1500,
    #             first_tob_ts + 1600,
    #         ]
    #         trades_for_ob = {
    #             "datasource": [4] * 3,
    #             "shard": ["ldprof"] * 3,
    #             "timestamp_micros": [x * 1000 for x in trades_times],
    #             "time_stamp": trades_times,
    #             "trade_date": [date] * 3,
    #             "trading_session": [date] * 3,
    #             "execution_id": range(1, 4, 1),
    #             "mtf_execution_id": ["a", "b", "c"],
    #             "order_id": range(1, 4, 1),
    #             "immediate_order": [1] * 3,
    #             "order_type": ["M"] * 3,
    #             "time_in_force": ["I"] * 3,
    #             "order_book_id": [12345] * 3,
    #             "instrument_id": [12345] * 3,
    #             "symbol": ["A/USD"] * 3,
    #             "tob_snapshot_bid_price": [
    #                 1.10004,
    #                 1.10001 + (0.00030 * 1),
    #                 1.10001 + (0.00030 * 1),
    #             ],
    #             "tob_snapshot_ask_price": [
    #                 1.10004,
    #                 1.10001 + (0.00030 * 1),
    #                 1.10001 + (0.00030 * 1),
    #             ],
    #             "bid_adjustment": [0] * 3,
    #             "ask_adjustment": [0] * 3,
    #             "price": [1.10004, 1.10001 + (0.00030 * 1), 1.10001 + (0.00030 * 1),],
    #             "notional_value": [
    #                 x * 10000
    #                 for x in [
    #                     1.10004,
    #                     1.10001 + (0.00030 * 1),
    #                     1.10001 + (0.00030 * 1),
    #                 ]
    #             ],
    #             "notional_value_usd": [
    #                 x * 10000 * 1
    #                 for x in [
    #                     1.10004,
    #                     1.10001 + (0.00030 * 1),
    #                     1.10001 + (0.00030 * 1),
    #                 ]
    #             ],
    #             "unit_quantity": [x * 10000 for x in [1, -1, -1]],
    #             "rate_to_usd": [1] * 3,
    #             "contract_qty": [1, -1, -1],
    #             "order_qty": [1, -1, -1],
    #             "unit_price": [10000] * 3,
    #             "currency": ["USD"] * 3,
    #             "broker_id": [10] * 3,
    #             "account_id": [1, 1, 100],
    #             "price_increment": [1e-05] * 3,
    #             "event_type": ["trade"] * 3,
    #         }
    #         trades = pd.concat([trades, pd.DataFrame(trades_for_ob)])
    #
    #     # noinspection PyTypeChecker
    #     config = BackTestingConfig(
    #         auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
    #     )
    #     config.optionally_override_running_config_parameters(
    #         start_date=start_date_str, end_date=end_date_str
    #     )
    #     config.optionally_set_target_accounts(
    #         target_accounts=pd.DataFrame(
    #             {
    #                 "account_id": [
    #                     1,
    #                     2,
    #                     3,
    #                     4,
    #                     5,
    #                     6,
    #                     7,
    #                     8,
    #                     9,
    #                     10,
    #                     11,
    #                     12,
    #                     13,
    #                     14,
    #                     15,
    #                     16,
    #                     17,
    #                     18,
    #                     19,
    #                     20,
    #                 ]
    #             }
    #         )
    #     )
    #     config.build_simulations_config(simulations_config, [])
    #     config.validate()
    #
    #     results: pd.DataFrame = self.run_simulator(config, trades)
    #     self.assertEqual(
    #         results["strategy_name"].values.tolist(), ["internalisation"] * 12
    #     )
    #
    #     self.assertEqual(results["venue"].values.tolist(), [1] * 12)
    #     self.assertEqual(results["symbol"].values.tolist(), ["A/USD"] * 12)
    #     self.assertEqual(results["currency"].values.tolist(), ["USD"] * 12)
    #     self.assertEqual(results["order_book_id"].values.tolist(), [12345] * 12)
    #     self.assertEqual(
    #         results["price"].values.tolist(), [1.10001, 1.10031, 1.10001, 1.10031] * 3
    #     )
    #     self.assertEqual(results["net_qty"].values.tolist(), [-1.0, 0.0] * 6)
    #     self.assertEqual(results["inventory"].values.tolist(), [-1.0, 0.0] * 6)
    #     self.assertEqual(results["trade_qty"].values.tolist(), [-1.0, 1.0] * 6)
    #     self.assertEqual(results["is_long"].values.tolist(), [0, 1] * 6)
    #     self.assertEqual(results["upnl"].values.tolist(), [0.0] * 12)
    #     self.assertEqual(results["rpnl_cum"].values.tolist(), [0.0, -2.7] * 6)
    #     self.assertEqual(
    #         results["notional_traded_cum"].values.tolist(), [11000.4, 22003.5] * 6
    #     )
    #     self.assertEqual(results["account_id"].values.tolist(), [1463064262] * 12)
    #     self.assertEqual(
    #         results["action"].values.tolist(),
    #         ["client_long_open_short", "client_short_open_long"] * 6,
    #     )
    #     self.assertEqual(results["type"].values.tolist(), ["internal"] * 12)
    #     self.assertEqual(results["portfolio"].values.tolist(), ["lmax"] * 12)
    #     self.assertEqual(
    #         results["trading_session"].values.tolist(),
    #         [
    #             int(
    #                 dt.datetime.strptime("2019-12-05", "%Y-%m-%d").timestamp()
    #                 * 1000000000
    #             )
    #         ]
    #         * 6
    #         + [
    #             int(
    #                 dt.datetime.strptime("2019-12-06", "%Y-%m-%d").timestamp()
    #                 * 1000000000
    #             )
    #         ]
    #         * 6,
    #     )
    #     self.assertEqual(results["rpnl"].values.tolist(), [0.0, -2.7] * 6)
    #     self.assertEqual(
    #         results["notional_traded"].values.tolist(), [11000.4, 11003.1] * 6
    #     )
    #     self.assertEqual(results["simulation"].values.tolist(), ["sim_1"] * 12)
    #     self.assertEqual(
    #         results["strategy_max_pos_qty"].values.tolist(),
    #         [100.0, 100.0, 200.0, 200.0, 300.0, 300.0] * 2,
    #     )
    #     self.assertEqual(
    #         results["strategy_max_pos_qty_buffer"].values.tolist(), [1.25] * 12
    #     )
    #     self.assertEqual(
    #         results["strategy_position_lifespan"].values.tolist(), ["gfw"] * 12
    #     )
    #     self.assertEqual(results["exit_name"].values.tolist(), ["aggressive"] * 12)
    #     self.assertEqual(results["exit_stoploss_limit"].values.tolist(), [70] * 12)
    #     self.assertEqual(results["exit_takeprofit_limit"].values.tolist(), [50] * 12)
    #     self.assertEqual(results["risk_name"].values.tolist(), ["no_risk"] * 12)
    #
    # def test_multi_day_multiple_simulations_on_single_order_book_rolling_pnl_lmax_snapshot(
    #     self,
    # ):
    #     # Load Data
    #     start_date_str: str = "2019-12-05"
    #     end_date_str: str = "2019-12-06"
    #
    #     # Setup Config
    #     pipeline_config = {
    #         "strategy": "internalisation",
    #         "lmax_account": 1463064262,
    #         "load_starting_positions": False,
    #         "level": "mark_to_market",
    #         "netting_engine": "fifo",
    #         "matching_method": "side_of_book",
    #         "simulator": "simulator_pool",
    #         "calculate_cumulative_daily_pnl": True,
    #         "shard": "ldprof",
    #         "process_client_portfolio": False,
    #         "store_client_trade_snapshot": False,
    #         "process_lmax_portfolio": True,
    #         "store_lmax_trade_snapshot": True,
    #         "event_stream_parameters": {
    #             "event_stream_type": "event_stream_snapshot",
    #             "sample_rate": "s",
    #             "excl_period": [[21, 30], [23, 30]],
    #         },
    #         "matching_engine_parameters": {
    #             "matching_engine_type": "matching_engine_default"
    #         },
    #         "simulation_parameters": {
    #             "sim_1": {
    #                 "instruments": [12345],
    #                 "strategy_parameters": {
    #                     "strategy_type": "internalisation",
    #                     "max_pos_qty_buffer": 1.25,
    #                     "max_pos_qty": [100, 200, 300],
    #                     "position_lifespan": "gfw",
    #                     "position_lifespan_exit_parameters": {
    #                         "exit_type": "exit_default"
    #                     },
    #                 },
    #                 "exit_parameters": {
    #                     "exit_type": "aggressive",
    #                     "takeprofit_limit": 50,
    #                     "stoploss_limit": 70,
    #                 },
    #                 "risk_parameters": {"risk_type": "no_risk"},
    #                 "constructor": "zip",
    #             },
    #         },
    #         "output_parameters": {
    #             "resample_rule": None,
    #             "to_csv": True,
    #             "save": False,
    #             "by": None,
    #             "csv_per_simulation": False,
    #             "filesystem": "local",
    #             "directory": "/home/jovyan/work/outputs",
    #             "event_features": ["symbol", "order_book_id", "trading_session",],
    #             "filename": None,
    #         },
    #         "server_configuration": {"cores": 1},
    #     }
    #     simulations_config = {
    #         "sim_1": {
    #             "instruments": [12345],
    #             "strategy_parameters": {
    #                 "strategy_type": "internalisation",
    #                 "max_pos_qty_buffer": 1.25,
    #                 "max_pos_qty": [100, 200, 300],
    #                 "position_lifespan": "gfw",
    #                 "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
    #             },
    #             "exit_parameters": {
    #                 "exit_type": "aggressive",
    #                 "takeprofit_limit": 50,
    #                 "stoploss_limit": 70,
    #             },
    #             "risk_parameters": {"risk_type": "no_risk"},
    #             "constructor": "zip",
    #         }
    #     }
    #     output_config = {
    #         "resample_rule": None,
    #         "to_csv": True,
    #         "save": False,
    #         "by": None,
    #         "csv_per_simulation": False,
    #         "filesystem": "local",
    #         "directory": "/home/jovyan/work/outputs",
    #         "event_features": ["symbol", "order_book_id", "trading_session",],
    #         "filename": None,
    #     }
    #
    #     # create trades
    #     dates = pd.date_range(start_date_str, end_date_str)
    #     time = dt.time(10, 0, 0)
    #     date_timestamps = [
    #         int(dt.datetime.timestamp(dt.datetime.combine(x, time)) * 1000)
    #         for x in dates
    #     ]
    #     trades = pd.DataFrame()
    #     for date, first_tob_ts in zip(dates, date_timestamps):
    #         date = date.date()
    #         trades_times = [
    #             first_tob_ts + 500,
    #             first_tob_ts + 1500,
    #             first_tob_ts + 1600,
    #         ]
    #         trades_for_ob = {
    #             "datasource": [4] * 3,
    #             "shard": ["ldprof"] * 3,
    #             "timestamp_micros": [x * 1000 for x in trades_times],
    #             "time_stamp": trades_times,
    #             "trade_date": [date] * 3,
    #             "trading_session": [date] * 3,
    #             "execution_id": range(1, 4, 1),
    #             "mtf_execution_id": ["a", "b", "c"],
    #             "order_id": range(1, 4, 1),
    #             "immediate_order": [1] * 3,
    #             "order_type": ["M"] * 3,
    #             "time_in_force": ["I"] * 3,
    #             "order_book_id": [12345] * 3,
    #             "instrument_id": [12345] * 3,
    #             "symbol": ["A/USD"] * 3,
    #             "tob_snapshot_bid_price": [
    #                 1.10004,
    #                 1.10001 + (0.00030 * 1),
    #                 1.10001 + (0.00030 * 1),
    #             ],
    #             "tob_snapshot_ask_price": [
    #                 1.10004,
    #                 1.10001 + (0.00030 * 1),
    #                 1.10001 + (0.00030 * 1),
    #             ],
    #             "bid_adjustment": [0] * 3,
    #             "ask_adjustment": [0] * 3,
    #             "price": [1.10004, 1.10001 + (0.00030 * 1), 1.10001 + (0.00030 * 1),],
    #             "notional_value": [
    #                 x * 10000
    #                 for x in [
    #                     1.10004,
    #                     1.10001 + (0.00030 * 1),
    #                     1.10001 + (0.00030 * 1),
    #                 ]
    #             ],
    #             "notional_value_usd": [
    #                 x * 10000 * 1
    #                 for x in [
    #                     1.10004,
    #                     1.10001 + (0.00030 * 1),
    #                     1.10001 + (0.00030 * 1),
    #                 ]
    #             ],
    #             "unit_quantity": [x * 10000 for x in [1, -1, -1]],
    #             "rate_to_usd": [1] * 3,
    #             "contract_qty": [1, -1, -1],
    #             "order_qty": [1, -1, -1],
    #             "unit_price": [10000] * 3,
    #             "currency": ["USD"] * 3,
    #             "broker_id": [10] * 3,
    #             "account_id": [1, 1, 100],
    #             "price_increment": [1e-05] * 3,
    #             "event_type": ["trade"] * 3,
    #         }
    #         trades = pd.concat([trades, pd.DataFrame(trades_for_ob)])
    #
    #     # noinspection PyTypeChecker
    #     config = BackTestingConfig(
    #         auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
    #     )
    #     config.optionally_override_running_config_parameters(
    #         start_date=start_date_str, end_date=end_date_str
    #     )
    #     config.optionally_set_target_accounts(
    #         target_accounts=pd.DataFrame(
    #             {
    #                 "account_id": [
    #                     1,
    #                     2,
    #                     3,
    #                     4,
    #                     5,
    #                     6,
    #                     7,
    #                     8,
    #                     9,
    #                     10,
    #                     11,
    #                     12,
    #                     13,
    #                     14,
    #                     15,
    #                     16,
    #                     17,
    #                     18,
    #                     19,
    #                     20,
    #                 ]
    #             }
    #         )
    #     )
    #     config.build_simulations_config(simulations_config, [])
    #     config.validate()
    #
    #     results: pd.DataFrame = self.run_simulator(config, trades)
    #
    #     self.assertEqual(results["venue"].values.tolist(), [1] * 12)
    #     self.assertEqual(results["symbol"].values.tolist(), ["A/USD"] * 12)
    #     self.assertEqual(results["currency"].values.tolist(), ["USD"] * 12)
    #     self.assertEqual(results["order_book_id"].values.tolist(), [12345] * 12)
    #     self.assertEqual(
    #         results["price"].values.tolist(), [1.10001, 1.10031, 1.10001, 1.10031] * 3
    #     )
    #     self.assertEqual(results["net_qty"].values.tolist(), [-1.0, 0.0] * 6)
    #     self.assertEqual(results["inventory"].values.tolist(), [-1.0, 0.0] * 6)
    #     self.assertEqual(results["trade_qty"].values.tolist(), [-1.0, 1.0] * 6)
    #     self.assertEqual(results["is_long"].values.tolist(), [0, 1] * 6)
    #     self.assertEqual(results["upnl"].values.tolist(), [0.0] * 12)
    #     self.assertEqual(
    #         results["rpnl_cum"].values.tolist(),
    #         [0.0, -2.7, -2.7, -5.4, 0.0, -2.7, -2.7, -5.4, 0.0, -2.7, -2.7, -5.4],
    #     )
    #     self.assertEqual(
    #         results["notional_traded_cum"].values.tolist(),
    #         [
    #             11000.4,
    #             22003.5,
    #             33003.9,
    #             44007.0,
    #             11000.4,
    #             22003.5,
    #             33003.9,
    #             44007.0,
    #             11000.4,
    #             22003.5,
    #             33003.9,
    #             44007.0,
    #         ],
    #     )
    #     self.assertEqual(results["account_id"].values.tolist(), [1463064262] * 12)
    #     self.assertEqual(
    #         results["action"].values.tolist(),
    #         ["client_long_open_short", "client_short_open_long"] * 6,
    #     )
    #     self.assertEqual(results["type"].values.tolist(), ["internal"] * 12)
    #     self.assertEqual(results["portfolio"].values.tolist(), ["lmax"] * 12)
    #     self.assertEqual(
    #         results["trading_session"].values.tolist(),
    #         [
    #             int(
    #                 dt.datetime.strptime("2019-12-05", "%Y-%m-%d").timestamp()
    #                 * 1000000000
    #             ),
    #             int(
    #                 dt.datetime.strptime("2019-12-05", "%Y-%m-%d").timestamp()
    #                 * 1000000000
    #             ),
    #             int(
    #                 dt.datetime.strptime("2019-12-06", "%Y-%m-%d").timestamp()
    #                 * 1000000000
    #             ),
    #             int(
    #                 dt.datetime.strptime("2019-12-06", "%Y-%m-%d").timestamp()
    #                 * 1000000000
    #             ),
    #         ]
    #         * 3,
    #     )
    #     self.assertEqual(
    #         results["rpnl"].values.tolist(),
    #         [0.0, -2.7, -2.7, -2.7, 0.0, -2.7, -2.7, -2.7, 0.0, -2.7, -2.7, -2.7],
    #     )
    #     self.assertEqual(
    #         results["notional_traded"].values.tolist(),
    #         [
    #             11000.4,
    #             11003.1,
    #             33003.9,
    #             11003.099999999999,
    #             11000.4,
    #             11003.1,
    #             33003.9,
    #             11003.099999999999,
    #             11000.4,
    #             11003.1,
    #             33003.9,
    #             11003.099999999999,
    #         ],
    #     )
    #     self.assertEqual(results["simulation"].values.tolist(), ["sim_1"] * 12)
    #     self.assertEqual(
    #         results["strategy_name"].values.tolist(), ["internalisation"] * 12
    #     )
    #     self.assertEqual(
    #         results["strategy_max_pos_qty"].values.tolist(),
    #         [
    #             100.0,
    #             100.0,
    #             100.0,
    #             100.0,
    #             200.0,
    #             200.0,
    #             200.0,
    #             200.0,
    #             300.0,
    #             300.0,
    #             300.0,
    #             300.0,
    #         ],
    #     )
    #     self.assertEqual(
    #         results["strategy_max_pos_qty_buffer"].values.tolist(), [1.25] * 12
    #     )
    #     self.assertEqual(
    #         results["strategy_position_lifespan"].values.tolist(), ["gfw"] * 12
    #     )
    #     self.assertEqual(results["exit_name"].values.tolist(), ["aggressive"] * 12)
    #     self.assertEqual(results["exit_stoploss_limit"].values.tolist(), [70] * 12)
    #     self.assertEqual(results["exit_takeprofit_limit"].values.tolist(), [50] * 12)
    #     self.assertEqual(results["risk_name"].values.tolist(), ["no_risk"] * 12)


class TestSimulatorPoolForBbookingProfilerStrategy(TestSimulatorPool):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    # tests for start_simulator

    def test_single_day_simulations_no_migrations_and_balanced_book(self):
        # Load Data
        date_str: str = "2019-12-05"

        # Setup Config
        pipeline_config = {
            "lmax_account": 12345678,
            "load_starting_positions": False,
            "load_client_starting_positions": False,
            "level": "trades_only",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": True,
            "shard": "ldprof",
            "process_client_portfolio": True,
            "store_client_trade_snapshot": False,
            "process_lmax_portfolio": True,
            "store_lmax_trade_snapshot": True,
            "event_stream_parameters": {
                "event_stream_type": "event_stream_snapshot",
                "sample_rate": "s",
                "excl_period": [[21, 30], [23, 30]],
            },
            "matching_engine_parameters": {
                "matching_engine_type": "matching_engine_default"
            },
        }
        simulations_config = {
            "sim_1": {
                "instruments": [12345],
                "strategy_parameters": {
                    "strategy_type": "bbooking_profiler",
                    "model_type": "profiling_ranks",
                    "train_freq": 20,
                    "train_period": 30,
                    "score_booking_risk": {0: 1, 1: 1, 2: 0.80},
                    "rank_evaluation_string": {
                        "rank1": "profitable_rpnl_ratio > losing_rpnl_ratio",
                        "rank2": "profitable_rpnl_trades_cnt > losing_rpnl_trades_cnt",
                    },
                },
                "exit_parameters": {"exit_type": "exit_default",},
                "constructor": "zip",
            },
        }
        output_config = {
            "resample_rule": None,
            "to_csv": True,
            "save": False,
            "by": None,
            "filesystem": "local",
            "freq": None,
            "directory": "/home/jovyan/work/outputs",
            "event_features": ["symbol", "order_book_id", "account_id"],
            "filename": None,
        }

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1, 2,]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        # create trades
        dates = pd.date_range(date_str, date_str)
        time = dt.time(10, 0, 0)
        date_timestamps = [
            int(dt.datetime.timestamp(dt.datetime.combine(x, time)) * 1000)
            for x in dates
        ]
        trades = pd.DataFrame()
        for date, first_tob_ts in zip(dates, date_timestamps):
            date = date.date()
            trades_times = [first_tob_ts + 500, first_tob_ts + 1500]
            trades_for_ob = {
                "datasource": [4] * 2,
                "shard": ["ldprof"] * 2,
                "timestamp_micros": [x * 1000 for x in trades_times],
                "time_stamp": trades_times,
                "trade_date": [date] * 2,
                "trading_session": [date] * 2,
                "execution_id": range(1, 3, 1),
                "order_id": range(1, 3, 1),
                "immediate_order": [1] * 2,
                "order_type": ["M"] * 2,
                "time_in_force": ["I"] * 2,
                "order_book_id": [12345] * 2,
                "instrument_id": [12345] * 2,
                "symbol": ["A/USD"] * 2,
                "price": [1.10004, 1.10001 + (0.00030 * 1)],
                "notional_value": [
                    x * 10000 for x in [1.10004, 1.10001 + (0.00030 * 1)]
                ],
                "notional_value_usd": [
                    x * 10000 * 1 for x in [1.10004, 1.10001 + (0.00030 * 1)]
                ],
                "unit_quantity": [x * 10000 for x in [1, -1]],
                "rate_to_usd": [1] * 2,
                "contract_qty": [1, -1],
                "order_qty": [1, -1],
                "unit_price": [10000] * 2,
                "currency": ["USD"] * 2,
                "broker_id": [10] * 2,
                "account_id": [1, 2],
                "price_increment": [1e-05] * 2,
                "event_type": ["trade"] * 2,
            }
            trades = pd.concat([trades, pd.DataFrame(trades_for_ob)])

        profiles = pd.DataFrame(
            {
                "profitable_rpnl_ratio": [0.3, 0.3],
                "losing_rpnl_ratio": [0.2, 0.2],
                "profitable_rpnl_trades_cnt": [1, 2],
                "losing_rpnl_trades_cnt": [2, 1],
            },
            index=[1, 2],
        )
        results: pd.DataFrame = self.run_simulator(config, trades, profiles=profiles)

        self.assertEqual(results["venue"].values.tolist(), [1] * 2)
        self.assertEqual(results["symbol"].values.tolist(), ["A/USD"] * 2)
        self.assertEqual(results["currency"].values.tolist(), ["USD"] * 2)
        self.assertEqual(results["order_book_id"].values.tolist(), [12345] * 2)
        self.assertEqual(results["trade_price"].values.tolist(), [1.10004, 1.10031])
        self.assertEqual(results["net_qty"].values.tolist(), [-1.0, 0.8])
        self.assertEqual(results["inventory_contracts"].values.tolist(), [-1.0, -0.2])
        self.assertEqual(
            results["inventory_dollars"].values.tolist(), [-11000.4, -2197.92]
        )
        self.assertEqual(results["trade_qty"].values.tolist(), [-1.0, 0.8])
        self.assertEqual(results["is_long"].values.tolist(), [0, 1])
        self.assertEqual(results["upnl"].values.tolist(), [0.0] * 2)
        self.assertEqual(results["rpnl_cum"].values.tolist(), [0.0, 0.0])
        self.assertEqual(
            results["notional_traded_cum"].values.tolist(), [11000.4, 8802.48]
        )
        self.assertEqual(results["account_id"].values.tolist(), [1, 2])
        self.assertEqual(results["counterparty_account_id"].values.tolist(), [1, 2])
        self.assertEqual(
            results["action"].values.tolist(), ["client_trade"] * 2,
                                               )
        self.assertEqual(results["type"].values.tolist(), ["internal"] * 2)
        self.assertEqual(results["portfolio"].values.tolist(), ["lmax"] * 2)
        self.assertEqual(
            results["trading_session"].values.tolist(),
            [
                int(
                    dt.datetime.strptime("2019-12-05", "%Y-%m-%d").timestamp()
                    * 1000000000
                )
            ]
            * 2,
            )
        self.assertEqual(results["rpnl"].values.tolist(), [0.0, 0.0])
        self.assertEqual(results["notional_traded"].values.tolist(), [11000.4, 8802.48])
        self.assertEqual(results["simulation"].values.tolist(), ["sim_1", "sim_1"])
        self.assertEqual(
            results["strategy_name"].values.tolist(), ["bbooking_profiler"] * 2
        )
        self.assertEqual(
            results["strategy_model_type"].values.tolist(), ["profiling_ranks"] * 2
        )
        self.assertEqual(results["strategy_train_freq"].values.tolist(), [20] * 2)
        self.assertEqual(results["strategy_train_period"].values.tolist(), [30] * 2)

    def test_single_day_simulations_with_migrations_and_balanced_book(self):
        # Load Data
        date_str: str = "2019-12-05"

        # Setup Config
        pipeline_config = {
            "lmax_account": 12345678,
            "load_starting_positions": False,
            "load_client_starting_positions": True,
            "level": "trades_only",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": True,
            "shard": "ldprof",
            "process_client_portfolio": True,
            "store_client_trade_snapshot": False,
            "process_lmax_portfolio": True,
            "store_lmax_trade_snapshot": True,
            "event_stream_parameters": {
                "event_stream_type": "event_stream_snapshot",
                "sample_rate": "s",
                "excl_period": [[21, 30], [23, 30]],
            },
            "matching_engine_parameters": {
                "matching_engine_type": "matching_engine_default"
            },
        }
        simulations_config = {
            "sim_1": {
                "instruments": [12345],
                "strategy_parameters": {
                    "strategy_type": "bbooking_profiler",
                    "model_type": "profiling_ranks",
                    "train_freq": 20,
                    "train_period": 30,
                    "score_booking_risk": {0: 1, 1: 1, 2: 0.80},
                    "rank_evaluation_string": {
                        "rank1": "profitable_rpnl_ratio > losing_rpnl_ratio",
                        "rank2": "profitable_rpnl_trades_cnt > losing_rpnl_trades_cnt",
                    },
                },
                "exit_parameters": {"exit_type": "exit_default",},
                "constructor": "zip",
            },
        }
        output_config = {
            "resample_rule": None,
            "to_csv": True,
            "save": False,
            "by": None,
            "filesystem": "local",
            "freq": None,
            "directory": "/home/jovyan/work/outputs",
            "event_features": ["symbol", "order_book_id", "account_id"],
            "filename": None,
        }

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        # create trades
        dates = pd.date_range(date_str, date_str)
        time = dt.time(10, 0, 0)
        date_timestamps = [
            int(dt.datetime.timestamp(dt.datetime.combine(x, time)) * 1000)
            for x in dates
        ]
        trades = pd.DataFrame()
        for date, first_tob_ts in zip(dates, date_timestamps):
            date = date.date()
            trades_for_ob = {
                "datasource": [4],
                "shard": ["ldprof"],
                "timestamp_micros": [(first_tob_ts + 500) * 1000],
                "time_stamp": [first_tob_ts + 500],
                "trade_date": [date],
                "trading_session": [date],
                "execution_id": [1],
                "order_id": [1],
                "immediate_order": [1],
                "order_type": ["M"],
                "time_in_force": ["I"],
                "order_book_id": [12345],
                "instrument_id": [12345],
                "symbol": ["A/USD"],
                "price": [1.10004],
                "notional_value": [1.10004],
                "notional_value_usd": [1.10004],
                "unit_quantity": [10000],
                "rate_to_usd": [1],
                "contract_qty": [1],
                "order_qty": [1],
                "unit_price": [10000],
                "contract_unit_of_measure": ["A"],
                "currency": ["USD"],
                "broker_id": [10],
                "account_id": [1],
                "price_increment": [1e-05],
                "event_type": ["trade"],
            }
            trades = pd.concat([trades, pd.DataFrame(trades_for_ob)])

        profiles = pd.DataFrame(
            {
                "profitable_rpnl_ratio": [0.3, 0.3],
                "losing_rpnl_ratio": [0.2, 0.2],
                "profitable_rpnl_trades_cnt": [1, 2],
                "losing_rpnl_trades_cnt": [2, 1],
            },
            index=[1, 2],
        )

        starting_positions = pd.DataFrame(
            {
                "shard": ["ldprof"],
                "symbol": ["A/USD"],
                "datasource": [4],
                "next_trading_day": [dates[0].date()],
                "account_id": [1],
                "position": [-1],
                "instrument_id": [12345],
                "unit_price": [10000],
                "price_increment": [1e-05],
                "currency": ["USD"],
                "contract_unit_of_measure": ["A"],
                "open_cost": [1.10003 * 1 * 10000],
            }
        )

        results: pd.DataFrame = self.run_simulator(
            config, trades, profiles=profiles, starting_positions=starting_positions
        )
        self.assertEqual(results["venue"].values.tolist(), [1] * 2)
        self.assertEqual(results["symbol"].values.tolist(), ["A/USD", "A/USD"])
        self.assertEqual(results["currency"].values.tolist(), ["USD", "USD"])
        self.assertEqual(
            results["contract_unit_of_measure"].values.tolist(), ["A", "A"]
        )
        self.assertEqual(results["order_book_id"].values.tolist(), [12345] * 2)
        self.assertEqual(results["trade_price"].values.tolist(), [1.10064, 1.10004])
        self.assertEqual(results["net_qty"].values.tolist(), [1.0, 0.0])
        self.assertEqual(results["inventory_contracts_cum"].values.tolist(), [1.0, 0.0])
        self.assertEqual(results["inventory_contracts"].values.tolist(), [1.0, -1.0])
        self.assertEqual(
            results["inventory_dollars_cum"].values.tolist(), [11006.4, 6.0]
        )
        self.assertEqual(
            results["inventory_dollars"].values.tolist(), [11006.4, -11000.4]
        )
        self.assertEqual(results["trade_qty"].values.tolist(), [1.0, -1.0])
        self.assertEqual(results["is_long"].values.tolist(), [1, 0])
        self.assertEqual(results["upnl_reversal"].values.tolist(), [-0.0, 0.3])
        self.assertEqual(results["upnl"].values.tolist(), [-0.3] * 2)
        self.assertEqual(results["rpnl_cum"].values.tolist(), [0.0, -6.0])
        self.assertEqual(results["pnl"].values.tolist(), [-0.3, -6.0])
        self.assertEqual(
            results["notional_traded_cum"].values.tolist(), [11006.4, 22006.8]
        )
        self.assertEqual(results["account_id"].values.tolist(), [1] * 2)
        self.assertEqual(results["counterparty_account_id"].values.tolist(), [1] * 2)
        self.assertEqual(
            results["action"].values.tolist(), ["account_migration", "client_trade"],
        )
        self.assertEqual(results["type"].values.tolist(), ["internal"] * 2)
        self.assertEqual(results["portfolio"].values.tolist(), ["lmax"] * 2)
        self.assertEqual(
            results["trading_session"].values.tolist(),
            [
                int(
                    dt.datetime.strptime("2019-12-05", "%Y-%m-%d").timestamp()
                    * 1000000000
                )
            ]
            * 2,
            )
        self.assertEqual(results["rpnl"].values.tolist(), [0.0, -6.0])
        self.assertEqual(results["notional_traded"].values.tolist(), [11006.4, 11000.4])
        self.assertEqual(results["simulation"].values.tolist(), ["sim_1", "sim_1"])
        self.assertEqual(
            results["strategy_name"].values.tolist(), ["bbooking_profiler"] * 2
        )
        self.assertEqual(
            results["strategy_model_type"].values.tolist(), ["profiling_ranks"] * 2
        )
        self.assertEqual(results["strategy_train_freq"].values.tolist(), [20] * 2)
        self.assertEqual(results["strategy_train_period"].values.tolist(), [30] * 2)


if __name__ == "__main__":
    unittest.main()
