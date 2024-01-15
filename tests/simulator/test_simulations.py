import datetime as dt
import unittest
from typing import List

import pandas as pd

from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.event_stream import EventStream
from risk_backtesting.loaders.load_snapshot import SnapshotLoader, parse_snapshot
from risk_backtesting.loaders.load_trades import TradesLoader
from risk_backtesting.matching_engine.matching_engine_default import (
    AbstractMatchingEngine,
)
from risk_backtesting.simulator.simulation_plan import SimulationPlan
from risk_backtesting.simulator.simulations import Simulations


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
        trades = self.trades.copy()

        trades["timestamp"] = pd.to_datetime(
            trades["timestamp_micros"], unit="us", utc=True
        )
        trades.set_index("timestamp", inplace=True)
        trades.sort_index(inplace=True)
        return trades


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


class DummyMatchingEngine(AbstractMatchingEngine):
    def match_order(self, event, order):
        pass


class DummyEventStream(EventStream):
    def __init__(self):
        super().__init__(None)

    def sample(self, tob: pd.DataFrame, trading_session: dt.datetime) -> pd.DataFrame:
        pass


class TestSimulations(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_create_event_filter_strings_for_relative_out_internalisation_scenerio(
            self,
    ):
        lmax_account = 121212

        direction = "out"
        comparison_accounts = [lmax_account]
        comparison_accounts_type = "internalisation"

        snapshot = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "internalisation_account_id": [
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    -1,
                ],
            }
        )
        # noinspection PyTypeChecker
        event_filter_string: List[str] = Simulations.create_event_filter_strings(
            lmax_account=lmax_account,
            snapshot=snapshot,
            direction=direction,
            comparison_accounts=comparison_accounts,
            comparison_accounts_type=comparison_accounts_type,
        )
        self.assertEqual(4, len(event_filter_string))
        self.assertEqual(
            [
                "account_id not in [4, 1] # exclude 1",
                "account_id not in [4, 2] # exclude 2",
                "account_id not in [4, 3] # exclude 3",
                "account_id not in [4, -1] # benchmark",
            ],
            event_filter_string,
        )

    def test_create_event_filter_strings_for_relative_out_client_scenerio(self):
        lmax_account = 121212

        direction = "out"
        comparison_accounts = [1, 2]
        comparison_accounts_type = "client"

        snapshot = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "internalisation_account_id": [
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    -1,
                ],
            }
        )
        # noinspection PyTypeChecker
        event_filter_string: List[str] = Simulations.create_event_filter_strings(
            lmax_account=lmax_account,
            snapshot=snapshot,
            direction=direction,
            comparison_accounts=comparison_accounts,
            comparison_accounts_type=comparison_accounts_type,
        )

        self.assertEqual(3, len(event_filter_string))
        self.assertEqual(
            [
                "account_id not in [1] # exclude 1",
                "account_id not in [2] # exclude 2",
                "account_id not in [-1] # benchmark",
            ],
            event_filter_string,
        )

    def test_create_event_filter_strings_for_relative_in_scenerio(self):
        lmax_account = 121212
        direction = "in"
        comparison_accounts = [131313]
        comparison_accounts_type = "internalisation"
        snapshot = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4, 5, 6],
                "internalisation_account_id": [
                    lmax_account,
                    lmax_account,
                    comparison_accounts[0],
                    comparison_accounts[0],
                    comparison_accounts[0],
                    -1,
                ],
            }
        )
        # noinspection PyTypeChecker
        event_filter_string: List[str] = Simulations.create_event_filter_strings(
            lmax_account=lmax_account,
            snapshot=snapshot,
            direction=direction,
            comparison_accounts=comparison_accounts,
            comparison_accounts_type=comparison_accounts_type,
        )
        self.assertEqual(4, len(event_filter_string))
        self.assertEqual(
            [
                "account_id not in [4, 5] # include 3",
                "account_id not in [3, 5] # include 4",
                "account_id not in [3, 4] # include 5",
                "account_id not in [3, 4, 5] # benchmark",
            ],
            event_filter_string,
        )

    # build simulation plans
    def test_create_simulation_plan_loading_nothing_from_snapshot(self):
        date_str = "2020-12-07"

        # pipeline Config
        pipeline_config = {
            "lmax_account": 1463064262,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123],
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "max_pos_qty": 100,
                    "allow_partial_fills": True,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session"],
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
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=None),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )
        self.assertEqual(1, len(plans))

        plan = plans[0]
        self.assertEqual(plan.name, "sim_1")
        self.assertEqual(plan.instruments, [123])
        self.assertEqual(plan.target_accounts_list, [1, 2, 3, 4, 5, 6, 7, 8, 9])
        self.assertEqual("d87e82a68b03570702aec2ef672c0e22", plan.hash)

    def test_create_simulation_plan_loading_position_limit_from_snapshot(self,):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123],
                "load_position_limits_from_snapshot": True,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "position_lifespan": "gfw",
                    "allow_partial_fills": True,
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session"],
            "filename": None,
        }

        # snapshot that will be used to load the position limit
        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int(
                                     (
                                             dt.datetime.strptime(date_str, "%Y-%m-%d")
                                             - dt.timedelta(days=1)
                                     ).timestamp()
                                     * 1000000
                                 )
                             ]
                             * 6,
                "account_id": [1, 2, 3, 1, 2, 3],
                "internalisation_position_limit": [100, 100, 100, None, None, None],
                "instrument_id": [123, 123, 123, 456, 456, 456],
                "internalisation_account_id": [
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    -1,
                    -1,
                    -1,
                ],
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=snapshot),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )

        self.assertEqual(1, len(plans))

        for idx, name, instrument, hash in zip(
                range(0, 1, 1), ["sim_1"], [123], ["7035fffc0e2436fb932776310dff282a"],
        ):
            plan = plans[idx]
            self.assertEqual(name, plan.name)
            self.assertEqual([instrument], plan.instruments)
            self.assertEqual(lmax_account, plan.lmax_account)
            self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9], plan.target_accounts_list)
            self.assertEqual(hash, plan.hash)

    def test_create_simulation_plan_loading_position_limit_from_snapshot_for_two_instruments(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123, 456],
                "load_position_limits_from_snapshot": True,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "allow_partial_fills": True,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session"],
            "filename": None,
        }

        # snapshot that will be used to load the position limit
        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int(
                                     (
                                             dt.datetime.strptime(date_str, "%Y-%m-%d")
                                             - dt.timedelta(days=1)
                                     ).timestamp()
                                     * 1000000
                                 )
                             ]
                             * 6,
                "account_id": [1, 2, 3, 1, 2, 3],
                "internalisation_position_limit": [100, 100, 100, 100, 100, 100],
                "instrument_id": [123, 123, 123, 456, 456, 456],
                "internalisation_account_id": [
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    lmax_account,
                ],
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=snapshot),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )

        self.assertEqual(2, len(plans))

        for idx, name, instrument, hash in zip(
                range(0, 2, 1),
                ["sim_1"] * 2,
                [123, 456],
                ["40bce716c3e16ac3d46d73bd16ee18e8", "009e084504ce3c3ff7e6c47b1b7f673f"],
        ):
            plan = plans[idx]
            self.assertEqual(name, plan.name)
            self.assertEqual([instrument], plan.instruments)
            self.assertEqual(lmax_account, plan.lmax_account)
            self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9], plan.target_accounts_list)
            self.assertEqual(hash, plan.hash)

    def test_create_simulation_plan_loading_instruments_from_snapshot_dont_filter_for_internalisation_strategy(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "load_instruments_from_snapshot": True,
                "filter_snapshot_for_strategy": False,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "max_pos_qty": 100,
                    "allow_partial_fills": True,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session"],
            "filename": None,
        }

        # snapshot that will be used to load the instruments
        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int(
                                     (
                                             dt.datetime.strptime(date_str, "%Y-%m-%d")
                                             - dt.timedelta(days=1)
                                     ).timestamp()
                                     * 1000000
                                 )
                             ]
                             * 6,
                "account_id": [1, 2, 3, 1, 2, 3],
                "instrument_id": [123, 123, 123, 456, 456, 456],
                "internalisation_account_id": [
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    -1,
                    -1,
                    -1,
                ],
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=snapshot),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )

        self.assertEqual(2, len(plans))

        for idx, name, instrument, hash in zip(
                range(0, 2, 1),
                ["sim_1", "sim_1"],
                [123, 456],
                ["554a53b60c75193ec2b06a90aa94dd43", "1e883477557267ec5ab5a4c99874c42d"],
        ):
            plan = plans[idx]
            self.assertEqual(name, plan.name)
            self.assertEqual([instrument], plan.instruments)
            self.assertEqual(lmax_account, plan.lmax_account)
            self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9], plan.target_accounts_list)
            self.assertEqual(hash, plan.hash)

    def test_create_simulation_plan_loading_instruments_from_snapshot_and_filter_for_internalisation_strategy(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "load_instruments_from_snapshot": True,
                "filter_snapshot_for_strategy": True,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "max_pos_qty": 100,
                    "allow_partial_fills": True,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session",],
            "filename": None,
        }

        # snapshot that will be used to load the instruments
        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int(
                                     (
                                             dt.datetime.strptime(date_str, "%Y-%m-%d")
                                             - dt.timedelta(days=1)
                                     ).timestamp()
                                     * 1000000
                                 )
                             ]
                             * 6,
                "account_id": [1, 2, 3, 1, 2, 3],
                "instrument_id": [123, 123, 123, 456, 456, 456],
                "internalisation_account_id": [
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    -1,
                    -1,
                    -1,
                ],
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=snapshot),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )
        self.assertEqual(1, len(plans))

        plan = plans[0]
        self.assertEqual("sim_1", plan.name)
        self.assertEqual([123], plan.instruments)
        self.assertEqual(lmax_account, plan.lmax_account)
        self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9], plan.target_accounts_list)
        self.assertEqual("554a53b60c75193ec2b06a90aa94dd43", plan.hash)

    def test_create_simulation_plan_loading_instruments_from_snapshot_dont_filter_for_bbooking_strategy(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "load_instruments_from_snapshot": True,
                "filter_snapshot_for_strategy": False,
                "strategy_parameters": {
                    "strategy_type": "bbooking",
                    "max_pos_qty_buffer": 1.25,
                    "max_pos_qty": 100,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session",],
            "filename": None,
        }

        # snapshot that will be used to load the instruments
        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int(
                                     (
                                             dt.datetime.strptime(date_str, "%Y-%m-%d")
                                             - dt.timedelta(days=1)
                                     ).timestamp()
                                     * 1000000
                                 )
                             ]
                             * 6,
                "account_id": [1, 2, 3, 1, 2, 3],
                "instrument_id": [123, 123, 123, 456, 456, 456],
                "booking_risk": [0, 0, 0, 100, 100, 100],
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=snapshot),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )

        self.assertEqual(2, len(plans))

        for idx, name, instrument, hash in zip(
                range(0, 2, 1),
                ["sim_1", "sim_1"],
                [123, 456],
                ["c891edfd2b2ab34e89051bda36406d5c", "41bf6e2b5892add15db6e365f53bf093"],
        ):
            plan = plans[idx]
            self.assertEqual(name, plan.name)
            self.assertEqual([instrument], plan.instruments)
            self.assertEqual(lmax_account, plan.lmax_account)
            self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9], plan.target_accounts_list)
            self.assertEqual(hash, plan.hash)

    def test_create_simulation_plan_loading_instruments_from_snapshot_and_filter_for_bbooking_strategy(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212

        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "load_instruments_from_snapshot": True,
                "filter_snapshot_for_strategy": True,
                "strategy_parameters": {
                    "strategy_type": "bbooking",
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session",],
            "filename": None,
        }

        # snapshot that will be used to load the instruments
        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int(
                                     (
                                             dt.datetime.strptime(date_str, "%Y-%m-%d")
                                             - dt.timedelta(days=1)
                                     ).timestamp()
                                     * 1000000
                                 )
                             ]
                             * 6,
                "account_id": [1, 2, 3, 1, 2, 3],
                "instrument_id": [123, 123, 123, 456, 456, 456],
                "booking_risk": [0, 0, 0, 100, 100, 100],
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=snapshot),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )

        self.assertEqual(1, len(plans))

        plan = plans[0]
        self.assertEqual("sim_1", plan.name)
        self.assertEqual([456], plan.instruments)
        self.assertEqual(lmax_account, plan.lmax_account)
        self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9], plan.target_accounts_list)
        self.assertEqual("41bf6e2b5892add15db6e365f53bf093", plan.hash)

    def test_create_simulation_plan_loading_target_accounts_and_instruments_from_snapshot_for_internalisation_strategy(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "load_instruments_from_snapshot": True,
                "load_target_accounts_from_snapshot": True,
                "filter_snapshot_for_strategy": True,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "allow_partial_fills": True,
                    "max_pos_qty": 100,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session",],
            "filename": None,
        }

        # snapshot that will be used to load the instruments
        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int(
                                     (
                                             dt.datetime.strptime(date_str, "%Y-%m-%d")
                                             - dt.timedelta(days=1)
                                     ).timestamp()
                                     * 1000000
                                 )
                             ]
                             * 6,
                "datasource": [4] * 6,
                "account_id": [1, 2, 3, 1, 2, 3],
                "liquidity_profile_id": [1, 1, 1, 2, 2, 2],
                "instrument_id": [123, 123, 123, 456, 456, 456],
                "counterparty_source": ["B"] * 6,
                "internalisation_account_id": [
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    -1,
                    -1,
                    -1,
                ],
                "booking_risk": [0] * 6,
                "secondary_booking_risk": [0] * 6,
                "internalisation_risk": [100, 100, 100, 0, 0, 0],
                "observe_spreads": ["N"] * 6,
                "synthetic_source": ["E"] * 6,
                "shard": ["ldprof"] * 6,
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=snapshot),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )

        self.assertEqual(1, len(plans))

        plan = plans[0]
        self.assertEqual("sim_1", plan.name)
        self.assertEqual([123], plan.instruments)
        self.assertEqual(lmax_account, plan.lmax_account)
        self.assertEqual([1, 2, 3], plan.target_accounts_list)
        self.assertEqual("554a53b60c75193ec2b06a90aa94dd43", plan.hash)

    def test_create_simulation_plan_loading_target_accounts_from_snapshot_for_internalisation_strategy(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123],
                "load_target_accounts_from_snapshot": True,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "max_pos_qty": 100,
                    "allow_partial_fills": True,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session",],
            "filename": None,
        }

        # snapshot that will be used to load the instruments
        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int(
                                     (
                                             dt.datetime.strptime(date_str, "%Y-%m-%d")
                                             - dt.timedelta(days=1)
                                     ).timestamp()
                                     * 1000000
                                 )
                             ]
                             * 6,
                "account_id": [1, 2, 3, 1, 2, 3],
                "instrument_id": [123, 123, 123, 456, 456, 456],
                "internalisation_account_id": [
                    lmax_account,
                    lmax_account,
                    lmax_account,
                    -1,
                    -1,
                    -1,
                ],
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=snapshot),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )

        self.assertEqual(1, len(plans))

        plan = plans[0]
        self.assertEqual("sim_1", plan.name)
        self.assertEqual([123], plan.instruments)
        self.assertEqual(lmax_account, plan.lmax_account)
        self.assertEqual([1, 2, 3], plan.target_accounts_list)
        self.assertEqual("84967754446bc4047487c114518b989b", plan.hash)

    def test_create_simulation_plan_dont_split_by_instruments(self,):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123, 456],
                "split_by_instrument": False,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "allow_partial_fills": True,
                    "max_pos_qty": 100,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session"],
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
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=None),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None,),
            dataserver=None,
        )

        self.assertEqual(1, len(plans))

        plan = plans[0]
        self.assertEqual("sim_1", plan.name)
        self.assertEqual([123, 456], plan.instruments)
        self.assertEqual(lmax_account, plan.lmax_account)
        self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9], plan.target_accounts_list)
        self.assertEqual("c8468f55e5f74eb69f7d9127da66fedc", plan.hash)

    def test_create_simulation_plan_only_create_plans_for_account_instrument_pairs_that_have_traded_target_accounts_doesnt_have_instrument_id(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123, 456, 789],
                "filter_snapshot_for_traded_account_instrument_pairs": True,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "max_pos_qty": 100,
                    "allow_partial_fills": True,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session",],
            "filename": None,
        }

        timestamp = dt.datetime.strptime(date_str, "%Y-%m-%d")

        trades = pd.DataFrame(
            {
                "timestamp_micros": [timestamp.timestamp() * 1000000] * 2,
                "time_stamp": [timestamp.timestamp() * 1000] * 2,
                "instrument_id": [123] * 2,
                "account_id": [1, 2],
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=None),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=trades),
            dataserver=None,
        )

        self.assertEqual(1, len(plans))

        plan = plans[0]
        self.assertEqual("sim_1", plan.name)
        self.assertEqual([123], plan.instruments)
        self.assertEqual(lmax_account, plan.lmax_account)
        self.assertEqual([1, 2], plan.target_accounts_list)
        self.assertEqual("84967754446bc4047487c114518b989b", plan.hash)

    def test_create_simulation_plan_only_create_plans_for_account_instrument_pairs_that_have_traded_target_accounts_has_instrument_id(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123, 456, 789],
                "filter_snapshot_for_traded_account_instrument_pairs": True,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "max_pos_qty": 100,
                    "allow_partial_fills": True,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session",],
            "filename": None,
        }

        timestamp = dt.datetime.strptime(date_str, "%Y-%m-%d")

        trades = pd.DataFrame(
            {
                "timestamp_micros": [timestamp.timestamp() * 1000000] * 2,
                "time_stamp": [timestamp.timestamp() * 1000] * 2,
                "instrument_id": [123] * 2,
                "account_id": [1, 2],
            }
        )

        # noinspection PyTypeChecker
        config = BackTestingConfig(
            auth=None, bucket="none", pipeline=pipeline_config, output=output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date=date_str, end_date=date_str
        )
        config.optionally_set_target_accounts(
            target_accounts=pd.DataFrame(
                {
                    "account_id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                    "instrument_id": [123, 123, 123, 123, 55, 66, 77, 88, 99],
                }
            )
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=None),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=trades),
            dataserver=None,
        )

        self.assertEqual(1, len(plans))

        plan = plans[0]
        self.assertEqual("sim_1", plan.name)
        self.assertEqual([123], plan.instruments)
        self.assertEqual(lmax_account, plan.lmax_account)
        self.assertEqual([1, 2], plan.target_accounts_list)
        self.assertEqual(
            plan.hash, "84967754446bc4047487c114518b989b",
        )

    def test_create_simulation_plan_relative_account_performance_out(self,):
        date_str = "2020-12-07"
        lmax_account = 121212
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123],
                "relative_simulation": {
                    "direction": "out",
                    "comparison_accounts_type": "client",
                    "comparison_accounts": [1, 2, 3],
                },
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "allow_partial_fills": True,
                    "max_pos_qty": 100,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session"],
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
            target_accounts=pd.DataFrame({"account_id": [1, 2, 3]})
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=None),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )

        self.assertEqual(4, len(plans))

        event_filter_strings: List[str] = [p.event_filter_string for p in plans]
        self.assertEqual(
            [
                "account_id not in [1] # exclude 1",
                "account_id not in [2] # exclude 2",
                "account_id not in [3] # exclude 3",
                "account_id not in [-1] # benchmark",
            ],
            event_filter_strings,
        )

    def test_create_simulation_plan_relative_internalisation_account_performance_in(
            self,
    ):
        date_str = "2020-12-07"
        lmax_account = 121212
        comparison_account = 131313
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123],
                "relative_simulation": {
                    "direction": "in",
                    "comparison_accounts": [comparison_account],
                    "comparison_accounts_type": "internalisation",
                },
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "max_pos_qty": 100,
                    "allow_partial_fills": True,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session"],
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
            target_accounts=pd.DataFrame(
                {
                    "account_id": [1, 2, 3, 4, 5, 6, 7],
                    "internalisation_account_id": [
                        lmax_account,
                        lmax_account,
                        lmax_account,
                        comparison_account,
                        comparison_account,
                        comparison_account,
                        -1,
                    ],
                }
            )
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=None),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )

        self.assertEqual(4, len(plans))

        event_filter_strings: List[str] = [p.event_filter_string for p in plans]

        self.assertEqual(
            [
                "account_id not in [5, 6] # include 4",
                "account_id not in [4, 6] # include 5",
                "account_id not in [4, 5] # include 6",
                "account_id not in [4, 5, 6] # benchmark",
            ],
            event_filter_strings,
        )

    def test_create_simulation_plan_relative_client_account_performance_in(self,):
        date_str = "2020-12-07"
        lmax_account = 121212
        comparison_account = [7, 8]
        # pipeline Config
        pipeline_config = {
            "lmax_account": lmax_account,
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "shard": "ldprof",
            "process_client_portfolio": False,
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
                "instruments": [123],
                "relative_simulation": {
                    "direction": "in",
                    "comparison_accounts": comparison_account,
                    "comparison_accounts_type": "client",
                },
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty_buffer": 1.25,
                    "max_pos_qty": 100,
                    "allow_partial_fills": True,
                    "position_lifespan": "gfw",
                    "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
                "risk_parameters": {"risk_type": "no_risk"},
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
            "event_features": ["symbol", "order_book_id", "trading_session"],
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
            target_accounts=pd.DataFrame(
                {
                    "account_id": [1, 2, 3, 4, 5, 6, 7, 8],
                    "internalisation_account_id": [
                        lmax_account,
                        lmax_account,
                        lmax_account,
                        lmax_account,
                        lmax_account,
                        lmax_account,
                        -1,
                        -1,
                    ],
                }
            )
        )
        config.build_simulations_config(simulations_config, [])
        config.validate()

        plans: List[SimulationPlan] = Simulations.build_simulation_plans(
            config=config,
            matching_engine=DummyMatchingEngine(),
            simulation_configs=config.simulation_configs,
            snapshot_loader=SnapshotDummyLoader(snapshot=None),
            event_stream=DummyEventStream(),
            trades_loader=TradesDummyLoader(trades=None),
            dataserver=None,
        )
        self.assertEqual(3, len(plans))

        event_filter_strings: List[str] = [p.event_filter_string for p in plans]
        self.assertEqual(
            [
                "account_id not in [8] # include 7",
                "account_id not in [7] # include 8",
                "account_id not in [7, 8] # benchmark",
            ],
            event_filter_strings,
        )


if __name__ == "__main__":
    unittest.main()
