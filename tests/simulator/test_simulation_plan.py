import datetime as dt
import unittest

import pandas as pd

from risk_backtesting.config.backtesting_output_config import BackTestingOutputConfig
from risk_backtesting.config.simulation_config import SimulationConfig
from risk_backtesting.exit_strategy.aggressive import Aggressive
from risk_backtesting.exit_strategy.exit_default import ExitDefault
from risk_backtesting.risk_manager import AbstractRiskManager, create_risk_manager
from risk_backtesting.risk_manager.no_risk import NoRisk
from risk_backtesting.simulator.simulation_plan import SimulationPlan
from risk_backtesting.strategy import AbstractStrategy, create_strategy
from risk_backtesting.strategy.internalisation_strategy import InternalisationStrategy


class TestSimulationPlan(unittest.TestCase):
    def test_create_simulation_config(self):
        date = dt.date(2020, 1, 1)

        strategy_params = {
            "strategy_type": "internalisation",
            "max_pos_qty": 100,
            "max_pos_qty_buffer": 1.25,
            "max_pos_qty_type": "contracts",
            "allow_partial_fills": True,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": "gfw",
            "position_lifespan_exit_parameters": {"exit_type": "exit_default"},
            "account_id": 4,
            "instruments": 123,
        }

        exit_params = {
            "exit_type": "aggressive",
            "takeprofit_limit": 50,
            "stoploss_limit": 70,
        }

        risk_params = {"risk_type": "no_risk"}
        output_parameters = {
            "resample_rule": None,
            "event_features": [
                "symbol",
                "order_book_id",
                "trading_session_year",
                "trading_session_month",
                "trading_session_day",
            ],
            "save": True,
            "by": None,
            "freq": "D",
            "mode": "a",
            "filesystem": "local",
            "bucket": "risk-temp",
            "directory": "/home/jovyan/work/outputs",
            "file_type": "csv",
            "store_index": False,
            "file": None,
        }

        simulation_config: SimulationConfig = SimulationConfig(
            name="test",
            uid="test1",
            version=1,
            shard="ldprof",
            start_date=date,
            end_date=date,
            load_starting_positions=True,
            load_client_starting_positions=False,
            load_target_accounts_from_snapshot=False,
            load_instruments_from_snapshot=False,
            load_position_limits_from_snapshot=False,
            load_booking_risk_from_snapshot=False,
            load_internalisation_risk_from_snapshot=False,
            load_booking_risk_from_target_accounts=False,
            load_internalisation_risk_from_target_accounts=False,
            filter_snapshot_for_strategy=False,
            filter_snapshot_for_traded_account_instrument_pairs=False,
            relative_simulation=False,
            relative_simulation_direction=None,
            relative_comparison_accounts=None,
            relative_comparison_accounts_type=None,
            instruments=[123],
            level="trades_only",
            event_filter_string="filter_string==1",
            calculate_cumulative_daily_pnl=True,
            target_accounts=pd.DataFrame(columns=["account_id"]),
            output=BackTestingOutputConfig(output_parameters, False),
            strategy_parameters=strategy_params,
            exit_parameters=exit_params,
            risk_parameters=risk_params,
            split_by_instrument=True,
        )

        strategy: AbstractStrategy = create_strategy(
            simulation_config.strategy_parameters, simulation_config.exit_parameters
        )
        risk_manager: AbstractRiskManager = create_risk_manager(
            simulation_config.risk_parameters
        )

        plan: SimulationPlan = SimulationPlan(
            name=simulation_config.name,
            uid=simulation_config.uid,
            version=simulation_config.version,
            shard=simulation_config.shard,
            start_date=simulation_config.start_date,
            end_date=simulation_config.end_date,
            target_accounts=pd.DataFrame({"account_id": [1, 2]}),
            load_snapshot=False,
            load_trades_iteratively_by_session=False,
            load_target_accounts_from_snapshot=False,
            load_instruments_from_snapshot=False,
            load_position_limits_from_snapshot=False,
            load_booking_risk_from_snapshot=False,
            load_internalisation_risk_from_snapshot=False,
            load_booking_risk_from_target_accounts=False,
            load_internalisation_risk_from_target_accounts=False,
            lmax_account=4,
            load_starting_positions=simulation_config.load_starting_positions,
            load_client_starting_positions=simulation_config.load_client_starting_positions,
            calculate_cumulative_daily_pnl=simulation_config.calculate_cumulative_daily_pnl,
            level=simulation_config.level,
            instruments=simulation_config.instruments,
            event_filter_string=simulation_config.event_filter_string,
            output=simulation_config.output,
            backtester=None,
            strategy=strategy,
            risk_manager=risk_manager,
        )

        self.assertIsInstance(plan.strategy, InternalisationStrategy)
        self.assertEqual(plan.hash, "a7c9082a299fb2b2c104f4f22aee86bb")
        self.assertEqual(
            plan.strategy.max_pos_qty, {123: strategy_params["max_pos_qty"]}
        )
        self.assertEqual(
            plan.strategy.max_pos_qty_buffer, strategy_params["max_pos_qty_buffer"],
        )
        self.assertEqual(
            plan.strategy.position_lifespan, strategy_params["position_lifespan"]
        )

        self.assertIsInstance(plan.strategy.exit_strategy, Aggressive)
        self.assertEqual(
            plan.strategy.exit_strategy.takeprofit_limit,
            exit_params["takeprofit_limit"],
        )
        self.assertEqual(
            plan.strategy.exit_strategy.stoploss_limit, exit_params["stoploss_limit"],
        )

        self.assertIsInstance(
            plan.strategy.position_lifespan_exit_strategy, ExitDefault
        )
        self.assertIsInstance(plan.output, BackTestingOutputConfig)
        self.assertIsInstance(plan.risk_manager, NoRisk)


if __name__ == "__main__":
    unittest.main()
