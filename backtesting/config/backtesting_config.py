import logging
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from backtesting.config.backtesting_output_config import BackTestingOutputConfig
from backtesting.config.simulation_config import SimulationConfig
from backtesting.config.simulation_config_factory import SimulationConfigFactory
from backtesting.config.type_parser import parse_bool, parse_date


def safe_get(dict_: Dict[str, Any], key: str, default: Any) -> Any:
    value: Any = dict_.get(key, default)
    if value is None:
        return default
    return value


_ACCOUNT: int = 4


class BackTestingConfig:
    def __init__(
        self,
        subscriptions: Dict[str, Any],
        subscriptions_cache: Dict[str, Any],
        pipeline: Dict[str, Any],
        output: Dict[str, Any],
    ):
        self.num_batches: int = 1
        self.num_cores: int = 1
        self.uid: str = pipeline.get("uid")
        self.version: int = int(pipeline["version"]) if pipeline.get(
            "version"
        ) is not None else None
        self.start_date: date = parse_date(pipeline.get("start_date"))
        self.end_date: date = parse_date(pipeline.get("end_date"))
        self.account: int = int(pipeline.get("account", _ACCOUNT))
        self.target_accounts: pd.DataFrame = pd.DataFrame(columns=["account_id"])
        self.level: str = pipeline.get("level", "mark_to_market")
        self.netting_engine: str = pipeline.get("netting_engine", "side_of_book")
        self.matching_method: str = pipeline.get("matching_method", "fifo")
        self.load_starting_positions: bool = parse_bool(
            pipeline.get("load_starting_positions", True)
        )
        self.calculate_cumulative_daily_pnl: bool = parse_bool(
            pipeline.get("calculate_cumulative_daily_pnl", True)
        )
        self.process_portfolio: bool = parse_bool(
            pipeline.get("process_portfolio", True)
        )
        self.store_trade_snapshot: bool = parse_bool(
            pipeline.get("store_trade_snapshot", True)
        )
        self.store_order_snapshot: bool = parse_bool(
            pipeline.get("store_order_snapshot", True)
        )
        self.store_md_snapshot: bool = parse_bool(
            pipeline.get("store_md_snapshot", True)
        )
        self.store_eod_snapshot: bool = parse_bool(
            pipeline.get("store_eod_snapshot", False)
        )
        self.simulator_type: str = pipeline.get("simulator", "simulation_pool")
        self.event_stream_params: Dict[str, Any] = safe_get(
            pipeline,
            "event_stream_parameters",
            {
                "event_stream_type": "event_stream_snapshot",
                "sample_rate": "s",
                "include_eod_snapshot": True,
            },
        )
        self.matching_engine_params: Dict[str, Any] = safe_get(
            pipeline,
            "matching_engine_parameters",
            {"matching_engine_type": "matching_engine_default"},
        )
        self.subscriptions: Dict[str, Any] = subscriptions
        self.subscriptions_cache: Dict[str, Any] = subscriptions_cache
        self.output: BackTestingOutputConfig = BackTestingOutputConfig.create(
            config=output,
            calculate_cumulative_daily_pnl=self.calculate_cumulative_daily_pnl
        )
        self.simulation_configs: Dict[str, SimulationConfig] = {}
        self.instruments: List[int] = []

    def optionally_override_running_config_parameters(
        self,
        start_date: str = None,
        end_date: str = None,
        num_cores: int = None,
        num_batches: int = None,
    ):
        if start_date:
            self.start_date = parse_date(start_date)
        if end_date:
            self.end_date = parse_date(end_date)
        if num_cores is not None:
            self.num_cores = num_cores
        if num_batches is not None:
            self.num_batches = num_batches

    def build_simulations_config(
        self, simulations: Dict[str, Any], simulations_filter: List[str],
    ):
        self.simulation_configs = SimulationConfigFactory.build_simulation_configs(
            simulations,
            simulations_filter,
            self.account,
            self.uid,
            self.version,
            self.start_date,
            self.end_date,
            self.load_starting_positions,
            self.calculate_cumulative_daily_pnl,
            self.level,
            self.output,
        )

        self.instruments = list(
                set(
                    [
                        x
                        for sublist in [
                            v.instruments for (k, v) in self.simulation_configs.items()
                        ]
                        for x in sublist
                    ]
                )
            )

    def validate(self):
        if not self.simulation_configs:
            raise ValueError(
                "No simulations provided. What, exactly, am I meant to backtest?"
            )
        misconfigured_simulations_instruments: List = []
        for label, details in self.simulation_configs.items():
            if not details.instruments:
                misconfigured_simulations_instruments.append(label)

        if misconfigured_simulations_instruments:
            raise ValueError(
                f"No instruments provided for simulations {misconfigured_simulations_instruments}"
            )

        if (
            self.store_eod_snapshot
            and not self.event_stream_params["include_eod_snapshot"]
        ):
            raise ValueError(
                "Cannot store the eod snapshot if event stream does not not have 'include_eod_snapshot' enabled"
            )

    @staticmethod
    def _warn(message: str):
        logging.getLogger("BackTestingConfig").warning(message)
