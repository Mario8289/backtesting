import logging
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from risk_backtesting.config.backtesting_output_config import BackTestingOutputConfig
from risk_backtesting.config.simulation_config import SimulationConfig
from risk_backtesting.config.simulation_config_factory import SimulationConfigFactory
from risk_backtesting.config.type_parser import parse_bool, parse_date


def safe_get(dict_: Dict[str, Any], key: str, default: Any) -> Any:
    value: Any = dict_.get(key, default)
    if value is None:
        return default
    return value


DEFAULT_LMAX_ACCOUNT: int = 4


class BackTestingConfig:
    def __init__(
        self,
        auth: Dict[str, Dict[str, Any]],
        bucket: str,
        pipeline: Dict[str, Any],
        output: Dict[str, Any],
    ):
        self.num_batches: int = 1
        self.num_cores: int = 1
        self.auth: Dict[str, Dict[str, Any]] = auth
        self.bucket: str = bucket
        self.uid: str = pipeline.get("uid")
        self.version: int = int(pipeline["version"]) if pipeline.get(
            "version"
        ) is not None else None
        self.start_date: date = parse_date(pipeline.get("start_date"))
        self.end_date: date = parse_date(pipeline.get("end_date"))
        self.lmax_account: int = int(pipeline.get("lmax_account", DEFAULT_LMAX_ACCOUNT))
        self.target_accounts: pd.DataFrame = pd.DataFrame(columns=["account_id"])
        self.level: str = pipeline.get("level", "mark_to_market")
        self.netting_engine: str = pipeline.get("netting_engine", "side_of_book")
        self.matching_method: str = pipeline.get("matching_method", "fifo")
        self.shard: str = pipeline["shard"]
        self.load_starting_positions: bool = parse_bool(
            pipeline.get("load_starting_positions", True)
        )
        self.load_trades_iteratively_by_session: bool = parse_bool(
            pipeline.get("load_trades_iteratively_by_session", False)
        )
        self.load_client_starting_positions: bool = parse_bool(
            pipeline.get("load_client_starting_positions", False)
        )
        self.calculate_cumulative_daily_pnl: bool = parse_bool(
            pipeline.get("calculate_cumulative_daily_pnl", True)
        )
        self.process_lmax_portfolio: bool = parse_bool(
            pipeline.get("process_lmax_portfolio", True)
        )
        self.store_lmax_trade_snapshot: bool = parse_bool(
            pipeline.get("store_lmax_trade_snapshot", True)
        )
        self.store_lmax_md_snapshot: bool = parse_bool(
            pipeline.get("store_lmax_md_snapshot", True)
        )
        self.store_lmax_eod_snapshot: bool = parse_bool(
            pipeline.get("store_lmax_eod_snapshot", False)
        )
        self.process_client_portfolio: bool = parse_bool(
            pipeline.get("process_client_portfolio", False)
        )
        self.store_client_trade_snapshot: bool = parse_bool(
            pipeline.get("store_client_trade_snapshot", False)
        )
        self.store_client_md_snapshot: bool = parse_bool(
            pipeline.get("store_client_md_snapshot", False)
        )
        self.store_client_eod_snapshot: bool = parse_bool(
            pipeline.get("store_client_eod_snapshot", False)
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
        self.output: BackTestingOutputConfig = BackTestingOutputConfig(
            output, self.calculate_cumulative_daily_pnl
        )
        self.simulation_configs: Dict[str, SimulationConfig] = {}
        self.instruments: List[int] = []

    @property
    def load_snapshot_from_history(self):
        return any([s.load_any_from_snapshot for s in self.simulation_configs.values()])

    @property
    def load_target_accounts_from_snapshot(self):
        return any(
            [
                s.load_target_accounts_from_snapshot
                for s in self.simulation_configs.values()
            ]
        )

    @property
    def load_trades_to_filter_snapshot(self):
        return any(
            [
                s.filter_snapshot_for_traded_account_instrument_pairs
                for s in self.simulation_configs.values()
            ]
        )

    @property
    def target_accounts_list(self) -> Optional[List[int]]:
        if self.target_accounts is None:
            return None
        if self.target_accounts.empty:
            return []
        return self.target_accounts.account_id.unique().tolist()

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

    def optionally_set_target_accounts(self, target_accounts: pd.DataFrame):
        if target_accounts is not None and not target_accounts.empty:
            if len(target_accounts.account_id) != len(
                target_accounts.account_id.unique()
            ):
                duplicate_columns = [
                    x
                    for x in ["timestamp", "account_id", "instrument_id"]
                    if x in target_accounts.columns
                ]
                target_accounts = target_accounts.drop_duplicates(
                    duplicate_columns, keep="first"
                ).reset_index(drop=True)
                BackTestingConfig._warn(
                    "Duplicate target accounts found in command line csv"
                )
            self.target_accounts = target_accounts

    def build_simulations_config(
        self, simulations: Dict[str, Any], simulations_filter: List[str],
    ):
        self.simulation_configs = SimulationConfigFactory.build_simulation_configs(
            simulations,
            simulations_filter,
            self.lmax_account,
            self.uid,
            self.version,
            self.shard,
            self.start_date,
            self.end_date,
            self.load_starting_positions,
            self.load_client_starting_positions,
            self.calculate_cumulative_daily_pnl,
            self.level,
            self.output,
            self.target_accounts,
        )

        # only set the global instruments if no simulations instruments list is derived from snapshot
        if all(
            [
                not x.load_instruments_from_snapshot
                for x in self.simulation_configs.values()
            ]
        ):
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
        # todo: cintezam, maybe refactor this a bit
        if self.target_accounts is None or self.target_accounts.empty:
            misconfigured_simulations_accounts: List[str] = []
            for label, details in self.simulation_configs.items():
                if not details.load_target_accounts_from_snapshot:
                    misconfigured_simulations_accounts.append(label)
            if misconfigured_simulations_accounts:
                raise ValueError(
                    f"No accounts provided for simulations {misconfigured_simulations_accounts}"
                )
        misconfigured_simulations_instruments: List[str] = []
        for label, details in self.simulation_configs.items():
            if not details.load_instruments_from_snapshot and not details.instruments:
                misconfigured_simulations_instruments.append(label)
        if misconfigured_simulations_instruments:
            raise ValueError(
                f"No instruments provided for simulations {misconfigured_simulations_instruments}"
            )
        if (
            self.store_client_eod_snapshot
            and not self.event_stream_params["include_eod_snapshot"]
        ):
            raise ValueError(
                "Cannot store the client eod snapshot if event stream does not not have 'include_eod_snapshot' enabled"
            )
        if (
            self.store_lmax_eod_snapshot
            and not self.event_stream_params["include_eod_snapshot"]
        ):
            raise ValueError(
                "Cannot store the lmax eod snapshot if event stream does not not have 'include_eod_snapshot' enabled"
            )

    @staticmethod
    def _warn(message: str):
        logging.getLogger("BackTestingConfig").warning(message)
