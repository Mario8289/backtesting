import datetime as dt
from typing import List, Union, Any, Dict

import pandas as pd

from risk_backtesting.config.backtesting_output_config import BackTestingOutputConfig
from risk_backtesting.risk_manager.base import AbstractRiskManager  # noqa


class SimulationConfig:
    """
    This class sets all of the configuration required to run a backtest from the input yaml file and optional command line arguments.
    """

    def __init__(
            self,
            name: str,
            uid: str,
            version: int,
            shard: str,
            start_date: dt.date,
            end_date: dt.date,
            load_starting_positions: bool,
            load_client_starting_positions: bool,
            load_target_accounts_from_snapshot: bool,
            load_instruments_from_snapshot: bool,
            load_position_limits_from_snapshot: bool,
            load_booking_risk_from_snapshot: bool,
            load_internalisation_risk_from_snapshot: bool,
            load_booking_risk_from_target_accounts: bool,
            load_internalisation_risk_from_target_accounts: bool,
            filter_snapshot_for_strategy: bool,
            filter_snapshot_for_traded_account_instrument_pairs: bool,
            relative_simulation: bool,
            relative_simulation_direction: str,
            relative_comparison_accounts: List[int],
            relative_comparison_accounts_type: str,
            instruments: List[int],
            level: str,
            event_filter_string: str,
            calculate_cumulative_daily_pnl: bool,
            target_accounts: pd.DataFrame,
            output: BackTestingOutputConfig,
            strategy_parameters: Dict[str, Any],
            exit_parameters: Dict[str, Any],
            risk_parameters: Dict[str, Any],
            split_by_instrument: bool,
    ):
        self.name: str = name
        self.uid: str = uid
        self.version: int = version
        self.shard: str = shard
        self.start_date: dt.date = start_date
        self.end_date: dt.date = end_date
        self.load_starting_positions: bool = load_starting_positions
        self.load_client_starting_positions: bool = load_client_starting_positions
        self.load_target_accounts_from_snapshot: bool = load_target_accounts_from_snapshot
        self.load_instruments_from_snapshot: bool = load_instruments_from_snapshot
        self.load_position_limits_from_snapshot: bool = load_position_limits_from_snapshot
        self.load_booking_risk_from_snapshot: bool = load_booking_risk_from_snapshot
        self.load_internalisation_risk_from_snapshot: bool = load_internalisation_risk_from_snapshot
        self.load_booking_risk_from_target_accounts: bool = load_booking_risk_from_target_accounts
        self.load_internalisation_risk_from_target_accounts: bool = load_internalisation_risk_from_target_accounts
        self.filter_snapshot_for_strategy: bool = filter_snapshot_for_strategy
        self.filter_snapshot_for_traded_account_instrument_pairs: bool = filter_snapshot_for_traded_account_instrument_pairs
        self.relative_simulation: bool = relative_simulation
        self.relative_simulation_direction: str = relative_simulation_direction
        self.relative_comparison_accounts: List[int] = relative_comparison_accounts
        self.relative_comparison_accounts_type: str = relative_comparison_accounts_type
        self.calculate_cumulative_daily_pnl: bool = calculate_cumulative_daily_pnl
        self.level: str = level
        self.instruments: List[int] = instruments
        self.event_filter_string: str = event_filter_string
        self.target_accounts: pd.DataFrame = target_accounts
        self.output: BackTestingOutputConfig = output
        self.strategy_parameters: Dict[str, Any] = strategy_parameters
        self.exit_parameters: Dict[str, Any] = exit_parameters
        self.risk_parameters: Dict[str, Any] = risk_parameters
        self.split_by_instrument: bool = split_by_instrument

    @property
    def load_any_from_snapshot(self):
        return (
                self.load_instruments_from_snapshot
                or self.load_target_accounts_from_snapshot
                or self.load_position_limits_from_snapshot
                or self.load_booking_risk_from_snapshot
                or self.load_internalisation_risk_from_snapshot
        )

    @property
    def instruments(self) -> List[int]:
        return self._instruments

    @instruments.setter
    def instruments(self, value: Union[int, List[int]]):
        if value is None:
            self._instruments = []
        else:
            self._instruments: List[int] = value if isinstance(value, list) else [value]
