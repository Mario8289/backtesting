import datetime as dt
from typing import List, Union, Any, Dict, AnyStr

import pandas as pd

from backtesting.config.backtesting_output_config import BackTestingOutputConfig
from backtesting.risk_manager.base import AbstractRiskManager  # noqa


class SimulationConfig:
    """
    This class sets all of the configuration required to run a backtest from the input yaml file and optional command line arguments.
    """

    def __init__(
            self,
            name: str,
            uid: str,
            version: int,
            start_date: dt.date,
            end_date: dt.date,
            load_starting_positions: bool,
            subscriptions: List[AnyStr],
            instruments: List[int],
            level: AnyStr,
            event_filter_string: AnyStr,
            calculate_cumulative_daily_pnl: bool,
            output: BackTestingOutputConfig,
            strategy_parameters: Dict[AnyStr, Any],
            exit_parameters: Dict[AnyStr, Any],
            risk_parameters: Dict[AnyStr, Any],
            split_by_instrument: bool,
    ):
        self.name: AnyStr = name
        self.uid: AnyStr = uid
        self.version: int = version
        self.start_date: dt.date = start_date
        self.end_date: dt.date = end_date
        self.load_starting_positions: bool = load_starting_positions
        self.subscriptions: bool = subscriptions
        self.calculate_cumulative_daily_pnl: bool = calculate_cumulative_daily_pnl
        self.level: str = level
        self.instruments: List[int] = instruments
        self.event_filter_string: str = event_filter_string
        self.output: BackTestingOutputConfig = output
        self.strategy_parameters: Dict[str, Any] = strategy_parameters
        self.exit_parameters: Dict[str, Any] = exit_parameters
        self.risk_parameters: Dict[str, Any] = risk_parameters
        self.split_by_instrument: bool = split_by_instrument

    @property
    def instruments(self) -> List[int]:
        return self._instruments

    @instruments.setter
    def instruments(self, value: Union[int, List[int]]):
        if value is None:
            self._instruments = []
        else:
            self._instruments: List[int] = value if isinstance(value, list) else [value]
