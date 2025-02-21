import datetime as dt
import hashlib
from typing import List, Type, Any, Union, Tuple, Set, Optional

import pandas as pd

from backtesting.backtester import Backtester
from backtesting.config.backtesting_output_config import BackTestingOutputConfig
from backtesting.exit_strategy.aggressive import Aggressive
from backtesting.exit_strategy.base import AbstractExitStrategy
from backtesting.exit_strategy.chaser import Chaser
from backtesting.exit_strategy.no_exit import NoExit
from backtesting.exit_strategy.profit_running import ProfitRunning
from backtesting.exit_strategy.trailing_stoploss import TrailingStopLoss
from backtesting.model import ProfilingRankModel
from backtesting.model.base import AbstractModel
from backtesting.risk_manager.base import AbstractRiskManager
from backtesting.strategy.base import AbstractStrategy
from backtesting.subscriptions_cache.subscriptions_cache import SubscriptionsCache


class SimulationPlan:
    def __init__(
            self,
            name: str,
            uid: str,
            version: int,
            start_date: dt.date,
            end_date: dt.date,
            account: int,
            load_starting_positions: bool,
            calculate_cumulative_daily_pnl: bool,
            level: str,
            instruments: List[int],
            event_filter_string: str,
            subscriptions_cache: SubscriptionsCache,
            output: BackTestingOutputConfig,
            backtester: Backtester,
            strategy: AbstractStrategy,
            risk_manager: AbstractRiskManager,
            hash_: str = None,
    ):
        self.name: str = name
        self.uid: str = uid
        self.version: int = version
        self.start_date: dt.date = start_date
        self.end_date: dt.date = end_date
        self.account: int = account
        self.load_starting_positions: bool = load_starting_positions
        self.calculate_cumulative_daily_pnl: bool = calculate_cumulative_daily_pnl
        self.level = level
        self.instruments: List[int] = instruments
        self.event_filter_string: str = event_filter_string
        self.subscriptions_cache: SubscriptionsCache = subscriptions_cache
        self.output: BackTestingOutputConfig = output
        self.backtester: Backtester = backtester
        self.strategy: AbstractStrategy = strategy
        self.risk_manager: AbstractRiskManager = risk_manager
        self.hash: str = hash_ if hash_ is not None else SimulationPlan._compute_hash(
            uid, version, instruments, event_filter_string, strategy, risk_manager
        )

    def append_strategy_params(self, df: pd.DataFrame):
        # add simulation and hash
        df["simulation"] = self.name
        df["hash"] = self.hash
        df["realised_pnl_cum_hash"] = df.groupby("hash")["realised_pnl"].cumsum()

        # add strategy properties
        for (slot, value) in SimulationPlan._output_values(self.strategy):
            if not isinstance(value, AbstractExitStrategy):
                df[f"strategy_{slot}"] = self.strategy.slot_lambda(df, slot, value)

        # add model strategy properties
        if hasattr(self.strategy, "model"):
            for (slot, value) in SimulationPlan._output_values(self.strategy.model):
                df[f"model_{slot}"] = value if value else -1

        # add exit strategy properties
        if hasattr(self.strategy, "exit_strategy"):
            for (slot, value) in SimulationPlan._output_values(
                    self.strategy.exit_strategy
            ):
                df[f"exit_{slot}"] = value if value else -1

        # add risk manager properties
        for (slot, value) in SimulationPlan._output_values(self.risk_manager):
            df[f"risk_{slot}"] = len(value) if isinstance(value, list) else value

        # add event filter string
        if self.event_filter_string:
            df["event_filter_string"] = self.event_filter_string

        return df

    @staticmethod
    def _compute_hash(
            uid: str,
            version: int,
            instruments: List[int],
            event_filter_string: str,
            strategy: AbstractStrategy,
            risk_manager: AbstractRiskManager,
    ) -> str:
        exit_strategies: Set[Type[AbstractExitStrategy]] = {
            TrailingStopLoss,
            Aggressive,
            ProfitRunning,
            Chaser,
            NoExit,
        }

        models: Set[Type[AbstractModel]] = {ProfilingRankModel}

        strategy_parameters: List = []
        for slot in SimulationPlan._safe_slots(strategy):
            obj = strategy.__getattribute__(slot)
            obj_type: Type = type(obj)

            if obj_type not in exit_strategies and obj_type not in models:
                strategy_parameters.append(obj)

        exit_parameters: List = []
        if hasattr(strategy, "exit_strategy"):
            exit_parameters.extend(
                [
                    strategy.exit_strategy.__getattribute__(slot)
                    for slot in SimulationPlan._safe_slots(strategy.exit_strategy)
                ]
            )

        lifespan_exit_parameters: List = []
        if hasattr(strategy, "position_lifespan_exit_strategy"):
            exit_parameters.extend(
                [
                    strategy.position_lifespan_exit_strategy.__getattribute__(slot)
                    for slot in SimulationPlan._safe_slots(
                    strategy.position_lifespan_exit_strategy
                )
                ]
            )

        risk_parameters: List = [
            risk_manager.__getattribute__(slot)
            for slot in SimulationPlan._safe_slots(risk_manager)
        ]

        hash_str: str = "".join(
            map(
                str,
                [
                    strategy_parameters,
                    exit_parameters,
                    lifespan_exit_parameters,
                    risk_parameters,
                    [instruments, event_filter_string, uid, version],
                ],
            )
        )

        return hashlib.md5(hash_str.encode()).hexdigest()

    @staticmethod
    def _safe_slots(obj: Any) -> Union[List, Tuple]:
        if hasattr(obj, "__slots__"):
            if isinstance(obj.__slots__, list) or isinstance(obj.__slots__, tuple):
                return obj.__slots__
            return [obj.__slots__]
        return []

    @staticmethod
    def _output_values(obj: Any) -> List[Tuple]:
        slots: List = SimulationPlan._safe_slots(obj)
        if hasattr(obj, "__output_slots__"):
            slots = obj.__output_slots__
        return [(slot, getattr(obj, slot)) for slot in slots]
