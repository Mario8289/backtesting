import datetime as dt
import hashlib
from typing import List, Type, Any, Union, Tuple, Set, Optional

import pandas as pd

from risk_backtesting.backtester import Backtester
from risk_backtesting.config.backtesting_output_config import BackTestingOutputConfig
from risk_backtesting.exit_strategy.aggressive import Aggressive
from risk_backtesting.exit_strategy.base import AbstractExitStrategy
from risk_backtesting.exit_strategy.chaser import Chaser
from risk_backtesting.exit_strategy.exit_default import ExitDefault
from risk_backtesting.exit_strategy.profit_running import ProfitRunning
from risk_backtesting.exit_strategy.trailing_stoploss import TrailingStopLoss
from risk_backtesting.model import ProfilingRankModel
from risk_backtesting.model.base import AbstractModel
from risk_backtesting.risk_manager.base import AbstractRiskManager
from risk_backtesting.strategy.base import AbstractStrategy


class SimulationPlan:
    def __init__(
            self,
            name: str,
            uid: str,
            version: int,
            shard: str,
            start_date: dt.date,
            end_date: dt.date,
            target_accounts: pd.DataFrame,
            load_snapshot: bool,
            load_trades_iteratively_by_session: bool,
            load_target_accounts_from_snapshot: bool,
            load_instruments_from_snapshot: bool,
            load_position_limits_from_snapshot: bool,
            load_booking_risk_from_snapshot: bool,
            load_internalisation_risk_from_snapshot: bool,
            load_booking_risk_from_target_accounts: bool,
            load_internalisation_risk_from_target_accounts: bool,
            lmax_account: int,
            load_starting_positions: bool,
            load_client_starting_positions: bool,
            calculate_cumulative_daily_pnl: bool,
            level: str,
            instruments: List[int],
            event_filter_string: str,
            output: BackTestingOutputConfig,
            backtester: Backtester,
            strategy: AbstractStrategy,
            risk_manager: AbstractRiskManager,
            hash_: str = None,
    ):
        self.name: str = name
        self.uid: str = uid
        self.version: int = version
        self.shard: str = shard
        self.start_date: dt.date = start_date
        self.end_date: dt.date = end_date
        self.target_accounts: pd.DataFrame = target_accounts
        self.load_snapshot: bool = load_snapshot
        self.load_trades_iteratively_by_session: bool = load_trades_iteratively_by_session
        self.load_target_accounts_from_snapshot: bool = load_target_accounts_from_snapshot
        self.load_instruments_from_snapshot: bool = load_instruments_from_snapshot
        self.load_position_limits_from_snapshot: bool = load_position_limits_from_snapshot
        self.load_booking_risk_from_snapshot: bool = load_booking_risk_from_snapshot
        self.load_internalisation_risk_from_snapshot: bool = load_internalisation_risk_from_snapshot
        self.load_booking_risk_from_target_accounts: bool = load_booking_risk_from_target_accounts
        self.load_internalisation_risk_from_target_accounts: bool = load_internalisation_risk_from_target_accounts
        self.lmax_account: int = lmax_account
        self.load_starting_positions: bool = load_starting_positions
        self.load_client_starting_positions: bool = load_client_starting_positions
        self.calculate_cumulative_daily_pnl: bool = calculate_cumulative_daily_pnl
        self.level = level
        self.instruments: List[int] = instruments
        self.event_filter_string: str = event_filter_string
        self.output: BackTestingOutputConfig = output
        self.backtester: Backtester = backtester
        self.strategy: AbstractStrategy = strategy
        self.risk_manager: AbstractRiskManager = risk_manager
        self.hash: str = hash_ if hash_ is not None else SimulationPlan._compute_hash(
            uid, version, instruments, event_filter_string, strategy, risk_manager
        )

    @property
    def target_accounts_list(self) -> Optional[List[int]]:
        if self.target_accounts is None:
            return None
        if self.target_accounts.empty:
            return []
        return self.target_accounts.account_id.unique().tolist()

    def optionally_set_target_accounts(self, target_accounts: pd.DataFrame):
        if self.target_accounts is not None and not self.target_accounts.empty:
            return

        self.target_accounts = self.filter_accounts(target_accounts)

    def filter_accounts(self, target_accounts: pd.DataFrame) -> pd.DataFrame:
        filtered_accounts: pd.DataFrame = target_accounts
        if "internalisation" == self.strategy.get_name():
            if self.lmax_account is None:
                raise ValueError(
                    "account_id must be provided for strategy type 'internalisation'"
                )
            filtered_accounts = filtered_accounts[
                filtered_accounts.internalisation_account_id == self.lmax_account
                ]

        if "bbooking" in self.strategy.get_name():
            filtered_accounts = filtered_accounts[filtered_accounts.booking_risk > 0]
        return filtered_accounts

    def append_strategy_params(self, df: pd.DataFrame):
        # add simulation and hash
        df["simulation"] = self.name
        df["hash"] = self.hash
        df["rpnl_cum_hash"] = df.groupby("hash")["rpnl"].cumsum()

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
            ExitDefault,
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
