import datetime as dt
from abc import ABC
from typing import List, Any, Union, Tuple

import pandas as pd
import pytz

from math import ceil

from .simulation_plan import SimulationPlan
from ..event_stream import EventStream
from ..loaders.load_price_slippage_model import PriceSlippageLoader
from ..loaders.dataserver import DataServerLoader
from ..loaders.load_profiling import ProfilingLoader
from ..loaders.load_snapshot import SnapshotLoader
from ..loaders.load_starting_positions import StartingPositionsLoader
from ..loaders.load_tob import TobLoader
from ..loaders.load_trades import TradesLoader
from ..simulator.base import AbstractSimulator

eastern = pytz.timezone("US/Eastern")


class Simulator(AbstractSimulator, ABC):
    def __init__(
            self,
            name: str,
            dataserver: DataServerLoader,
            trades_loader: TradesLoader,
            event_stream: EventStream,
            tob_loader: TobLoader = None,
            starting_positions_loader: StartingPositionsLoader = None,
            price_slippage_loader: PriceSlippageLoader = None,
            snapshot_loader: SnapshotLoader = None,
            profiling_loader: ProfilingLoader = None,
    ):
        self.name: str = name
        self.dataserver: DataServerLoader = dataserver
        self.tob_loader: TobLoader = tob_loader
        self.trades_loader: TradesLoader = trades_loader
        self.starting_positions_loader: StartingPositionsLoader = starting_positions_loader
        self.price_slippage_loader: PriceSlippageLoader = price_slippage_loader
        self.event_stream: EventStream = event_stream
        self.snapshot_loader: SnapshotLoader = snapshot_loader
        self.profiling_loader: ProfilingLoader = profiling_loader
        self.tob: pd.DataFrame = pd.DataFrame()
        self.execution_attempt: int = 3

    @staticmethod
    def split_simulations(
            simulations: List[Any], no_of_batches: int
    ) -> List[List[Any]]:
        sims_per_batch: int = ceil(len(simulations) / no_of_batches) if len(
            simulations
        ) > 1 else 1
        sim_batches: List[List[Any]] = [
            simulations[i : i + sims_per_batch]
            for i in range(0, len(simulations), sims_per_batch)
        ]
        return sim_batches

    @staticmethod
    def filter_simulation_plan(
            plans: List[SimulationPlan], results_cache: pd.DataFrame
    ):
        plans_copy: List[SimulationPlan] = []
        _results_cache = results_cache.reset_index()  # noqa
        for i, plan in enumerate(plans):
            _sim_hash = plan.hash  # noqa
            _start_date = plan.start_date  # noqa
            _end_date = plan.end_date  # noqa
            if pd.eval(
                    "_start_date <= _results_cache.trading_session >= _end_date and _results_cache.hash == _sim_hash"
            ).any():
                continue
            else:
                plans_copy.append(plan)

        return plans_copy

    @staticmethod
    def filter_tob_for_trading_session(
            tob: pd.DataFrame, day, instruments: List[int]
    ) -> pd.DataFrame:
        tob_for_trading_session: pd.DataFrame = tob[
            tob.order_book_id.isin(instruments) & (tob.trading_session == day.date())
            ]
        return tob_for_trading_session

    @staticmethod
    def filter_trades_for_simulation(
            day: dt.datetime,
            target_account: List[int],
            instruments: List[int],
            event_filter_string: str,
            trades: pd.DataFrame,
            target_accounts: pd.DataFrame = pd.DataFrame(),
    ):
        trades_for_plan = trades[
            (trades.account_id.isin(target_account))
            & (trades.order_book_id.isin(instruments))
            & (trades.trade_date == day)
            ]
        if event_filter_string:
            trades_for_plan = trades_for_plan.query(event_filter_string)

        if any(
                [
                    x in target_accounts.columns
                    for x in [
                    "internalisation_risk",
                    "booking_risk",
                    "internalise_limit_orders",
                ]
                ]
        ):
            trades_for_plan = Simulator.add_risk_settings(
                target_accounts, trades_for_plan
            )

        return trades_for_plan

    @staticmethod
    def filter_trades_for_pushed_trades(
            day: dt.datetime,
            target_account: List[int],
            instruments: List[int],
            event_filter_string: str,
            trades: pd.DataFrame,
            target_accounts: pd.DataFrame = pd.DataFrame(),
    ):

        last_timestamp = (
            pytz.UTC.localize(day + dt.timedelta(days=1))
            .astimezone(eastern)
            .replace(hour=17)
            .astimezone(pytz.UTC)
        )

        pushed_trades = trades.loc[:][
            (trades.account_id.isin(target_account))
            & (trades.order_book_id.isin(instruments))
            & (trades.trade_date > day)
            & (trades.index.get_level_values("timestamp") < last_timestamp)
            ]

        if event_filter_string:
            pushed_trades = pushed_trades.query(event_filter_string)

        return pushed_trades

    @staticmethod
    def add_risk_settings(target_accounts, trades_for_plan):
        merge_columns = [
            x for x in ["account_id", "instrument_id"] if x in target_accounts.columns
        ]
        trades_index = trades_for_plan.index.name
        if "timestamp" in target_accounts.columns:
            target_accounts2 = target_accounts.copy()
            target_accounts2["timestamp"] = pd.to_datetime(
                target_accounts2["timestamp"], unit="ms", utc=True
            )
            trades_for_plan = pd.merge_asof(
                trades_for_plan.reset_index(),
                target_accounts2,
                on="timestamp",
                by=merge_columns,
            ).set_index(trades_index)
        else:
            trades_for_plan = pd.merge(
                trades_for_plan.reset_index(),
                target_accounts,
                on=merge_columns,
                how="left",
            ).set_index(trades_index)
        return trades_for_plan

    @staticmethod
    def _safe_slots(slots: Any) -> Union[List, Tuple]:
        if isinstance(slots, list) or isinstance(slots, tuple):
            return slots
        return [slots]
