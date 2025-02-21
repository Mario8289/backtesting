import logging
from copy import deepcopy
from typing import List, Dict, Optional, Tuple, AnyStr

import pandas as pd

from backtesting.backtester import Backtester
from backtesting.config.backtesting_config import BackTestingConfig
from backtesting.config.simulation_config import SimulationConfig
from backtesting.event_stream import EventStream
from backtesting.matching_engine import AbstractMatchingEngine
from backtesting.risk_manager import AbstractRiskManager, create_risk_manager
from backtesting.simulator.simulation_plan import SimulationPlan
from backtesting.strategy import AbstractStrategy, create_strategy
from backtesting.subscriptions.subscription import Subscription
from backtesting.subscriptions_cache.subscriptions_cache import SubscriptionsCache


class Simulations:
    @staticmethod
    def get_event_filter_string(account) -> str:
        account_inout = account[1]
        direction = account[2]
        if direction == "out":
            account_exc = account[0] + [account[1]]
            if account_inout == -1:
                return f"account_id not in [{', '.join(map(str, account_exc))}] # benchmark"
            else:
                return f"account_id not in [{', '.join(map(str, account_exc))}] # exclude {account_inout}"
        else:
            account_exc = account[0]
            if account_inout == -1:
                return f"account_id not in [{', '.join(map(str, account_exc))}] # benchmark"
            else:
                return f"account_id not in [{', '.join(map(str, account_exc))}] # include {account_inout}"

    @staticmethod
    def create_event_filter_strings(
            lmax_account: int,
            snapshot: pd.DataFrame,
            direction: str,
            comparison_accounts: List[int],
            comparison_accounts_type: str,
    ) -> List[str]:
        if direction == "out":
            if comparison_accounts_type == "internalisation":
                accounts = (
                    snapshot[snapshot.internalisation_account_id == lmax_account][
                        "account_id"
                    ]
                    .unique()
                    .tolist()
                )
                accounts.append(-1)
                exc_accounts = (
                    snapshot[snapshot.internalisation_account_id != lmax_account][
                        "account_id"
                    ]
                    .unique()
                    .tolist()
                )
                accounts = [[exc_accounts, x, direction] for x in accounts]

            elif comparison_accounts_type == "client":

                accounts = (
                    snapshot[snapshot.account_id.isin(comparison_accounts)][
                        "account_id"
                    ]
                    .unique()
                    .tolist()
                )
                accounts.append(-1)
                accounts = [[[], x, direction] for x in accounts]
            else:
                raise ValueError(
                    f"comparison_account_type {comparison_accounts_type} is not valid"
                )
            string_filters = list(map(Simulations.get_event_filter_string, accounts))

            return string_filters

        elif direction == "in":

            if comparison_accounts_type == "internalisation":
                accounts = (
                    snapshot[
                        snapshot.internalisation_account_id.isin(comparison_accounts)
                    ]["account_id"]
                    .unique()
                    .tolist()
                )
                accounts.append(-1)
                accounts = [
                    [[x2 for x2 in accounts if x2 not in [x, -1]], x, direction]
                    for x in accounts
                ]
            elif comparison_accounts_type == "client":

                accounts = (
                    snapshot[snapshot.account_id.isin(comparison_accounts)][
                        "account_id"
                    ]
                    .unique()
                    .tolist()
                )
                accounts.append(-1)
                accounts = [
                    [[x2 for x2 in accounts if x2 not in [x, -1]], x, direction]
                    for x in accounts
                ]

            else:
                raise ValueError(
                    f"comparison_account_type {comparison_accounts_type} is not valid"
                )

            return list(map(Simulations.get_event_filter_string, accounts))
        return []

    @staticmethod
    def filter_snapshot_by_instruments(snapshot: pd.DataFrame, instruments: List[int]):
        if "instrument_id" in snapshot.columns:
            s = snapshot[snapshot.instrument_id.isin(instruments)]
            return s
        else:
            return snapshot

    @staticmethod
    def filter_snapshot_for_traded_account_instrument_pairs(
            snapshot: pd.DataFrame, trades: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, List[int]]:
        traded_account_instrument_pairs = (
            trades[["instrument_id", "account_id"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        if "instrument_id" in snapshot.columns:
            filtered: pd.DataFrame = snapshot.merge(traded_account_instrument_pairs)
            return filtered, filtered.instrument_id.unique().tolist()
        else:
            filtered: pd.DataFrame = snapshot.merge(
                traded_account_instrument_pairs.loc[:, ["account_id"]]
            )
            instruments: List[int] = (
                traded_account_instrument_pairs[
                    traded_account_instrument_pairs.account_id.isin(snapshot.account_id)
                ]["instrument_id"]
                .unique()
                .tolist()
            )
            return filtered, instruments

    @staticmethod
    def filter_snapshot_for_position_limit(
            account: int, instruments: List, snapshot: pd.DataFrame
    ) -> pd.DataFrame:
        return snapshot[
            (snapshot.internalisation_account_id == account)
            & (snapshot.instrument_id.isin(instruments))
            ].drop_duplicates(
            subset=[
                "internalisation_position_limit",
                "instrument_id",
                "internalisation_account_id",
            ],
            keep="first",
        )

    @staticmethod
    def get_position_limit_from_snapshot(
            instruments: List, snapshot: pd.DataFrame
    ) -> pd.DataFrame:
        return (
            snapshot[(snapshot.instrument_id.isin(instruments))][
                "internalisation_position_limit"
            ]
            .unique()
            .item()
        )

    @staticmethod
    def build_simulation_plans(
            config: BackTestingConfig,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[AnyStr, SimulationConfig],
            subscriptions: Dict[AnyStr, Subscription],
            subscriptions_cache: SubscriptionsCache,
            event_stream: EventStream,
    ) -> List[SimulationPlan]:
        plans: List[SimulationPlan] = []

        for simulation_config in simulation_configs.values():

            strategy: AbstractStrategy = create_strategy(
                simulation_config.strategy_parameters,
                simulation_config.exit_parameters,
            )

            strategy_subscriptions = {k: v for (k, v) in subscriptions.items() if k in simulation_config.subscriptions}

            instruments: List[int] = simulation_config.instruments

            plans.extend(
                Simulations.split_simulation_config(
                    config=config,
                    event_stream=event_stream,
                    instruments=instruments,
                    matching_engine=matching_engine,
                    simulation_config=simulation_config,
                    subscriptions=strategy_subscriptions,
                    subscriptions_cache=subscriptions_cache,
                    strategy=strategy,
                )
            )

        return plans

    @staticmethod
    def split_simulation_config(
            config: BackTestingConfig,
            event_stream: EventStream,
            instruments: List[int],
            matching_engine: AbstractMatchingEngine,
            simulation_config: SimulationConfig,
            strategy: AbstractStrategy,
            subscriptions: Dict[AnyStr, Subscription],
            subscriptions_cache: SubscriptionsCache,
            position_limit_snapshot: pd.DataFrame = None,
    ) -> List[SimulationPlan]:
        logger = logging.getLogger("SplitSimulations")
        event_filter_strings: List[str] = []
        plans_delta: List[SimulationPlan] = []

        if simulation_config.split_by_instrument:
            for instrument in instruments:
                instrument_strategy = deepcopy(strategy)
                instrument_strategy.filter(instrument=instrument)

                if event_filter_strings:
                    for i, event_filter_string in enumerate(event_filter_strings):
                        logger.info(
                            f"Split Simulations {config.instruments} {i}/{len(event_filter_strings)}, "
                            f"using filter string {event_filter_string}"
                        )
                        plans_delta.append(
                            Simulations.build_simulation_plan(
                                config=config,
                                matching_engine=matching_engine,
                                event_stream=event_stream,
                                config_template=simulation_config,
                                instruments=[instrument],
                                strategy=instrument_strategy,
                                subscriptions=subscriptions,
                                subscriptions_cache=subscriptions_cache,
                                event_filter_string=event_filter_string,
                            )
                        )
                else:
                    plans_delta.append(
                        Simulations.build_simulation_plan(
                            config=config,
                            matching_engine=matching_engine,
                            event_stream=event_stream,
                            config_template=simulation_config,
                            instruments=[instrument],
                            strategy=instrument_strategy,
                            subscriptions=subscriptions,
                            subscriptions_cache=subscriptions_cache,
                            event_filter_string=simulation_config.event_filter_string,
                        )
                    )
        else:

            if simulation_config.load_position_limits_from_snapshot:
                simulation_config.strategy_parameters[
                    "max_pos_qty"
                ] = strategy.update_max_pos_qty(
                    Simulations.get_position_limit_from_snapshot(
                        instruments, position_limit_snapshot
                    )
                )

            if event_filter_strings:
                for event_filter_string in event_filter_strings:
                    plans_delta.append(
                        Simulations.build_simulation_plan(
                            config=config,
                            matching_engine=matching_engine,
                            event_stream=event_stream,
                            config_template=simulation_config,
                            instruments=instruments,
                            strategy=strategy,
                            event_filter_string=event_filter_string,
                        )
                    )
            else:
                plans_delta.append(
                    Simulations.build_simulation_plan(
                        config=config,
                        matching_engine=matching_engine,
                        event_stream=event_stream,
                        config_template=simulation_config,
                        instruments=instruments,
                        strategy=strategy,
                        subscriptions=subscriptions,
                        event_filter_string=simulation_config.event_filter_string,
                    )
                )

        return plans_delta

    @staticmethod
    def build_simulation_plan(
            config: BackTestingConfig,
            matching_engine: AbstractMatchingEngine,
            event_stream: EventStream,
            config_template: SimulationConfig,
            instruments: List[int],
            strategy: AbstractStrategy,
            subscriptions: Dict[AnyStr, Subscription],
            subscriptions_cache: SubscriptionsCache,
            event_filter_string: str,
    ) -> SimulationPlan:
        simulation_config = deepcopy(config_template)
        simulation_config.instruments = instruments

        risk_manager: AbstractRiskManager = create_risk_manager(
            simulation_config.risk_parameters
        )
        return SimulationPlan(
            name=simulation_config.name,
            uid=simulation_config.uid,
            version=simulation_config.version,
            start_date=simulation_config.start_date,
            end_date=simulation_config.end_date,
            account=config.account,
            load_starting_positions=simulation_config.load_starting_positions,
            subscriptions_cache=subscriptions_cache,
            calculate_cumulative_daily_pnl=simulation_config.calculate_cumulative_daily_pnl,
            level=simulation_config.level,
            instruments=simulation_config.instruments,
            event_filter_string=event_filter_string,
            output=simulation_config.output,
            strategy=strategy,
            risk_manager=risk_manager,
            backtester=Backtester(
                risk_manager=risk_manager,
                strategy=strategy,
                instrument=simulation_config.instruments,
                netting_engine=config.netting_engine,
                matching_method=config.matching_method,
                event_stream=event_stream,
                subscriptions=subscriptions,
                matching_engine=matching_engine,
                process_portfolio=config.process_portfolio,
                store_order_snapshot=config.store_order_snapshot,
                store_md_snapshot=config.store_md_snapshot,
                store_trade_snapshot=config.store_trade_snapshot,
                store_eod_snapshot=config.store_eod_snapshot,
            ),
        )
