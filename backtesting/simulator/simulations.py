import logging
from copy import deepcopy
from typing import List, Dict, Optional, Tuple

import pandas as pd

from risk_backtesting.backtester import Backtester
from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.config.simulation_config import SimulationConfig
from risk_backtesting.event_stream import EventStream
from risk_backtesting.loaders.load_snapshot import SnapshotLoader
from risk_backtesting.loaders.load_trades import load_trades, TradesLoader
from risk_backtesting.loaders.dataserver import DataServer
from risk_backtesting.matching_engine import AbstractMatchingEngine
from risk_backtesting.risk_manager import AbstractRiskManager, create_risk_manager
from risk_backtesting.simulator.simulation_plan import SimulationPlan
from risk_backtesting.strategy import AbstractStrategy, create_strategy


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
            simulation_configs: Dict[str, SimulationConfig],
            snapshot_loader: SnapshotLoader,
            event_stream: EventStream,
            trades_loader: TradesLoader,
            dataserver: DataServer,
    ) -> List[SimulationPlan]:
        plans: List[SimulationPlan] = []

        full_snapshot: Optional[pd.DataFrame] = None
        trades: Optional[pd.DataFrame] = None
        symbol_currencies: Optional[Dict] = None

        # if there are not instruments set in the config then load snapshot from history
        if config.load_snapshot_from_history:
            full_snapshot = snapshot_loader.get_liquidity_profile_snapshot(
                datasource_label=config.shard,
                start_date=config.start_date,
                end_date=config.end_date,
                instruments=config.instruments,
            )

        if config.load_trades_to_filter_snapshot:
            trades: pd.DataFrame = load_trades(
                loader=trades_loader,
                datasource_label=config.shard,
                instrument=config.instruments,
                account=[]
                if config.load_target_accounts_from_snapshot
                else config.target_accounts_list,
                start_date=config.start_date,
                end_date=config.end_date,
            )

        for simulation_config in simulation_configs.values():
            if (
                    simulation_config.strategy_parameters.get("max_pos_qty_level", -1)
                    == "currency"
            ):
                symbol_currencies = (
                    dataserver.get_order_book_details(
                        shard=config.shard, instruments=config.instruments
                    )
                    .set_index("order_book_id")["currency"]
                    .to_dict()
                )

            if simulation_config.load_any_from_snapshot:
                if simulation_config.load_position_limits_from_snapshot:
                    simulation_config.strategy_parameters[
                        "max_pos_qty"
                    ] = simulation_config.strategy_parameters.get("max_pos_qty", 0)

                strategy: AbstractStrategy = create_strategy(
                    simulation_config.strategy_parameters,
                    simulation_config.exit_parameters,
                    symbol_currencies=symbol_currencies,
                )

                snapshot: pd.DataFrame = full_snapshot.copy() if simulation_config.load_target_accounts_from_snapshot else simulation_config.target_accounts.copy()

                position_limit_snapshot: pd.DataFrame = Simulations.filter_snapshot_for_position_limit(
                    config.lmax_account, config.instruments, full_snapshot
                ) if simulation_config.load_position_limits_from_snapshot else None

                if simulation_config.filter_snapshot_for_strategy:
                    snapshot = strategy.filter_snapshot(
                        snapshot,
                        simulation_config.relative_comparison_accounts_type,
                        simulation_config.relative_comparison_accounts,
                    )

                # todo: simplify this
                instruments: List[
                    int
                ] = simulation_config.instruments if not simulation_config.load_instruments_from_snapshot else (
                    snapshot
                    if simulation_config.load_target_accounts_from_snapshot
                    else (
                        full_snapshot
                        if not simulation_config.filter_snapshot_for_strategy
                        else strategy.filter_snapshot(
                            full_snapshot,
                            simulation_config.relative_comparison_accounts_type,
                            simulation_config.relative_comparison_accounts,
                        )
                    )
                ).instrument_id.unique().tolist()

                if (
                        simulation_config.filter_snapshot_for_traded_account_instrument_pairs
                ):
                    (
                        snapshot,
                        instruments,
                    ) = Simulations.filter_snapshot_for_traded_account_instrument_pairs(
                        snapshot, trades
                    )

                plans.extend(
                    Simulations.split_simulation_config(
                        config,
                        event_stream,
                        instruments,
                        matching_engine,
                        simulation_config,
                        snapshot,
                        strategy,
                        position_limit_snapshot,
                    )
                )
            else:

                strategy: AbstractStrategy = create_strategy(
                    simulation_config.strategy_parameters,
                    simulation_config.exit_parameters,
                    symbol_currencies=symbol_currencies,
                )

                snapshot: pd.DataFrame = strategy.filter_snapshot(
                    simulation_config.target_accounts,
                    simulation_config.relative_comparison_accounts_type,
                    simulation_config.relative_comparison_accounts,
                ) if simulation_config.filter_snapshot_for_strategy else simulation_config.target_accounts

                instruments: List[int] = simulation_config.instruments

                if (
                        simulation_config.filter_snapshot_for_traded_account_instrument_pairs
                ):
                    (
                        snapshot,
                        instruments,
                    ) = Simulations.filter_snapshot_for_traded_account_instrument_pairs(
                        snapshot, trades
                    )

                plans.extend(
                    Simulations.split_simulation_config(
                        config,
                        event_stream,
                        instruments,
                        matching_engine,
                        simulation_config,
                        snapshot,
                        strategy,
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
            snapshot: pd.DataFrame,
            strategy: AbstractStrategy,
            position_limit_snapshot: pd.DataFrame = None,
    ) -> List[SimulationPlan]:
        logger = logging.getLogger("SplitSimulations")
        event_filter_strings: List[str] = []
        plans_delta: List[SimulationPlan] = []

        if simulation_config.split_by_instrument:
            for instrument in instruments:
                instrument_strategy = deepcopy(strategy)
                instrument_strategy.filter(instrument=instrument)

                instrument_snapshot: pd.DataFrame = Simulations.filter_snapshot_by_instruments(
                    snapshot, [instrument]
                )

                if simulation_config.load_position_limits_from_snapshot:
                    simulation_config.strategy_parameters[
                        "max_pos_qty"
                    ] = instrument_strategy.update_max_pos_qty(
                        Simulations.get_position_limit_from_snapshot(
                            [instrument], position_limit_snapshot
                        ),
                        instrument,
                    )

                if simulation_config.relative_simulation:

                    event_filter_strings = Simulations.create_event_filter_strings(
                        config.lmax_account,
                        instrument_snapshot,
                        simulation_config.relative_simulation_direction,
                        simulation_config.relative_comparison_accounts,
                        simulation_config.relative_comparison_accounts_type,
                    )

                if event_filter_strings:
                    for i, event_filter_string in enumerate(event_filter_strings):
                        logger.info(
                            f"Split Simulations {config.instruments} {i}/{len(event_filter_strings)}, "
                            f"using filter string {event_filter_string}"
                        )
                        plans_delta.append(
                            Simulations.build_simulation_plan(
                                config,
                                matching_engine,
                                event_stream,
                                simulation_config,
                                [instrument],
                                instrument_snapshot,
                                instrument_strategy,
                                event_filter_string,
                            )
                        )
                else:
                    plans_delta.append(
                        Simulations.build_simulation_plan(
                            config,
                            matching_engine,
                            event_stream,
                            simulation_config,
                            [instrument],
                            instrument_snapshot,
                            instrument_strategy,
                            simulation_config.event_filter_string,
                        )
                    )
        else:
            snapshot: pd.DataFrame = Simulations.filter_snapshot_by_instruments(
                snapshot, instruments
            )

            if simulation_config.load_position_limits_from_snapshot:
                simulation_config.strategy_parameters[
                    "max_pos_qty"
                ] = strategy.update_max_pos_qty(
                    Simulations.get_position_limit_from_snapshot(
                        instruments, position_limit_snapshot
                    )
                )

            if simulation_config.relative_simulation:
                event_filter_strings = Simulations.create_event_filter_strings(
                    config.lmax_account,
                    snapshot,
                    simulation_config.relative_simulation_direction,
                    simulation_config.relative_comparison_accounts,
                    simulation_config.relative_comparison_accounts_type,
                )

            if event_filter_strings:
                for event_filter_string in event_filter_strings:
                    plans_delta.append(
                        Simulations.build_simulation_plan(
                            config,
                            matching_engine,
                            event_stream,
                            simulation_config,
                            instruments,
                            snapshot,
                            strategy,
                            event_filter_string,
                        )
                    )
            else:
                plans_delta.append(
                    Simulations.build_simulation_plan(
                        config,
                        matching_engine,
                        event_stream,
                        simulation_config,
                        instruments,
                        snapshot,
                        strategy,
                        simulation_config.event_filter_string,
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
            accounts: pd.DataFrame,
            strategy: AbstractStrategy,
            event_filter_string: str,
    ) -> SimulationPlan:
        simulation_config = deepcopy(config_template)
        simulation_config.instruments = instruments

        if "instrument_id" in accounts.columns:
            simulation_config.target_accounts = accounts[
                accounts.instrument_id.isin(simulation_config.instruments)
            ]
        else:
            simulation_config.target_accounts = accounts

        risk_manager: AbstractRiskManager = create_risk_manager(
            simulation_config.risk_parameters
        )
        return SimulationPlan(
            name=simulation_config.name,
            uid=simulation_config.uid,
            version=simulation_config.version,
            shard=simulation_config.shard,
            start_date=simulation_config.start_date,
            end_date=simulation_config.end_date,
            target_accounts=simulation_config.target_accounts,
            load_snapshot=simulation_config.load_any_from_snapshot,
            load_trades_iteratively_by_session=config.load_trades_iteratively_by_session,
            load_target_accounts_from_snapshot=simulation_config.load_target_accounts_from_snapshot,
            load_instruments_from_snapshot=simulation_config.load_instruments_from_snapshot,
            load_position_limits_from_snapshot=simulation_config.load_position_limits_from_snapshot,
            load_booking_risk_from_snapshot=simulation_config.load_booking_risk_from_snapshot,
            load_internalisation_risk_from_snapshot=simulation_config.load_internalisation_risk_from_snapshot,
            load_booking_risk_from_target_accounts=simulation_config.load_booking_risk_from_target_accounts,
            load_internalisation_risk_from_target_accounts=simulation_config.load_internalisation_risk_from_target_accounts,
            lmax_account=config.lmax_account,
            load_starting_positions=simulation_config.load_starting_positions,
            load_client_starting_positions=simulation_config.load_client_starting_positions,
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
                netting_engine={
                    "client": config.netting_engine,
                    "lmax": config.netting_engine,
                },
                matching_method=config.matching_method,
                event_stream=event_stream,
                matching_engine=matching_engine,
                process_client_portfolio=config.process_client_portfolio,
                process_lmax_portfolio=config.process_lmax_portfolio,
                store_client_md_snapshot=config.store_client_md_snapshot,
                store_client_trade_snapshot=config.store_client_trade_snapshot,
                store_client_eod_snapshot=config.store_client_eod_snapshot,
                store_lmax_md_snapshot=config.store_lmax_md_snapshot,
                store_lmax_trade_snapshot=config.store_lmax_trade_snapshot,
                store_lmax_eod_snapshot=config.store_lmax_eod_snapshot,
            ),
        )
