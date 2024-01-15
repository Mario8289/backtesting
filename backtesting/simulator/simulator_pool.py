import datetime as dt
import logging
import multiprocessing as mp
from copy import deepcopy
from typing import List, Dict, Union, Optional

import pandas as pd

# from ..config.config import Config  caused a circular import error
from risk_backtesting.config.simulation_config import SimulationConfig
from risk_backtesting.simulator.simulation_result import (
    SimulationResult,
    success,
    failure,
)
from .simulation_batch_result import SimulationBatchResult
from .simulation_plan import SimulationPlan
from .simulations import Simulations
from ..config.backtesting_config import BackTestingConfig
from ..event_stream import EventStream
from ..loaders.load_price_slippage_model import PriceSlippageLoader
from ..loaders.load_profiling import ProfilingLoader
from ..loaders.load_snapshot import SnapshotLoader
from ..loaders.load_starting_positions import (
    load_open_positions,
    load_open_positions_with_risk,
    StartingPositionsLoader,
)
from ..loaders.load_tob import load_tob, TobLoader
from ..loaders.load_trades import load_trades, TradesLoader
from ..loaders.dataserver import DataServerLoader
from ..matching_engine import AbstractMatchingEngine
from ..risk_backtesting_result import BackTestingResults
from ..save_simulations import save_simulation
from ..simulator.simulator import Simulator
from ..writers import Writer


class SimulatorPool(Simulator):
    def __init__(
            self,
            dataserver: DataServerLoader,
            trades_loader: TradesLoader,
            event_stream: EventStream,
            tob_loader: TobLoader = None,
            starting_positions_loader: StartingPositionsLoader = None,
            price_slippage_loader: PriceSlippageLoader = None,
            snapshot_loader: SnapshotLoader = None,
            profiling_loader: ProfilingLoader = None,
    ):
        super().__init__(
            "simulator_pool",
            dataserver=dataserver,
            tob_loader=tob_loader,
            trades_loader=trades_loader,
            starting_positions_loader=starting_positions_loader,
            event_stream=event_stream,
            price_slippage_loader=price_slippage_loader,
            snapshot_loader=snapshot_loader,
            profiling_loader=profiling_loader,
        )
        self.migrations: pd.DataFrame = None

    def parallelise_simulations(
            self, plans: List[SimulationPlan], cores=1
    ) -> List[SimulationResult]:
        pool = mp.Pool(cores)
        results: List[SimulationResult] = pool.map(self.run_simulation, plans)
        pool.close()
        return results

    def run_simulation(self, plan: SimulationPlan) -> SimulationResult:
        logger: logging.Logger = logging.getLogger(
            f"SimulationPlan[{plan.name}/{plan.hash}]"
        )
        start_time = dt.datetime.now()
        snapshot: pd.DataFrame = pd.DataFrame()
        closing_prices: pd.DataFrame = pd.DataFrame()
        delta_trades: pd.DataFrame = pd.DataFrame()
        upnl_reversals: pd.DataFrame = pd.DataFrame()

        # maybe just add the contract_unit_of_measure to the trades parquet?
        order_book_details: pd.DataFrame = self.dataserver.get_order_book_details(
            plan.shard, plan.instruments,
        )

        try:
            logger.info(
                f"attempt SimulationPlan[{plan.name}/{plan.hash}], instruments {plan.instruments}"
            )

            if not plan.load_trades_iteratively_by_session:
                trades: pd.DataFrame = load_trades(
                    loader=self.trades_loader,
                    datasource_label=plan.shard,
                    instrument=plan.instruments,
                    account=plan.target_accounts_list,
                    start_date=plan.start_date,
                    end_date=plan.end_date,
                    order_book_details=order_book_details,
                )

                # logger.info(f"[{plan.name}/{plan.hash}], successfully loaded {trades.shape[0]} trades for instrument {plan.instruments}, date_range {plan.start_date}-{plan.end_date}")

            for day in pd.date_range(plan.start_date, plan.end_date):
                day_date = day.date()
                accounts: List[int] = plan.target_accounts_list

                if plan.load_trades_iteratively_by_session:
                    trades: pd.DataFrame = load_trades(
                        loader=self.trades_loader,
                        datasource_label=plan.shard,
                        instrument=plan.instruments,
                        account=accounts,
                        start_date=day_date,
                        end_date=day_date,
                        order_book_details=order_book_details,
                    )

                    if not delta_trades.empty:

                        delta_trades_for_trade_date = delta_trades[
                            (delta_trades.account_id.isin(accounts))
                            & (delta_trades.order_book_id.isin(plan.instruments))
                            & (delta_trades.trade_date == day)
                            ]
                        if not delta_trades_for_trade_date.empty:

                            trades = (
                                pd.concat([delta_trades_for_trade_date, trades])
                                .sort_index()
                                .drop_duplicates(trades.columns, keep="first")
                            )

                            delta_trades = pd.concat(
                                [delta_trades, delta_trades_for_trade_date]
                            ).drop_duplicates(keep=False)

                    # logger.info(f"[{plan.name}/{plan.hash}], successfully loaded {trades.shape[0]}, trades for instrument {plan.instruments}, date_range {day_date}-{day_date}")

                if plan.level == "mark_to_market":
                    if plan.calculate_cumulative_daily_pnl:
                        original_tob = load_tob(
                            self.tob_loader,
                            datasource_label=plan.shard,
                            instrument=plan.instruments,
                            start_date=day - dt.timedelta(days=1),
                            end_date=day,
                            tier=[1],
                        )

                        filtered_tob = self.filter_tob_for_trading_session(
                            original_tob, day, plan.instruments
                        )

                        tob_for_plan = self.event_stream.sample(filtered_tob, day)

                    else:
                        tob_for_plan = self.filter_tob_for_trading_session(
                            self.tob, day, plan.instruments
                        )
                else:
                    tob_for_plan = pd.DataFrame()

                # if we're loading from history and running a rolling simulation, we should get the account and trades information
                #    again at this point. we can't really load everything into memory because over a large enough time span python
                #    doesn't play nice with passing large amounts of data around between processes. At least not in our current impl
                if plan.load_snapshot:
                    snapshot: pd.DataFrame = self.snapshot_loader.get_liquidity_profile_snapshot(
                        datasource_label=plan.shard,
                        start_date=day_date,
                        end_date=day_date,
                        instruments=plan.instruments,
                    )

                if (
                        plan.load_target_accounts_from_snapshot
                        and plan.calculate_cumulative_daily_pnl
                ):
                    accounts = (
                        plan.filter_accounts(snapshot).account_id.unique().tolist()
                    )

                    trades = load_trades(
                        loader=self.trades_loader,
                        datasource_label=plan.shard,
                        instrument=plan.instruments,
                        account=accounts,
                        start_date=day_date,
                        end_date=day_date,
                        order_book_details=order_book_details,
                    )

                if plan.load_starting_positions and plan.start_date == day_date:

                    (
                        plan.backtester.lmax_portfolio.positions,
                        plan.backtester.lmax_portfolio.total_net_position,
                    ) = load_open_positions_with_risk(
                        loader=self.starting_positions_loader,
                        datasource_label=plan.shard,
                        start_date=plan.start_date,
                        end_date=plan.start_date,
                        account=self.get_lmax_accounts_for_loading_starting_positions(
                            plan
                        ),
                        invert_position=self.invert_starting_position(
                            plan.strategy.get_name()
                        ),
                        instrument=plan.instruments,
                        netting_engine=plan.backtester.lmax_portfolio.netting_engine,
                        load_booking_risk=plan.load_booking_risk_from_snapshot,
                        load_booking_risk_from_target_accounts=plan.load_booking_risk_from_target_accounts,
                        load_internalisation_risk=plan.load_internalisation_risk_from_snapshot,
                        snapshot=snapshot,
                        target_accounts=plan.target_accounts,
                    )

                    upnl_reversals = self.dataserver.get_upnl_reversal(
                        plan.shard,
                        day,
                        day,
                        plan.instruments,
                        self.get_lmax_accounts_for_loading_starting_positions(plan),
                    )

                if plan.load_client_starting_positions and plan.start_date == day_date:
                    (
                        plan.backtester.client_portfolio.positions,
                        plan.backtester.client_portfolio.total_net_position,
                    ) = load_open_positions(
                        loader=self.starting_positions_loader,
                        datasource_label=plan.shard,
                        start_date=plan.start_date,
                        end_date=plan.start_date,
                        account=plan.target_accounts_list,
                        instrument=plan.instruments,
                        netting_engine=plan.backtester.client_portfolio.netting_engine,
                    )

                trades_for_plan = Simulator.filter_trades_for_simulation(
                    day,
                    accounts,
                    plan.instruments,
                    plan.event_filter_string,
                    trades,
                    plan.target_accounts,
                )

                if plan.load_trades_iteratively_by_session:

                    pushed_trades = Simulator.filter_trades_for_pushed_trades(
                        day,
                        accounts,
                        plan.instruments,
                        plan.event_filter_string,
                        trades,
                        plan.target_accounts,
                    )

                    if not pushed_trades.empty:
                        delta_trades = pd.concat(
                            [delta_trades, pushed_trades]
                        ).drop_duplicates(trades.columns)

                    # print(f"{day} PUSHED TRADES, shape {pushed_trades.shape}, DELTA TRADES shape {delta_trades.shape}")

                if "model" in plan.backtester.matching_engine.__class__.__slots__:
                    plan.backtester.matching_engine.load_model(
                        loader=self.price_slippage_loader,
                        datasource_label=plan.shard,
                        date=day_date,
                    )
                if "model_type" in plan.strategy.__class__.__slots__:
                    if plan.strategy.retrain_model(day_date):
                        start_date = day - dt.timedelta(
                            days=plan.strategy.train_period + 1
                        )
                        end_date = day - dt.timedelta(days=1)
                        profiles_for_plan = self.profiling_loader.load_closed_positions(
                            plan.shard, start_date, end_date, plan.target_accounts
                        )
                        plan.strategy.model.train(profiles_for_plan)

                        logger.info(
                            f"Model retained on: {day.date()} for dates: ({start_date.date()}, {end_date.date()}), profiles found: {len(profiles_for_plan)}/{len(plan.target_accounts)}"
                        )
                self.migrations = plan.backtester.strategy.get_account_migrations(
                    day=day_date,
                    target_accounts=plan.target_accounts,
                    shard=plan.shard,
                    instruments=plan.instruments,
                    tob_loader=self.tob_loader,
                    dataserver=self.dataserver,
                    load_booking_risk_from_snapshot=plan.load_booking_risk_from_snapshot,
                    load_booking_risk_from_target_accounts=plan.load_booking_risk_from_target_accounts,
                    load_internalisation_risk_from_snapshot=plan.load_internalisation_risk_from_snapshot,
                    load_internalisation_risk_from_target_accounts=False,
                    snapshot=snapshot,
                    order_book_details=order_book_details,
                )
                account_migrations = self.migrations

                plan.backtester.strategy.update(
                    shard=plan.shard,
                    date=day,
                    dataserver=self.dataserver,
                    instruments=plan.instruments,
                )

                if self.event_stream.include_eod_snapshot:
                    closing_prices = self.dataserver.get_closing_prices(
                        shard=plan.shard,
                        start_date=day,
                        end_date=day,
                        instruments=plan.instruments,
                    )

                plan.backtester.run_day_simulation(
                    date=day_date,
                    trades=trades_for_plan,
                    venue=1,
                    tob=tob_for_plan,
                    closing_prices=closing_prices,
                    account_migrations=account_migrations,
                )

                # logger.info(
                #     f"[{plan.name}/{plan.hash}], run_day_simulation complete for date: {day_date}, instruments {plan.instruments}."
                # )

            if len(plan.backtester.statistics.events) != 0:
                df: pd.DataFrame = plan.backtester.statistics.events_to_df(
                    event_features=plan.output.event_features,
                    upnl_reversals=upnl_reversals,
                )
                if plan.output.resample_rule is not None:
                    df = plan.backtester.statistics.aggregate_returns(
                        df,
                        plan.output.resample_rule,
                        event_features=plan.output.event_features,
                        metrics=plan.output.metrics,
                    )

                plan.append_strategy_params(df)

                result: SimulationResult = success(plan, True, df, start_time)
                logger.info(f"{result}")
                return result

            result: SimulationResult = success(plan, False, pd.DataFrame(), start_time)
            logger.info(f"{result}")
            return result

        except Exception as be:
            result: SimulationResult = failure(plan, start_time, be)
            logger.error(f"{result}", exc_info=result.payload)
            return result

    def get_lmax_accounts_for_loading_starting_positions(self, plan):
        return (
            plan.target_accounts_list
            if "bbooking" in plan.strategy.get_name()
            else [plan.lmax_account]
        )

    def invert_starting_position(self, strategy_name):
        return True if "bbooking" in strategy_name else False

    def create_simulation_plans(
            self,
            config: BackTestingConfig,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[str, SimulationConfig],
    ) -> List[SimulationPlan]:
        return Simulations.build_simulation_plans(
            config=config,
            matching_engine=matching_engine,
            simulation_configs=simulation_configs,
            snapshot_loader=self.snapshot_loader,
            event_stream=self.event_stream,
            trades_loader=self.trades_loader,
            dataserver=self.dataserver,
        )

    def start_simulator(
            self,
            config: BackTestingConfig,
            results_cache: pd.DataFrame,
            writer: Writer,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[str, SimulationConfig],
            results: BackTestingResults,
    ):
        if config.calculate_cumulative_daily_pnl:
            self.start_simulator_rolling(
                config,
                results_cache,
                writer,
                matching_engine,
                simulation_configs,
                results,
            )
        else:
            self.start_simulator_not_rolling(
                config,
                results_cache,
                writer,
                matching_engine,
                simulation_configs,
                results,
            )

    def start_simulator_rolling(
            self,
            config: BackTestingConfig,
            results_cache: pd.DataFrame,
            writer: Writer,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[str, SimulationConfig],
            results: BackTestingResults,
    ):
        logger: logging.Logger = logging.getLogger(
            "SimulationPool: start_simulator_rolling"
        )
        logger.info("lets build those plans")
        all_plans: List[SimulationPlan] = self.create_simulation_plans(
            config, matching_engine, simulation_configs
        )
        logger.info("plans all build")

        # filter out plans that have already been generated on previous execution
        if results_cache is not None:
            plans = self.filter_simulation_plan(all_plans, results_cache)
            logger.info(
                f"plans: (date) {config.start_date}-{config.end_date}, (total) {len(all_plans)}, (to execute) {len(plans)} (from cache) {len(all_plans) - len(plans)}"
            )
            if len(plans) == 0:
                return results_cache[
                    results_cache.hash.isin(
                        [v.hash for (k, v) in simulation_configs.items()]
                    )
                ]
        else:
            plans = all_plans

        batched_simulations: List[List[SimulationPlan]] = self.split_simulations(
            plans, config.num_batches
        )

        for index, batch in enumerate(batched_simulations):
            results_batch: SimulationBatchResult = self.run_simulation_for_batch(
                batch, config, writer
            )
            results.accumulate(results_batch)

        if results_cache is not None:
            results.accumulate_df(
                results_cache[
                    results_cache.hash.isin(
                        [v.hash for (k, v) in simulation_configs.items()]
                    )
                ],
                sort=True,
            )

    def start_simulator_not_rolling(
            self,
            config: BackTestingConfig,
            results_cache: pd.DataFrame,
            writer: Writer,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[str, SimulationConfig],
            results: BackTestingResults,
    ):
        logger: logging.Logger = logging.getLogger(
            "SimulationPool: start_simulator_not_rolling"
        )

        for day in pd.date_range(config.start_date, config.end_date, freq="1D"):
            if day.weekday() < 5:
                # create a deep copy of the config and update date_range
                config_iter = deepcopy(config)

                config_iter.start_date = day.date()
                config_iter.end_date = day.date()
                for label, simulation_config in simulation_configs.items():
                    setattr(simulation_config, "start_date", config_iter.start_date)
                    setattr(simulation_config, "end_date", config_iter.end_date)

                all_plans: List[SimulationPlan] = self.create_simulation_plans(
                    config_iter, matching_engine, simulation_configs
                )

                # filter out plans that have already been generated on previous execution
                if results_cache is not None:
                    plans = self.filter_simulation_plan(all_plans, results_cache)
                    logger.info(
                        f"plans: (date) {day.date()}, (total) {len(all_plans)}, (to execute) {len(plans)} (from cache) {len(all_plans) - len(plans)}"
                    )
                else:
                    plans = all_plans
                    logger.info(
                        f"plans: (date) {day.date()}, (total) {len(all_plans)}, (to execute) {len(plans)} (from cache) 0"
                    )

                # if either all plans have been filtered out due to simulation options or all plans loaded from cache then continue to next day
                if len(plans) == 0:
                    continue

                instruments: List[int] = list(
                    set(
                        [
                            instrument
                            for instruments in [p.instruments for p in plans]
                            for instrument in instruments
                        ]
                    )
                )

                if config_iter.level == "mark_to_market":
                    start_datetime: dt.datetime = dt.datetime.combine(
                        config_iter.start_date, dt.datetime.min.time()
                    )
                    end_datetime: dt.datetime = dt.datetime.combine(
                        config_iter.end_date, dt.datetime.min.time()
                    )
                    start_time = dt.datetime.now()
                    tob: pd.DataFrame = load_tob(
                        self.tob_loader,
                        datasource_label=config_iter.shard,
                        instrument=instruments,
                        start_date=start_datetime - dt.timedelta(days=1),
                        end_date=end_datetime,
                        tier=[1],
                    )
                    logger.info(
                        f"loaded tob: (date) {day.date()}, (duration) {dt.datetime.now() - start_time}"
                    )

                    filtered_tob = self.filter_tob_for_trading_session(
                        tob, day, instruments
                    )

                    self.tob = self.event_stream.sample(filtered_tob, day)

                batched_plans: List[List[SimulationPlan]] = self.split_simulations(
                    plans, config_iter.num_batches
                )

                for index, batch in enumerate(batched_plans):
                    # self.logger.info(f"  method: start_simulator_not_rolling, batch {batch_i+1}/{len(plans)}")
                    if config_iter.output.mode == "w" and index != 0:
                        config_iter.output.mode = "a"
                    results_batch: SimulationBatchResult = self.run_simulation_for_batch(
                        batch, config_iter, writer
                    )
                    results.accumulate(results_batch)

                if results_cache is not None:
                    if day in results_cache.index:
                        results_cache_for_day = results_cache.loc[day]
                        results.accumulate_df(
                            results_cache_for_day[
                                results_cache_for_day.hash.isin(
                                    [p.hash for p in all_plans]
                                )
                            ],
                            sort=True,
                        )

    def run_simulation_for_batch(
            self, batch: List[SimulationPlan], config: BackTestingConfig, writer: Writer
    ) -> SimulationBatchResult:
        logger = logging.getLogger("SimulationPool")

        results_batch: List[SimulationResult] = []
        retryable_error: Optional[Union[KeyError, OSError]] = None
        errors: List[SimulationResult] = []

        for i in range(self.execution_attempt):
            retryable_error = None
            logger.info(
                f"run_simulation_for_batch: (plans) {len(batch)}, (attempt) {i+1}/{self.execution_attempt}"
            )

            try:
                results_batch = self.parallelise_simulations(
                    batch, cores=config.num_cores
                )
                break
            except (KeyError, OSError) as e:  # noqa
                logger.error(f"run_simulation_for_batch: (OS error) {e}")
                retryable_error = e
            except Exception as e:
                logger.error(f"run_simulation_for_batch: (Unknown Exception) {e}")
                raise e

        if retryable_error is not None:
            raise retryable_error

        results_dfs: List[pd.DataFrame] = []

        for result in results_batch:
            if result.is_success():
                results_dfs.append(result.payload)
            else:
                errors.append(result)
                logger.error(f"{result}", exc_info=result.payload)

        results_df: pd.DataFrame = pd.concat(results_dfs)

        # save output
        if config.output.save and not results_df.empty:
            save_simulation(
                writer=writer,
                df=results_df,
                uid=config.uid,
                version=config.version,
                mode=config.output.mode,
                directory=config.output.directory,
                datasource_label=config.shard,
                store_index=config.output.store_index,
                split_results_by=config.output.by,
                split_results_freq=config.output.freq,
                bucket=config.output.bucket,
                file=config.output.file,
            )
        return SimulationBatchResult(results_df, errors)
