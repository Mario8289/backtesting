import datetime as dt
import logging
import multiprocessing as mp
from copy import deepcopy
from typing import List, Dict, Union, Optional, AnyStr
from datetime import timedelta

import pandas as pd

# from ..config.config import Config  caused a circular import error
from backtesting.config.simulation_config import SimulationConfig
from backtesting.simulator.simulation_result import (
    SimulationResult,
    success,
    failure,
)
from .simulation_batch_result import SimulationBatchResult
from .simulation_plan import SimulationPlan
from .simulations import Simulations
from ..config.backtesting_config import BackTestingConfig
from ..event_stream import EventStream
from ..subscriptions.subscription import Subscription

from ..matching_engine import AbstractMatchingEngine
from ..backtesting_result import BackTestingResults
from ..save_simulations import save_simulation
from ..simulator.simulator import Simulator
from ..writers import Writer
from ..subscriptions_cache import SubscriptionsCache


class SimulatorPool(Simulator):
    def __init__(
            self,
            event_stream: EventStream
    ):
        super().__init__(
            "simulator_pool",
            event_stream=event_stream,
        )

    @staticmethod
    def get_missing_date_ranges(missing_dates):
        missing_date_ranges = []
        missing_date_range = [missing_dates[0]]
        for _idx, _date in enumerate(missing_dates[1:]):
            next_day = missing_date_range[-1] + timedelta(days=1)
            if next_day == _date:
                missing_date_range.append(_date)
            else:
                missing_date_ranges.append(missing_date_range)
                missing_date_range = [_date]
        else:
            missing_date_ranges.append(missing_date_range)
        return missing_date_ranges

    def parallelise_simulations(
            self, plans: List[SimulationPlan], cores=1
    ) -> List[SimulationResult]:
        pool = mp.Pool(cores)
        results: List[SimulationResult] = pool.map(self.run_simulation, plans)
        pool.close()
        return results

    def load_subscription_events(self, plan, sub_name, sub_obj):

        interval = '1d'
        save = False

        if plan.subscriptions_cache.enable_cache and plan.subscriptions_cache.mode != 'w':
            subscription_events, missing_dates = plan.subscriptions_cache.get(
                subscription=sub_name,
                start_date=str(plan.start_date),
                end_date=str(plan.end_date),
                instruments=plan.instruments,
                interval=interval
            )

            if missing_dates:
                missing_date_ranges = self.get_missing_date_ranges(missing_dates)

                for date_range in missing_date_ranges:
                    start_date = date_range[0].strftime('%Y-%m-%d')
                    end_date = date_range[-1].strftime('%Y-%m-%d')

                    _subscription_events = sub_obj.get(
                        start_date=start_date,
                        end_date=end_date,
                        instruments=plan.instruments,
                        interval=interval
                    )

                    plan.subscriptions_cache.save(
                        subscription=sub_name,
                        subscription_events=_subscription_events,
                        interval=interval,
                    )

                    subscription_events = pd.concat([subscription_events, _subscription_events])

        else:
            subscription_events = sub_obj.get(
                start_date=str(plan.start_date),
                end_date=str(plan.end_date),
                instruments=plan.instruments,
                interval=interval
            )

            if plan.subscriptions_cache.enable_cache:
                plan.subscriptions_cache.save(
                    subscription=sub_name,
                    subscription_events=subscription_events,
                    interval=interval
                )

        return subscription_events

    def run_simulation(self, plan: SimulationPlan) -> SimulationResult:
        logger: logging.Logger = logging.getLogger(
            f"SimulationPlan[{plan.name}/{plan.hash}]"
        )
        start_time = dt.datetime.now()
        upnl_reversals: pd.DataFrame = pd.DataFrame()

        try:
            logger.info(
                f"attempt SimulationPlan[{plan.name}/{plan.hash}], instruments {plan.instruments}"
            )

            # can these not be shared between each simulation
            _cached_subscription_events = {}
            for sub_name, sub_obj in plan.backtester.subscriptions.items():
                if not sub_obj.load_by_session:
                    subscription_events = self.load_subscription_events(
                        plan=plan,
                        sub_name=sub_name,
                        sub_obj=sub_obj
                    )

                    _cached_subscription_events[sub_name] = subscription_events

            for day in pd.date_range(plan.start_date, plan.end_date):
                subscriptions: List[pd.DataFrame] = []

                day_date = str(day.date())

                for sub_name, sub_obj in plan.backtester.subscriptions.items():

                    if sub_obj.load_by_session:
                        subscription_events_for_day = self.load_subscription_events(
                            plan=plan,
                            sub_name=sub_name,
                            sub_obj=sub_obj
                        )
                        subscription_events_for_day = self.event_stream.sample(subscription_events_for_day, day)
                        subscriptions.append(subscription_events_for_day)

                        logger.info(f"[{plan.name}/{plan.hash}], successfully loaded {subscription_events_for_day.shape[0]}"
                                    f" events for subscription {sub_name}, "
                                    f"date_range {day_date}-{day_date}")
                    else:
                        subscription_events = _cached_subscription_events[sub_name]
                        subscription_events_for_day = subscription_events[subscription_events.index.date == day.date()]
                        subscription_events_for_day = self.event_stream.sample(subscription_events_for_day, day)
                        subscriptions.append(subscription_events_for_day)

                if plan.load_starting_positions and plan.start_date == day_date:
                    # TODO: not implemented loading starting positions
                    pass
                    # (
                    #     plan.backtester.portfolio.positions,
                    #     plan.backtester.portfolio.total_net_position,
                    # ) = load_starting_positions
                if "model" in plan.backtester.matching_engine.__class__.__slots__:
                    # TODO: not implemented loading model
                    pass
                    # plan.backtester.matching_engine.load_model
                if "model_type" in plan.strategy.__class__.__slots__:
                    if plan.strategy.retrain_model(day_date):
                        pass
                        # data = pd.DataFrame()
                        # plan.strategy.model.train(data)

                plan.backtester.strategy.update(
                    **plan.__dict__
                )

                plan.backtester.run_day_simulation(
                    date=day_date,
                    subscriptions=subscriptions
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

                df = plan.append_strategy_params(df)

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
            simulation_configs: Dict[AnyStr, SimulationConfig],
            subscriptions: Dict[AnyStr, Subscription],
            subscriptions_cache: SubscriptionsCache
    ) -> List[SimulationPlan]:
        return Simulations.build_simulation_plans(
            config=config,
            matching_engine=matching_engine,
            simulation_configs=simulation_configs,
            subscriptions=subscriptions,
            subscriptions_cache=subscriptions_cache,
            event_stream=self.event_stream,
        )

    def start_simulator(
            self,
            config: BackTestingConfig,
            results_cache: pd.DataFrame,
            subscriptions_cache: SubscriptionsCache,
            writer: Writer,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[AnyStr, SimulationConfig],
            subscriptions: Dict[AnyStr, Subscription],
            results: BackTestingResults,
    ):
        if config.calculate_cumulative_daily_pnl:
            self.start_simulator_rolling(
                config=config,
                results_cache=results_cache,
                subscriptions_cache=subscriptions_cache,
                writer=writer,
                matching_engine=matching_engine,
                simulation_configs=simulation_configs,
                subscriptions=subscriptions,
                results=results,
            )
        else:
            self.start_simulator_not_rolling(
                config=config,
                results_cache=results_cache,
                subscriptions_cache=subscriptions_cache,
                writer=writer,
                matching_engine=matching_engine,
                simulation_configs=simulation_configs,
                subscriptions=subscriptions,
                results=results,
            )

    def start_simulator_rolling(
            self,
            config: BackTestingConfig,
            results_cache: pd.DataFrame,
            subscriptions_cache: SubscriptionsCache,
            writer: Writer,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[str, SimulationConfig],
            subscriptions: List[Subscription],
            results: BackTestingResults,
    ):
        logger: logging.Logger = logging.getLogger(
            "SimulationPool: start_simulator_rolling"
        )
        logger.info("lets build those plans")
        all_plans: List[SimulationPlan] = self.create_simulation_plans(
            config=config,
            matching_engine=matching_engine,
            simulation_configs=simulation_configs,
            subscriptions=subscriptions,
            subscriptions_cache=subscriptions_cache
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
            subscriptions_cache: SubscriptionsCache,
            writer: Writer,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[str, SimulationConfig],
            subscriptions: List[Subscription],
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
                    config=config_iter,
                    matching_engine=matching_engine,
                    simulation_configs=simulation_configs,
                    subscriptions=subscriptions,
                    subscriptions_cache=subscriptions_cache
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

                if config_iter.level == "mark_to_market":
                    # TODO: not yet configured
                    pass

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
                store_index=config.output.store_index,
                split_results_by=config.output.by,
                split_results_freq=config.output.freq,
                file=config.output.file,
            )
        return SimulationBatchResult(results_df, errors)
