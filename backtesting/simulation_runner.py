import logging
from typing import Optional, List, Dict, AnyStr

import pandas as pd

from backtesting.config.backtesting_config import BackTestingConfig
from backtesting.event_stream import create_event_stream, EventStream

from backtesting.matching_engine import (
    create_matching_engine,
    AbstractMatchingEngine,
)
from backtesting.backtesting_result import BackTestingResults
from backtesting.simulator.simulator import Simulator
from backtesting.simulator.simulator_pool import SimulatorPool
from backtesting.writers import create_writer, Writer
from backtesting.subscriptions_cache import create_subscriptions_cache, SubscriptionsCache
from backtesting.subscriptions.subscription import Subscription
from backtesting.subscriptions import create_subscription


class SimulationRunner:
    def __init__(self):
        self.logger: logging.Logger = logging.getLogger("SimulationRunner")

    def run(self, config: BackTestingConfig, results: BackTestingResults):
        active_subscriptions = [sub for c in config.simulation_configs.values() for sub in c.subscriptions]

        subscriptions: Dict[AnyStr, Subscription] = {
            k: create_subscription(k, v) for (k,v) in config.subscriptions.items() if k in active_subscriptions
        }

        #starting_positions_loader: StartingPositionsLoader = StartingPositionsLoader.build(config.starting_positions)

        self.logger.debug("initialise event stream")
        event_stream: EventStream = create_event_stream(config.event_stream_params)

        self.logger.debug("initialise simulator")
        if "simulator_pool" != config.simulator_type:
            raise ValueError(
                f"Simulator {config.simulator_type} doesnt exist, please update config"
            )

        simulator: Simulator = SimulatorPool(
            event_stream=event_stream
        )

        self.logger.debug("initialise writer and matching engine")
        writer: Writer = create_writer(
            config.output.datastore,
            config.output.datastore_parameters
        )
        subscriptions_cache: SubscriptionsCache = create_subscriptions_cache(
            config.subscriptions_cache['datastore'],
            config.subscriptions_cache['datastore_parameters'],
            config.subscriptions_cache['enable_cache'],
            config.subscriptions_cache['mode'],
        )
        matching_engine: AbstractMatchingEngine = create_matching_engine(
            config.matching_engine_params, config.matching_method
        )

        self.logger.debug("run simulator")
        # todo cintezam: maybe pass in just config and not sim_cfg and instruments?
        simulator.start_simulator(
            config=config,
            results_cache=SimulationRunner._build_cache(config, config.instruments),
            subscriptions_cache=subscriptions_cache,
            writer=writer,
            matching_engine=matching_engine,
            simulation_configs=config.simulation_configs,
            subscriptions=subscriptions,
            results=results,
        )

    @staticmethod
    def _build_cache(
            config: BackTestingConfig, instruments: List[int]
    ) -> Optional[pd.DataFrame]:
        if "a" == config.output.mode:
            boto3_fs = initialise_boto3(config.auth, "output")
            loader: BacktestingLoader = BacktestingLoader(boto3_fs=boto3_fs)

            results_cache: pd.DataFrame = loader.load_sims_s3(
                bucket=config.output.bucket,
                base_prefix=config.output.directory,
                shard=config.shard,
                uid=config.uid,
                version=config.version,
                start_date=config.start_date,
                end_date=config.end_date,
                instruments=instruments,
            )

            if results_cache is not None:
                results_cache = results_cache.set_index(
                    config.output.by if config.output.by is not None else "timestamp"
                )

            return results_cache
        return None
