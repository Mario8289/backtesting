import logging
from typing import Optional, List

import pandas as pd
import s3fs

from lmax_analytics.dataloader.backtesting_dataloader import BacktestingLoader
from lmax_analytics.dataloader.parquet import Parquet
from risk_backtesting.loaders.dataserver import DataServerLoader
from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.event_stream import create_event_stream, EventStream
from risk_backtesting.filesystems import initialise_s3fs, initialise_boto3
from risk_backtesting.loaders.load_price_slippage_model import PriceSlippageS3Loader
from risk_backtesting.loaders.load_profiling import ProfilingLoader
from risk_backtesting.loaders.load_snapshot import (
    SnapshotLoader,
    SnapshotParquetLoader,
)
from risk_backtesting.loaders.load_starting_positions import StartingPositionsLoader
from risk_backtesting.loaders.load_tob import TobParquetLoader
from risk_backtesting.loaders.load_trades import TradesParquetLoader
from risk_backtesting.matching_engine import (
    create_matching_engine,
    AbstractMatchingEngine,
)
from risk_backtesting.risk_backtesting_result import BackTestingResults
from risk_backtesting.simulator.simulator import Simulator
from risk_backtesting.simulator.simulator_pool import SimulatorPool
from risk_backtesting.writers import create_writer, Writer


class SimulationRunner:
    def __init__(self):
        self.logger: logging.Logger = logging.getLogger("SimulationRunner")

    def run(self, config: BackTestingConfig, results: BackTestingResults):
        self.logger.debug("initialise s3fs")
        filesystem: s3fs.S3FileSystem = initialise_s3fs(config.auth, "input")
        self.logger.debug("initialise loaders")
        dataserver: DataServerLoader = DataServerLoader.initialise(
            config.auth, "dataserver"
        )
        self.logger.debug("initialise loaders")
        parquet_loader: Parquet = Parquet(config.bucket, filesystem)
        trades_loader: TradesParquetLoader = TradesParquetLoader(loader=parquet_loader)

        tob_loader: TobParquetLoader = TobParquetLoader(loader=parquet_loader)

        starting_positions_loader: StartingPositionsLoader = StartingPositionsLoader(
            loader=dataserver
        )
        price_slippage_loader: PriceSlippageS3Loader = PriceSlippageS3Loader(
            loader=filesystem
        )
        profiling_loader: ProfilingLoader = ProfilingLoader(loader=parquet_loader)
        snapshot_loader: SnapshotLoader = SnapshotParquetLoader(loader=parquet_loader)

        self.logger.debug("initialise event stream")
        event_stream: EventStream = create_event_stream(config.event_stream_params)

        self.logger.debug("initialise simulator")
        if "simulator_pool" != config.simulator_type:
            raise ValueError(
                f"Simulator {config.simulator_type} doesnt exist, please update config"
            )

        simulator: Simulator = SimulatorPool(
            dataserver=dataserver,
            tob_loader=tob_loader,
            trades_loader=trades_loader,
            starting_positions_loader=starting_positions_loader,
            price_slippage_loader=price_slippage_loader,
            snapshot_loader=snapshot_loader,
            event_stream=event_stream,
            profiling_loader=profiling_loader,
        )

        self.logger.debug("initialise writer and matching engine")
        writer: Writer = create_writer(
            config.output.filesystem_type, initialise_s3fs(config.auth, "output")
        )
        matching_engine: AbstractMatchingEngine = create_matching_engine(
            config.matching_engine_params, config.matching_method
        )

        self.logger.debug("run simulator")
        # todo cintezam: maybe pass in just config and not sim_cfg and instruments?
        simulator.start_simulator(
            config=config,
            results_cache=SimulationRunner._build_cache(config, config.instruments),
            writer=writer,
            matching_engine=matching_engine,
            simulation_configs=config.simulation_configs,
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
