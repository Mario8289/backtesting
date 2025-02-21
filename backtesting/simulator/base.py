from abc import ABCMeta, abstractmethod
from typing import List, Dict, AnyStr

import pandas as pd

from backtesting.config.backtesting_config import BackTestingConfig
from backtesting.config.simulation_config import SimulationConfig
from backtesting.matching_engine import AbstractMatchingEngine
from backtesting.backtesting_result import BackTestingResults
from backtesting.writers import Writer
from backtesting.subscriptions.subscription import Subscription


class AbstractSimulator(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def start_simulator(
            self,
            config: BackTestingConfig,
            results_cache: pd.DataFrame,
            writer: Writer,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[AnyStr, SimulationConfig],
            subscriptions: Dict[AnyStr, Subscription],
            results: BackTestingResults,
    ):
        raise NotImplementedError("Should implement start backtester")

    @abstractmethod
    def run_simulation(self, config: SimulationConfig):
        raise NotImplementedError("Should implement run simulations")

    @abstractmethod
    def create_simulation_plans(
            self,
            config: BackTestingConfig,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[str, SimulationConfig],
    ) -> List[SimulationConfig]:
        raise NotImplementedError("Should implement create simulation plan")
