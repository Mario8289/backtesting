from abc import ABCMeta, abstractmethod
from typing import List, Dict

import pandas as pd

from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.config.simulation_config import SimulationConfig
from risk_backtesting.matching_engine import AbstractMatchingEngine
from risk_backtesting.risk_backtesting_result import BackTestingResults
from risk_backtesting.writers import Writer


class AbstractSimulator(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def start_simulator(
            self,
            config: BackTestingConfig,
            results_cache: pd.DataFrame,
            writer: Writer,
            matching_engine: AbstractMatchingEngine,
            simulation_configs: Dict[str, SimulationConfig],
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
