from typing import List

import pandas as pd

from risk_backtesting.simulator.simulation_result import SimulationResult


class SimulationBatchResult:
    def __init__(self, df: pd.DataFrame, errors: List[SimulationResult]):
        self.df: pd.DataFrame = df
        self.errors: List[SimulationResult] = errors
