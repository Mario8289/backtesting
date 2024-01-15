from typing import List

import pandas as pd

from risk_backtesting.simulator.simulation_batch_result import SimulationBatchResult
from risk_backtesting.simulator.simulation_result import SimulationResult


class BackTestingResults:
    def __init__(self):
        self.errors: List[SimulationResult] = []
        self.df: pd.DataFrame = pd.DataFrame()

    def accumulate(self, result: SimulationBatchResult):
        self.errors.extend(result.errors)

    def accumulate_df(self, df: pd.DataFrame, sort: bool = False):
        pass


class DataFrameAccumulatingBackTestingResults(BackTestingResults):
    def accumulate(self, result: SimulationBatchResult):
        super().accumulate(result)
        self.df = pd.concat([self.df, result.df])

    def accumulate_df(self, df: pd.DataFrame, sort: bool = False):
        self.df = pd.concat([self.df, df], sort=sort)


def build_backtesting_results(return_result: bool) -> BackTestingResults:
    if return_result:
        return DataFrameAccumulatingBackTestingResults()
    return BackTestingResults()
