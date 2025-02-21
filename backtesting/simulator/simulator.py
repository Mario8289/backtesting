import datetime as dt
from abc import ABC
from typing import List, Any, Union, Tuple

import pandas as pd
import pytz

from math import ceil

from .simulation_plan import SimulationPlan
from ..event_stream import EventStream
from ..simulator.base import AbstractSimulator

eastern = pytz.timezone("US/Eastern")


class Simulator(AbstractSimulator, ABC):
    def __init__(
            self,
            name: str,
            event_stream: EventStream,
    ):
        self.name: str = name
        self.event_stream: EventStream = event_stream
        self.execution_attempt: int = 3

    @staticmethod
    def split_simulations(
            simulations: List[Any], no_of_batches: int
    ) -> List[List[Any]]:
        sims_per_batch: int = ceil(len(simulations) / no_of_batches) if len(
            simulations
        ) > 1 else 1
        sim_batches: List[List[Any]] = [
            simulations[i : i + sims_per_batch]
            for i in range(0, len(simulations), sims_per_batch)
        ]
        return sim_batches

    @staticmethod
    def filter_simulation_plan(
            plans: List[SimulationPlan], results_cache: pd.DataFrame
    ):
        plans_copy: List[SimulationPlan] = []
        _results_cache = results_cache.reset_index()  # noqa
        for i, plan in enumerate(plans):
            _sim_hash = plan.hash  # noqa
            _start_date = plan.start_date  # noqa
            _end_date = plan.end_date  # noqa
            if pd.eval(
                    "_start_date <= _results_cache.trading_session >= _end_date and _results_cache.hash == _sim_hash"
            ).any():
                continue
            else:
                plans_copy.append(plan)

        return plans_copy

    @staticmethod
    def _safe_slots(slots: Any) -> Union[List, Tuple]:
        if isinstance(slots, list) or isinstance(slots, tuple):
            return slots
        return [slots]
