from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Any, List

import pandas as pd

from risk_backtesting.simulator.simulation_plan import SimulationPlan


class SimulationResult(ABC):
    def __init__(
            self,
            name: str,
            hash_: str,
            start: date,
            end: date,
            instruments: List[int],
            start_time: datetime,
            end_time: datetime,
    ):
        self.name: str = name
        self.hash: str = hash_
        self.start: date = start
        self.end: date = end
        self.instruments: List[int] = instruments
        self.start_time: datetime = start_time
        self.end_time: datetime = end_time
        self.duration: timedelta = end_time - start_time

    @abstractmethod
    def is_success(self) -> bool:
        raise ValueError("Method `is_success` needs to be implemented")

    @property
    @abstractmethod
    def payload(self) -> Any:
        raise ValueError("Property `payload` needs to be implemented.")

    @abstractmethod
    def __str__(self) -> str:
        raise ValueError("Method `__str__` needs to be implemented")


class SimulationSuccess(SimulationResult):
    def __init__(
            self,
            name: str,
            hash_: str,
            start: date,
            end: date,
            instruments: List[int],
            start_time: datetime,
            end_time: datetime,
            has_events: bool,
            results: pd.DataFrame,
    ):
        super().__init__(name, hash_, start, end, instruments, start_time, end_time)
        self.has_events: bool = has_events
        self.results: pd.DataFrame = results

    def is_success(self) -> bool:
        return True

    @property
    def payload(self) -> pd.DataFrame:
        return self.results

    def __str__(self) -> str:
        return (
            f"Success on plan {self.name} / {self.hash},"
            f" start: {self.start},"
            f" end: {self.end},"
            f" instruments: {self.instruments},"
            f" has_events: {self.has_events},"
            f" start_time: {self.start_time},"
            f" end_time: {self.end_time},"
            f" duration: {self.duration}"
        )


def success(
        plan: SimulationPlan, has_events: bool, result: pd.DataFrame, start_time: datetime
) -> SimulationResult:
    return SimulationSuccess(
        plan.name,
        plan.hash,
        plan.start_date,
        plan.end_date,
        plan.instruments,
        start_time,
        datetime.now(),
        has_events,
        result,
    )


class SimulationFailure(SimulationResult):
    def __init__(
            self,
            name: str,
            hash_: str,
            start: date,
            end: date,
            instruments: List[int],
            start_time: datetime,
            end_time: datetime,
            exception: BaseException,
    ):
        super().__init__(name, hash_, start, end, instruments, start_time, end_time)
        self.exception: BaseException = exception

    def is_success(self) -> bool:
        return False

    @property
    def payload(self) -> BaseException:
        return self.exception

    def __str__(self) -> str:
        return (
            f"Failure on plan {self.name} / {self.hash},"
            f" start: {self.start},"
            f" end: {self.end},"
            f" instruments: {self.instruments},"
            f" start_time: {self.start_time},"
            f" end_time: {self.end_time},"
            f" duration: {self.duration},"
            f" exception: {self.exception}"
        )


def failure(
        plan: SimulationPlan, start_time: datetime, exception: BaseException
) -> SimulationResult:
    return SimulationFailure(
        plan.name,
        plan.hash,
        plan.start_date,
        plan.end_date,
        plan.instruments,
        start_time,
        datetime.now(),
        exception,
    )
