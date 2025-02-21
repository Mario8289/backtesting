import datetime as dt
from abc import ABCMeta, abstractmethod, ABC
from typing import List

import pandas as pd

from backtesting.event import Event
from backtesting.order import Order


class AbstractStrategy(ABC):
    __metaclass__ = ABCMeta

    @classmethod
    def create(cls, **kwargs):
        properties = {
            k: v
            for (k, v) in kwargs.items()
            if k in [x if x[0] != "_" else x[1:] for x in cls.__slots__]
        }
        return cls(**properties)

    # todo: create abstract portfolios that both backtester and closed positions and anything else can reference
    @abstractmethod
    def on_state(self, portfolio, event: Event) -> List[Order]:
        """
        handles the current state of the backtester
        """
        raise NotImplementedError("Should implement on_state()")

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError("Should implement get_name()")

    # noinspection PyMethodMayBeStatic
    def slot_lambda(self, df: pd.DataFrame, slot: str, value):
        return value if value else -1

    # noinspection PyMethodMayBeStatic
    def filter_snapshot(
            self, snapshot: pd.DataFrame, relative_type: str, relative_accounts: List[int]
    ) -> pd.DataFrame:
        return snapshot

    # noinspection PyMethodMayBeStatic
    def get_account_migrations(self, **kwargs):
        return pd.DataFrame()

    # noinspection PyMethodMayBeStatic
    def update(self, **kwargs) -> None:
        return None

    # noinspection PyMethodMayBeStatic
    def filter(self, **kwargs) -> None:
        return None

    def retrain_model(self, date: dt.date) -> bool:
        return False
