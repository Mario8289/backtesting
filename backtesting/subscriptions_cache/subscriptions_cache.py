from typing import Dict, Any, AnyStr
from pathlib import Path
from pandas import DataFrame

from abc import abstractmethod

from ..datastore.datastore import DataStore


class SubscriptionsCache:
    def __init__(
            self,
            name: str,
            enable_cache: bool,
            mode: str,
    ):
        self.name: str = name
        self.enable_cache: bool = enable_cache
        self.mode: str = mode

    @classmethod
    def create(
            cls,
            datastore: DataStore,
            enable_cache: bool,
            mode: str
    ):

        instance = cls(
            datastore=datastore,
            enable_cache=enable_cache,
            mode=mode
        )

        return instance

    @abstractmethod
    def get(
            self,
            subscription,
            start_date,
            end_date,
            instruments,
            interval
    ):
        pass

    @abstractmethod
    def save(
            self,
            subscription: str,
            subscription_events: DataFrame,
            interval
    ):
        pass







