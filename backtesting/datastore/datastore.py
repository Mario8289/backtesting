import datetime as dt
from abc import ABC, abstractmethod
from typing import List, AnyStr

import pandas as pd
import pytz

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")
eastern_tz = pytz.timezone("US/Eastern")


class DataStore(ABC):
    def __init__(
            self,
            subscriptions: List[AnyStr],

    ):
        self.subscriptions: str = subscriptions

    @abstractmethod
    def load(self, subscription, parameters):
        pass



