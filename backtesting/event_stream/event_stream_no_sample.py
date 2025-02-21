import datetime as dt
from typing import List

import pandas as pd
import pytz

from ..event_stream.event_stream import EventStream

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")
eastern_tz = pytz.timezone("US/Eastern")


class EventStreamNoSample(EventStream):
    __slots__ = ("name", "excl_period", "include_eod_snapshot")

    def __init__(
            self,
            excl_period: List[List[int]] = None,
            include_eod_snapshot: bool = False,
    ):
        super().__init__(
            sample_rate=None,
            name="event_stream_no_sample",
            include_eod_snapshot=include_eod_snapshot,
            excl_period=excl_period
        )

    def sample(self, market_data: pd.DataFrame, trading_session: dt.datetime) -> pd.DataFrame:
        return market_data
