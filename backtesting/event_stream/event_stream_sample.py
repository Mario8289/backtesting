import datetime as dt
from typing import List

import pandas as pd
import pytz

from ..event_stream.event_stream import EventStream

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")
eastern_tz = pytz.timezone("US/Eastern")


class EventStreamSample(EventStream):
    __slots__ = ("name", "sample_rate", "excl_period", "include_eod_snapshot")

    def __init__(
            self,
            sample_rate: str,
            excl_period: List[List[int]] = None,
            include_eod_snapshot: bool = False,
    ):
        super().__init__(
            sample_rate=sample_rate,
            name="event_stream_sample",
            excl_period=excl_period,
            include_eod_snapshot=include_eod_snapshot,
        )

    def sample(self, tob: pd.DataFrame, trading_session: dt.datetime) -> pd.DataFrame:
        if not tob.empty:
            tob_sample = (
                tob.groupby(["order_book_id"])
                .apply(lambda grp: grp.sample(frac=self.sample_rate))
                .reset_index(level=0, drop=True)
            )
            return tob_sample
        else:
            return tob
