import datetime as dt
from typing import List

import pandas as pd
import pytz

from ..subscriptions.attribute_codes import Apply_Sampling
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

    def sample(self, data: pd.DataFrame, trading_session: dt.datetime) -> pd.DataFrame:
        if not data.empty:
            apply_sample_df = data[data[Apply_Sampling] == True]
            dont_apply_sample_df = data[data[Apply_Sampling] == False]

            sampled_df = (
                apply_sample_df.groupby(['symbol'])
                .apply(lambda grp: grp.sample(frac=self.sample_rate))
                .reset_index(level=0, drop=True)
            )
            data = pd.concat([dont_apply_sample_df, sampled_df]).sort_index()
        return data
