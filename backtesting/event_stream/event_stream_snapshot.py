import datetime as dt
from typing import List

import pandas as pd
import pytz

from ..event_stream.event_stream import EventStream

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")
eastern_tz = pytz.timezone("US/Eastern")


class EventStreamSnapshot(EventStream):
    __slots__ = ("name", "sample_rate", "excl_period", "include_eod_snapshot")

    def __init__(
            self,
            sample_rate: str,
            excl_period: List[List[int]] = None,
            include_eod_snapshot: bool = False,
    ):
        super().__init__(
            sample_rate, "event_stream_snapshot", include_eod_snapshot, excl_period
        )

    @staticmethod
    def get_trading_session_bounds(trading_session: dt.datetime) -> List[dt.datetime]:
        localised = eastern_tz.localize(trading_session)
        start = (localised.replace(hour=17) - dt.timedelta(days=1)).astimezone(utc_tz)
        end = (localised.replace(hour=17) - dt.timedelta(microseconds=1)).astimezone(
            utc_tz
        )
        return [start, end]

    def sample(self, market_data: pd.DataFrame, trading_session: dt.datetime) -> pd.DataFrame:
        if not market_data.empty:
            time_bounds = self.get_trading_session_bounds(trading_session)

            _tick_index: pd.DatetimeIndex = pd.date_range(
                *time_bounds, freq=self.sample_rate, tz="UTC"
            )

            _tick_df: pd.DataFrame = pd.DataFrame()

            for symbol_id in market_data.symbol_id.unique().tolist():
                _tick_symbol_df = pd.DataFrame(
                    index=_tick_index,
                    data={"symbol_id": symbol_id, "event_type": "market_data"},
                )
                _tick_df = pd.concat([_tick_df, _tick_symbol_df], axis=0)
                _tick_df.sort_index(inplace=True)

            market_data_sample: pd.DataFrame = pd.merge_asof(
                _tick_df,
                market_data,
                direction="backward",
                by="symbol_id",
                left_index=True,
                right_index=True,
            )
            return market_data_sample
        else:
            return market_data
