import os
from typing import Dict, AnyStr, Any
import pandas as pd
from datetime import timedelta

from backtesting.datastore.csv_datastore import CsvDataStore
from backtesting.subscriptions_cache.subscriptions_cache import SubscriptionsCache


class CsvCache(SubscriptionsCache):
    def __init__(self, datastore, enable_cache, mode):
        self.datastore: CsvDataStore = datastore
        super().__init__(
            name='CsvCache',
            enable_cache=enable_cache,
            mode=mode
        )

    def get(
            self,
            subscription,
            start_date,
            end_date,
            instruments,
            interval
    ):
        dates = pd.date_range(start_date, end_date)
        combinations = [[d.strftime("%Y-%m-%d"), i] for d in dates for i in instruments]

        files = [
            self.datastore.entry_point / subscription / interval / _date / f"{_instrument}.csv"
            for (_date, _instrument) in combinations
        ]

        missing_dates = []
        data = pd.DataFrame()
        for _date, _file in zip(dates, files):
            try:
                _data = pd.read_csv(_file)
                _data = _data.set_index('timestamp')
                _data = pd.to_datetime(_data.index)
                data = pd.concat([data, _data])
            except FileNotFoundError:
                missing_dates.append(_date)
            except Exception as e:
                raise e

        return data, missing_dates

    def save(
            self,
            subscription: str,
            subscription_events: pd.DataFrame,
            interval
    ):
        for (_trade_date, _symbol), grp in \
                subscription_events.reset_index().groupby([pd.Grouper(key='timestamp', freq='D'), 'symbol']):
            grp = grp.set_index('timestamp')
            _trade_date_str = _trade_date.strftime('%Y-%m-%d')
            _dir = self.datastore.entry_point / subscription / interval / _trade_date_str
            os.makedirs(_dir, exist_ok=True)
            grp.to_csv(_dir / f"{_symbol}.csv")

        # TODO: if there are dates with no Prices then create a blank file, this will make it easier when loading
        #  from cache and working out where the gaps are that you have to load







