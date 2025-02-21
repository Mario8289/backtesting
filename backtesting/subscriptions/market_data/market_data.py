from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from backtesting.subscriptions.subscription import Subscription
from backtesting.subscriptions.subscription import set_dtypes

from backtesting.subscriptions.attribute_codes import Apply_Sampling
from backtesting.subscriptions.attribute_codes import Date
from backtesting.subscriptions.attribute_codes import Event_Type
from backtesting.subscriptions.attribute_codes import Price_Increment
from backtesting.subscriptions.attribute_codes import Price
from backtesting.subscriptions.attribute_codes import Symbol
from backtesting.subscriptions.attribute_codes import Source
from backtesting.subscriptions.attribute_codes import Symbol_Id
from backtesting.subscriptions.attribute_codes import Timestamp_Millis
from backtesting.subscriptions.attribute_codes import Contract_Size
from backtesting.subscriptions.attribute_codes import Rate_To_Usd
from backtesting.subscriptions.attribute_codes import Contract_Unit_of_Measure
from backtesting.subscriptions.attribute_codes import Currency

schema = {}

schema.update({
    Timestamp_Millis: "int64",
    Symbol: "object",
    Date: "object",
    Price: "float",
    Contract_Unit_of_Measure: "object",
    Currency: "object",
    Symbol: "object",
    Symbol_Id: "object",
    Price_Increment: "float",
    Event_Type: "object",
    Contract_Size: "float",
    Rate_To_Usd: "float",
    Source: "object",
    Apply_Sampling: "bool"
})


class MarketData(Subscription):
    def __init__(self, load_by_session=True):
        super().__init__(
            load_by_session=load_by_session
        )

    @abstractmethod
    def subscribe(self, api_key=None):
        pass

    @abstractmethod
    def _get(
            self,
            start_date,
            end_date,
            instruments,
            interval
    ):
        pass

    def get(
            self,
            start_date,
            end_date,
            instruments,
            interval
    ):
        df = self._get(
            start_date,
            end_date,
            instruments,
            interval
        )

        if df is None:
            raise TypeError(f"No Data retrieved between dates {start_date} - {end_date} for symbols {', '.join(instruments)}")

        df[Apply_Sampling] = True

        # add closing price events
        cdf = df.reset_index().groupby(Symbol_Id).last().reset_index().set_index('timestamp')
        cdf[Event_Type] = 'closing_price'
        cdf[Apply_Sampling] = False

        df = pd.concat([df, cdf])

        df = set_dtypes(df, schema)

        return df
