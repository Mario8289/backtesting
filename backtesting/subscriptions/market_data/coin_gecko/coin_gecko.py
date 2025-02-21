import requests
import pandas as pd
import datetime as dt

from pathlib import Path
from datetime import datetime
from decimal import Decimal

from backtesting.subscriptions.market_data.market_data import MarketData

from backtesting.subscriptions.attribute_codes import Apply_Sampling
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
    Timestamp_Millis: "int",
    Symbol: "object",
    Price: "float",
    Contract_Unit_of_Measure: "object",
    Currency: "object"
})

_file = Path(__file__)

event_type = _file.parents[1].name
event_src = _file.name.split('.')[0]
closing_price = 'closing_price'


class CoinGeckoMarketData(MarketData):
    def __init__(self, load_by_session=True):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = None

        super().__init__(
            load_by_session=load_by_session
        )

    def subscribe(self, api_key=None):
        """
        Initializes the required keys or configurations to access the data.
        :param api_key: Optional API key for authentication (not required for CoinGecko's free tier)
        """
        self.api_key = api_key
        print("Subscription initialized. API Key set." if api_key else "Subscription initialized without API Key.")

    def _get(
            self,
            start_date,
            end_date,
            instruments,
            interval
    ):
        """
        Loads data for a given cryptocurrency symbol for a specified date range.
        :param symbol: The cryptocurrency symbol (e.g., 'bitcoin', 'ethereum')
        :param start_date: The start date for data retrieval in 'YYYY-MM-DD' format
        :param end_date: The end date for data retrieval in 'YYYY-MM-DD' format
        :return: A pandas DataFrame containing historical data
        """
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_date = datetime.strptime(end_date, "%Y-%m-%d") + dt.timedelta(days=1)
        end_timestamp = int(end_date.timestamp())

        df = pd.DataFrame()

        for instrument in instruments:
            url = f"{self.base_url}/coins/{instrument}/market_chart/range"
            params = {
                'vs_currency': 'usd',
                'from': start_timestamp,
                'to': end_timestamp
            }

            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                prices = data.get('prices', [])
                price_increment = max([abs(Decimal(str(x[1])).as_tuple().exponent) for x in prices])

                # Convert the prices data to a pandas DataFrame
                _df = pd.DataFrame(prices, columns=[Timestamp_Millis, Price])
                _df['timestamp'] = pd.to_datetime(_df[Timestamp_Millis], unit='ms')
                _df.set_index('timestamp', inplace=True)
                _df[Symbol] = instrument
                _df[Symbol_Id] = f"{event_src}_{instrument}"
                _df[Price_Increment] = price_increment
                _df[Currency] = 'USD'
                _df[Contract_Size] = 1
                _df[Rate_To_Usd] = 1
                _df[Source] = event_src

                df = pd.concat([df, _df])
            else:
                print(f"Failed to fetch {instrument} data: {response.status_code} - {response.text}")
                return None

            coin_metadata_url = f"{self.base_url}/coins/{instrument}"
            coin_metadata_response = requests.get(coin_metadata_url, params=params)
            if coin_metadata_response.status_code == 200:
                coin_metadata = coin_metadata_response.json()
                currency = coin_metadata['symbol']
                df[Contract_Unit_of_Measure] = currency.upper()
            else:
                print(f"Failed to fetch {instrument} metadata: {coin_metadata_response.status_code} - {coin_metadata_response.text}")
                return None

        df[Event_Type] = event_type

        return df

#
# # Example usage:
# loader = CoinGeckoDataLoader()
# loader.subscribe()  # No API key is needed for basic usage
# df = loader.load('bitcoin', '2022-01-01', '2022-01-31')
#
# if df is not None:
#     print(df.head())