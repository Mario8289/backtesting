from typing import Dict, Any, AnyStr, Type
from pandas import DataFrame

from backtesting.subscriptions.subscription import Subscription

from backtesting.subscriptions.market_data.lmax.lmax import LmaxMarketData
from backtesting.subscriptions.market_data.coin_gecko.coin_gecko import CoinGeckoMarketData
from backtesting.subscriptions.market_data.yahoo.yahoo import YahooMarketData

from backtesting.subscriptions.indicator.crypto_fear_greed_index.crypto_fear_greed_index import CryptoFearGreedIndex


def determine_subscription_constructor(subscription: str) -> Type[Subscription]:
    if "LmaxMarketData" == subscription:
        return LmaxMarketData
    elif "CoinGecko" == subscription:
        return CoinGeckoMarketData
    elif "Yahoo" == subscription:
        return YahooMarketData
    elif "CryptoFearGreedIndex" == subscription:
        return CryptoFearGreedIndex
    else:
        raise ValueError(f"Invalid subscription type {subscription}")


def create_subscription(
        subscription: str,
        subscription_parameters: Dict[AnyStr, Any]
) -> Subscription:
    constructor = determine_subscription_constructor(subscription)
    instance = constructor.create(**subscription_parameters)
    return instance
