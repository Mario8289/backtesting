from backtesting.subscriptions_cache.subscriptions_cache import SubscriptionsCache
from ..subscriptions_cache.csv_cache import CsvCache
from ..datastore.csv_datastore import CsvDataStore


def get_subscriptions_datastore(name):
    if name == CsvCache.__name__:
        datastore = CsvDataStore
    return datastore


def get_subscriptions_cache(name):
    if name == CsvCache.__name__:
        cache = CsvCache
    return cache


def create_subscriptions_cache(
        cache_name: str,
        datastore_parameters,
        mode: str,
        enable_cache: bool
) -> SubscriptionsCache:
    _cache = get_subscriptions_cache(cache_name)

    _auth = datastore_parameters.get('auth', {})
    _datastore = get_subscriptions_datastore(cache_name)
    datastore = _datastore.create(
        {k: v for (k, v) in datastore_parameters.items() if k != 'auth'}
    )
    datastore.authenticate(
        auth=_auth
    )

    cache_ = _cache.create(
        datastore=datastore,
        mode=mode,
        enable_cache=enable_cache
    )
    return cache_
