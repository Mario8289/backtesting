import datetime as dt


class Trade:

    __slots__ = (
        "timestamp",
        "source",
        "symbol",
        "symbol_id",
        "account_id",
        "contract_qty",
        "price",
        "event_type",
        "rate_to_usd"
    )

    def __init__(
            self,
            timestamp: dt.datetime,
            source: str,
            symbol: str,
            symbol_id: str,
            account_id: int,
            contract_qty: float,
            price: float,
            rate_to_usd: float,
            event_type: str = 'trade'
    ):
        self.timestamp: dt.datetime = timestamp
        self.source: str = source
        self.symbol: str = symbol
        self.symbol_id: str = symbol_id
        self.account_id: int = account_id
        self.contract_qty: float = contract_qty
        self.price: float = price
        self.event_type: str = event_type
        self.rate_to_usd: float = rate_to_usd

    @property
    def has_price(self):
        return hasattr(self, 'price') or hasattr(self, 'ask_price') or hasattr(self, 'bid_price')

    def get_price(
            self,
            is_long: bool,
            matching_method: str
    ):
        if hasattr(self, 'ask_price') or hasattr(self, 'bid_price'):
            if matching_method == 'side_of_book':
                if is_long:
                    price = getattr(self, 'ask_price')
                else:
                    price = getattr(self, 'bid_price')
            elif matching_method == 'mid_price':
                price = (getattr(self, 'ask_price') + getattr(self, 'bid_price')) / 2
        else:
            price = getattr(self, 'price')

        return price



