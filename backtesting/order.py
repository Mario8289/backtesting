import datetime as dt


class Order:

    __slots__ = (
        "timestamp",
        "source",
        "symbol",
        "symbol_id",
        "account_id",
        "_contract_qty",
        "order_type",
        "time_in_force",
        "event_type",
        "price",
        "limit_price",
        "is_long",
        "filled_cost",
        "_unfilled_qty",
        "cancelled",
        "closed",
        "filled",
        "signal",
        "cancellation_reason"
    )

    def __init__(
            self,
            timestamp: dt.datetime,
            source: str,
            symbol_id: int,
            account_id: int,
            contract_qty: int,
            order_type: str,
            time_in_force: str,
            symbol: str = None,
            price: int = None,
            limit_price: int = None,
            signal: str = None,
            event_type: str = 'order',
    ):
        self.timestamp: dt.datetime = timestamp
        self.source: str = source
        self.symbol_id: int = symbol_id
        self.symbol: str = symbol
        self.account_id: int = account_id
        self.contract_qty: int = contract_qty
        self.order_type: str = order_type
        self.time_in_force: str = time_in_force
        self.price: int = price
        self.limit_price: int = limit_price
        self.filled_cost: int = 0
        self.unfilled_qty: int = self._contract_qty
        self.cancelled: bool = False
        self.closed: bool = False
        self.filled: bool = False
        self.signal: str = signal
        self.event_type: str = event_type
        self.cancellation_reason: str = None

    @property
    def unfilled_qty(self):
        return self._unfilled_qty

    @unfilled_qty.setter
    def unfilled_qty(self, value):
        self._unfilled_qty = value
        if self._unfilled_qty == 0:
            self.filled = True

    @property
    def contract_qty(self):
        return self._contract_qty

    @contract_qty.setter
    def contract_qty(self, value):
        self._contract_qty = value
        self.is_long = True if value > 0 else False
