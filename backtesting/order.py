import datetime as dt


class Order:

    __slots__ = (
        "timestamp",
        "symbol",
        "account_id",
        "_order_qty",
        "order_type",
        "time_in_force",
        "event_type",
        "price",
        "limit_price",
        "is_long",
        "filled_cost",
        "_unfilled_qty",
        "cancelled",
        "order_book_id",
        "closed",
        "filled",
        "signal",
    )

    def __init__(
            self,
            timestamp: dt.datetime,
            order_book_id: int,
            account_id: int,
            order_qty: int,
            order_type: str,
            time_in_force: str,
            symbol: str = None,
            price: int = None,
            limit_price: int = None,
            signal: str = None,
            event_type: str = None,
    ):
        self.timestamp: dt.datetime = timestamp
        self.order_book_id: int = order_book_id
        self.symbol: str = symbol
        self.account_id: int = account_id
        self.order_qty: int = order_qty
        self.order_type: str = order_type
        self.time_in_force: str = time_in_force
        self.price: int = price
        self.limit_price: int = limit_price
        self.filled_cost: int = 0

        self.unfilled_qty: int = self._order_qty
        self.cancelled: bool = False
        self.closed: bool = False
        self.filled: bool = False
        self.signal: str = signal
        self.event_type: str = event_type

    @property
    def unfilled_qty(self):
        return self._unfilled_qty

    @unfilled_qty.setter
    def unfilled_qty(self, value):
        self._unfilled_qty = value
        if self._unfilled_qty == 0:
            self.filled = True

    @property
    def order_qty(self):
        return self._order_qty

    @order_qty.setter
    def order_qty(self, value):
        self._order_qty = value
        self.is_long = True if value > 0 else False
