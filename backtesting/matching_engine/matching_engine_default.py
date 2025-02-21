from ..trade import Trade
from ..matching_engine.base import AbstractMatchingEngine  # noqa


class MatchingEngineDefault(AbstractMatchingEngine):
    __slots__ = ("name", "matching_method")

    def __init__(self, matching_method):
        super().__init__()
        self.name = "matching_engine_default"
        self.matching_method = matching_method

    def match_order(self, event, order):

        if not order.price:
            order.price = event.get_price(
                is_long=not order.is_long,
                matching_method=self.matching_method
            )

        trades = [
            Trade(
                timestamp=order.timestamp,
                account_id=order.account_id,
                source=order.source,
                symbol_id=order.symbol_id,
                symbol=order.symbol,
                contract_qty=order.contract_qty,
                price=order.price,
                rate_to_usd=event.rate_to_usd
            )
        ]
        return trades
