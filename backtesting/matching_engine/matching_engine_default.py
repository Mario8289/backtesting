from ..event import Event
from ..matching_engine.base import AbstractMatchingEngine  # noqa


class MatchingEngineDefault(AbstractMatchingEngine):
    __slots__ = ("name", "matching_method")

    def __init__(self, matching_method):
        super().__init__()
        self.name = "matching_engine_default"
        self.matching_method = matching_method

    def match_order(self, event, order):

        if not order.price:
            order.price = event.get_tob_price(
                not order.is_long, match=self.matching_method, standardised=True
            )

        trades = [
            Event(
                order_book_id=event.order_book_id,
                unit_price=event.unit_price,
                symbol=event.symbol,
                currency=event.currency,
                contract_unit_of_measure=event.contract_unit_of_measure,
                price_increment=event.price_increment,
                timestamp=event.timestamp,
                account_id=order.account_id,
                counterparty_account_id=event.account_id,
                contract_qty=order.order_qty,
                price=order.price,
                event_type=order.event_type,
                ask_price=event.ask_price,
                ask_qty=event.ask_qty,
                bid_price=event.bid_price,
                bid_qty=event.bid_qty,
                venue=event.venue,
                rate_to_usd=event.rate_to_usd,
                trading_session=event.trading_session,
            )
        ]
        return trades
