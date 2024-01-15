class Event:
    def __init__(
            self,
            datasource=None,
            order_book_id=None,
            unit_price=0,
            symbol=None,
            price_increment=None,
            timestamp=None,
            timestamp_micros=None,
            account_id=0,
            contract_qty=0,
            price=0,
            event_type=None,
            booking_risk=None,
            internalisation_risk=None,
            internalise_limit_orders=True,
            secondary_booking_risk=None,
            order_qty=None,
            ask_price=0,
            ask_qty=0,
            bid_price=0,
            bid_qty=0,
            currency=None,
            contract_unit_of_measure=None,
            execution_id=None,
            venue=None,
            pegged_price=None,
            tob_snapshot_bid_price=None,
            tob_snapshot_ask_price=None,
            bid_adjustment=None,
            ask_adjustment=None,
            rate_to_usd=1,
            trading_session=None,
            trading_session_year=None,
            trading_session_month=None,
            trading_session_day=None,
            utc_year=None,
            utc_month=None,
            utc_day=None,
            utc_hour=None,
            utc_minute=None,
            gfd=None,
            gfw=None,
            order_id=None,
            counterparty_account_id=None,
            immediate_order=None,
            untrusted=None,
    ):
        self.datasource = datasource
        self.execution_id = execution_id
        self.order_book_id = order_book_id
        self.immediate_order = immediate_order
        self.unit_price = unit_price
        self.symbol = symbol
        self.price_increment = price_increment
        self.timestamp = timestamp
        self.timestamp_micros = timestamp_micros
        self.type = event_type
        self.ask_price = int(ask_price)
        self.ask_qty = ask_qty
        self.bid_price = int(bid_price)
        self.bid_qty = bid_qty
        self.tob_snapshot_bid_price = tob_snapshot_bid_price
        self.tob_snapshot_ask_price = tob_snapshot_ask_price
        self.bid_adjustment = bid_adjustment
        self.ask_adjustment = ask_adjustment
        self.currency = currency
        self.contract_unit_of_measure = contract_unit_of_measure
        self.account_id = int(account_id)
        self.counterparty_account_id = counterparty_account_id
        self.contract_qty = int(contract_qty)
        self.order_qty = int(self.contract_qty) if order_qty is None else int(order_qty)
        self.order_id = order_id
        self.event_type = event_type
        self.booking_risk: float = booking_risk
        self.internalisation_risk: float = internalisation_risk
        self.internalise_limit_orders: bool = bool(internalise_limit_orders)
        self.secondary_booking_risk: float = secondary_booking_risk
        self.price = int(price)
        self.notional_value = self.contract_qty * self.unit_price * self.price
        self.is_long = True if self.contract_qty > 0 else False
        self.pegged_price = pegged_price
        self.venue = venue
        self.rate_to_usd = rate_to_usd
        self.trading_session = trading_session
        self.trading_session_year = trading_session_year
        self.trading_session_month = trading_session_month
        self.trading_session_day = trading_session_day
        self.utc_year = utc_year
        self.utc_month = utc_month
        self.utc_day = utc_day
        self.utc_hour = utc_hour
        self.utc_minute = utc_minute
        self.gfd = gfd
        self.gfw = gfw
        self.untrusted = untrusted

    def get_tob_price(self, is_long=True, match="mid", standardised=False):
        if match == "side_of_book":
            if standardised:
                price = round((self.bid_price if is_long else self.ask_price))
            else:

                price = (self.bid_price if is_long else self.ask_price) / 1000000
        elif match == "mid":
            if standardised:
                price = int(((self.ask_price + self.bid_price) / 2))
            else:
                price = ((self.ask_price + self.bid_price) / 2) / 1000000
        else:
            return KeyError(f"match key {match} not valide")

        return price

    def get_tob_quantity(self, is_long=True, standardised=False):
        if standardised:
            price = self.bid_qty if is_long else self.ask_qty
        else:
            price = (self.bid_qty if is_long else self.ask_qty) / 1000000
        return price

    def get_timestamp(self):
        return self.timestamp
