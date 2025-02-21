from datetime import datetime
from zoneinfo import ZoneInfo

# One-liner to convert timestamp to US Eastern Time and extract the date


# event types
Market_Data = 'market_data'
Trade_Data = 'trade_data'
Closing_Price = 'closing_price'


__required_slots__ = [
    "timestamp_millis"
]


class Event:

    def __init__(
            self,
            **kwargs
    ):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def create(cls, attributes):

        missing_required_slot = [s for s in __required_slots__ if s not in attributes.keys()]

        if len(missing_required_slot) != 0:
            raise KeyError(f"{__file__} is mising the required slots {','.join(missing_required_slot)}")

        attributes['trading_session'] = datetime.fromtimestamp(
            attributes['timestamp_millis'] / 1000, tz=ZoneInfo('UTC')
        ).astimezone(ZoneInfo('America/New_York')).date()

        event = Event(**attributes)
        return event

    @property
    def has_price(self):
        return hasattr(self, 'price') or hasattr(self, 'ask_price') or hasattr(self, 'bid_price')

    def get_price(
            self,
            is_long: bool = None,
            matching_method: str = None
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
                price = (getattr(self, 'ask_price') + getattr(self, 'bid_price')) / 2
        else:
            price = getattr(self, 'price')

        return price

    def get_timestamp(self):
        return self.timestamp
