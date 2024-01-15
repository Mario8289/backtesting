import operator
from typing import List, Dict

import numpy as np
import pandas as pd
import datetime as dt
import pytz
from math import floor

from ..event import Event
from ..exit_strategy import AbstractExitStrategy
from ..order import Order
from ..strategy.base import AbstractStrategy

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")


class InternalisationStrategy(AbstractStrategy):
    __output_slots__ = [
        "name",
        "max_pos_qty",
        "allow_partial_fills",
        "max_pos_qty_buffer",
        "position_lifespan",
        "max_pos_qty_type",
        "max_pos_qty_level",
        "max_pos_qty_rebalance_rate",
        "exit_strategy",
        "position_lifespan_exit_strategy",
    ]
    __slots__ = __output_slots__ + [
        "account_id",
        "last_order_id",
        "last_order_filled",
        "_max_pos_qty",
        "_next_rebalance_date",
    ]

    # TODO: __slots__ conflicts with class variable workout when using setters and slots

    def __init__(
            self,
            account_id: int,
            max_pos_qty: int,
            max_pos_qty_buffer: float,
            allow_partial_fills: bool,
            exit_strategy: AbstractExitStrategy,
            max_pos_qty_type: str = "contracts",
            max_pos_qty_level: str = "instrument",
            max_pos_qty_rebalance_rate: str = None,
            position_lifespan=None,
            position_lifespan_exit_strategy: AbstractExitStrategy = None,
    ):
        self.name: str = "internalisation"
        self.account_id: int = account_id
        self._max_pos_qty: int = dict()
        self.max_pos_qty: int = max_pos_qty
        self.max_pos_qty_buffer: float = max_pos_qty_buffer
        self.allow_partial_fills: bool = allow_partial_fills
        self.max_pos_qty_type: str = max_pos_qty_type
        self.max_pos_qty_level: str = max_pos_qty_level
        self.max_pos_qty_rebalance_rate: str = max_pos_qty_rebalance_rate
        self.position_lifespan = position_lifespan
        self.position_lifespan_exit_strategy: AbstractExitStrategy = position_lifespan_exit_strategy
        self.exit_strategy: AbstractExitStrategy = exit_strategy
        self.last_order_filled: bool = False
        self.last_order_id: int = None
        self._next_rebalance_date: dt.datetime = None

    @classmethod
    def create(cls, **kwargs):
        properties = dict()
        for (k, v) in kwargs.items():
            if k in [x if x[0] != "_" else x[1:] for x in cls.__slots__]:
                if k == "max_pos_qty":
                    instruments = (
                        kwargs["instruments"]
                        if type(kwargs["instruments"]) == list
                        else [kwargs["instruments"]]
                    )
                    if type(v) == int:
                        if kwargs.get("max_pos_qty_level", None) == "currency":
                            currency_limits = {}
                            for i in instruments:
                                currency = kwargs["metadata"].get("symbol_currencies")[
                                    i
                                ]
                                currency_limits[currency] = v

                            properties[k] = currency_limits
                        else:
                            properties[k] = {i: v for i in instruments}
                    else:
                        raise TypeError(
                            f"internalisation strategy can not handle type: {type(v)} for property {k}"
                        )
                else:
                    properties[k] = v
            else:
                pass

        return cls(**properties)

    def filter(self, instrument):
        self.max_pos_qty = {
            k: v for (k, v) in self.max_pos_qty.items() if k == instrument
        }

    def update_max_pos_qty(self, value: int, key):
        if self.max_pos_qty_type == "dollars":
            raise ValueError(
                "this method can only be used with max_pos_qty type 'contracts'"
            )

        self.max_pos_qty[key] = value
        self._max_pos_qty[key] = value * 1e2

    def set_next_rebalance_date(self):
        self._next_rebalance_date = self._next_rebalance_date + pd.to_timedelta(
            self.max_pos_qty_rebalance_rate
        )

    @staticmethod
    def dollars_to_contracts(
            usd_amount, instrument_class, unit_price, rate, closing_price=None
    ):
        rate = 1 / rate
        if instrument_class == "EQUITY_INDEX_CFD":
            return round(usd_amount / unit_price * rate / closing_price)
        else:
            return round(usd_amount / unit_price * rate)

    def slot_lambda(self, df: pd.DataFrame, slot: str, value):
        if slot in ("max_pos_qty"):
            if self.max_pos_qty_level == "instrument":
                return df["order_book_id"].map(lambda x: value[x])
            else:
                return df["contract_unit_of_measure"].map(lambda x: value[x])
        else:
            return value if value else -1

    def set_max_pos_qty(
            self, date: dt.datetime, shard: str, instruments: Dict, dataserver
    ) -> float:
        closing_prices: pd.DataFrame = pd.DataFrame()

        rates = dataserver.get_usd_rate_for_instruments_unit_of_measure(
            shard=shard, date=self._next_rebalance_date, instruments=instruments,
        )

        if any(rates.get("class") == "EQUITY_INDEX_CFD"):
            closing_prices = dataserver.get_closing_prices(
                shard=shard, start_date=date, end_date=date, instruments=instruments,
            )

        for row in rates[
            ["instrument_id", "class", "contract_unit_of_measure"]
        ].itertuples():
            instrument = row[1]
            instrument_cla = row[2]
            unit_of_measure = row[3]
            closing_price = None

            if instrument_cla == "EQUITY_INDEX_CFD":
                closing_price = closing_prices.loc[
                    closing_prices["order_book_id"] == instrument, "price"
                ].item()

            unit_price = rates.loc[
                rates["instrument_id"] == instrument, "unit_price"
            ].item()
            rate = rates.loc[rates["instrument_id"] == instrument, "rate"].item()

            if self.max_pos_qty_level == "currency":
                key = unit_of_measure
            else:
                key = instrument

            contracts = self.dollars_to_contracts(
                self.max_pos_qty[key], instrument_cla, unit_price, rate, closing_price,
            )
            self._max_pos_qty[key] = contracts * 1e2

    def update(self, shard: str, date: dt.datetime, instruments: List[int], dataserver):
        if self.max_pos_qty_type == "dollars":
            if self.max_pos_qty_rebalance_rate is None:
                # only set the contracts for the first session you have rates for
                if self._next_rebalance_date is None:
                    self._next_rebalance_date = date

                if self._next_rebalance_date == date:
                    self.set_max_pos_qty(date, shard, instruments, dataserver)
                else:  # contracts have already been set, so no need to set them again.
                    pass
            else:
                if self._next_rebalance_date is None:
                    self._next_rebalance_date = date
                    self.set_max_pos_qty(date, shard, instruments, dataserver)

                    self.set_next_rebalance_date()
                elif self._next_rebalance_date == date:
                    self.set_max_pos_qty(date, shard, instruments, dataserver)
                    self.set_next_rebalance_date()
                else:
                    self._next_rebalance_date == date
        else:
            if len(self._max_pos_qty) == 0:
                self._max_pos_qty = {k: v * 1e2 for (k, v) in self.max_pos_qty.items()}

    def get_account_migrations(self, **kwargs):
        # TODO: implement this
        return pd.DataFrame()

    def filter_snapshot(
            self, snapshot: pd.DataFrame, relative_type: str, relative_accounts: List[int]
    ) -> pd.DataFrame:
        if "internalisation_account_id" in snapshot.columns:
            internalisation_accounts: List[int] = [self.account_id]
            if "internalisation" == relative_type:
                internalisation_accounts.extend(relative_accounts)
                return snapshot[
                    snapshot.internalisation_account_id.isin(internalisation_accounts)
                ]
            elif "client" == relative_type:
                return snapshot[
                    (snapshot.internalisation_account_id.isin(internalisation_accounts))
                    | (snapshot.account_id.isin(relative_accounts))
                    ]
            else:
                return snapshot[
                    snapshot.internalisation_account_id.isin(internalisation_accounts)
                ]
        return snapshot

    def calculate_market_signals(self, lmax_portfolio, event: Event) -> List[Order]:
        # Signal Triggers
        orders: List[Order] = []

        if 0 == len(lmax_portfolio.positions):
            return orders

        position = lmax_portfolio.positions.get(
            (event.venue, event.order_book_id, self.account_id)
        )
        open_positions = position.open_positions

        price_tick = event.get_tob_price(
            match=lmax_portfolio.matching_method,
            is_long=True if position.net_position > 0 else False,  # inverse
            standardised=True,
        )

        # TODO: can do this on the position object as I build up position
        # get average position
        total_opening_cost = (
            open_positions.cost
            if lmax_portfolio.netting_engine == "avg_price"
            else sum([x.price * x.quantity for x in open_positions])
        )
        total_open_quantity = (
            open_positions.quantity
            if lmax_portfolio.netting_engine == "avg_price"
            else sum([x.quantity for x in open_positions])
        )
        avg_price = int(round((total_opening_cost / total_open_quantity), 0))

        if self.position_lifespan:
            lifespan_orders: List[
                Order
            ] = self.position_lifespan_exit_strategy.generate_exit_order_signal(
                event=event,
                account=self.account_id,
                avg_price=avg_price,
                tick_price=price_tick,
                position=position,
            )

            if getattr(event, self.position_lifespan):
                return lifespan_orders

        if not event.untrusted:
            return self.exit_strategy.generate_exit_order_signal(
                event=event,
                account=self.account_id,
                avg_price=avg_price,
                tick_price=price_tick,
                position=position,
            )
        return orders

    def set_position_skew(self, net_position, key) -> List[float]:
        short_max_contracts: int = (
                                           self._max_pos_qty[key] * -1
                                   ) * self.max_pos_qty_buffer
        long_max_contracts: int = self._max_pos_qty[key] * self.max_pos_qty_buffer

        position_skew: List[float] = [
            short_max_contracts,
            long_max_contracts,
        ]

        if net_position:
            position_skew = [operator.sub(x, net_position) for x in position_skew]
            # ensure that either side of skew has not been inverted due to position limit update
            if np.sign(position_skew[0]) == 1:
                position_skew[0] = 0
            if np.sign(position_skew[1]) == -1:
                position_skew[1] = 0

            # ensure that if you are inside the buffer then set position skew to 0
            if position_skew[0] > (short_max_contracts + self._max_pos_qty[key]):
                position_skew[0] = 0
            if position_skew[1] < (long_max_contracts - self._max_pos_qty[key]):
                position_skew[1] = 0

        return position_skew

    def get_trade_size(
            self,
            contract_qty: int,
            order_qty: int,
            internalisation_risk: float,
            remaining_contracts: float,
            same_order: bool,
    ):
        order_qty = order_qty * -1

        if internalisation_risk:
            trade_size = floor(contract_qty * internalisation_risk) * -1

        else:
            trade_size = contract_qty * -1

        if self.allow_partial_fills:
            if np.sign(trade_size) == 1:
                trade_size = min(trade_size, remaining_contracts)
            else:
                trade_size = max(trade_size, remaining_contracts)
        else:
            if same_order:
                pass
            else:
                if np.sign(trade_size) == 1:
                    if order_qty > remaining_contracts:
                        trade_size = 0
                else:
                    if order_qty < remaining_contracts:
                        trade_size = 0

        return trade_size

    def calculate_trade_signals(self, lmax_portfolio, event: Event) -> List[Order]:
        lmax_inst_position = lmax_portfolio.positions.get(
            (event.venue, event.order_book_id, self.account_id)
        )

        if self.max_pos_qty_level == "currency":
            net_position: int = lmax_portfolio.inventory_contracts.get(
                event.contract_unit_of_measure, 0
            )
            position_limit_key = event.contract_unit_of_measure
        else:
            net_position: int = lmax_inst_position.net_position if lmax_inst_position else 0
            position_limit_key = event.order_book_id

        orders: List[Order] = []
        same_order: None

        position_skew: List[float] = self.set_position_skew(
            net_position, position_limit_key
        )

        same_order: bool = (
            (self.last_order_id == event.order_id and self.last_order_filled)
            if hasattr(self, "last_order_id")
            else False
        )

        trade_size = self.get_trade_size(
            event.contract_qty,
            event.order_qty,
            event.internalisation_risk,
            position_skew[0] if np.sign(event.contract_qty) == 1 else position_skew[1],
            same_order,
        )

        cost = self.capture_tighten_cost(event)

        # open short position on client long
        if (
                np.sign(event.contract_qty) == 1
                and ((event.immediate_order == 1) | (event.internalise_limit_orders))
                and abs(trade_size) > 0
        ):
            orders.append(
                Order(
                    event.timestamp,
                    event.order_book_id,
                    self.account_id,
                    trade_size,
                    order_type="N",
                    time_in_force="K",
                    price=event.price,
                    symbol=event.symbol,
                    signal="client_long_open_short",
                    event_type="internal",
                )
            )
            self.last_order_filled = True

            if lmax_inst_position:
                # todo: either add a comment as to why this is done here or move it to a different method that makes the behaviour clear
                lmax_inst_position.tighten_cost += cost

        # open long position on client short
        elif (
                np.sign(event.contract_qty) == -1
                and ((event.immediate_order == 1) | (event.internalise_limit_orders))
                and abs(trade_size) > 0
        ):
            orders.append(
                Order(
                    event.timestamp,
                    event.order_book_id,
                    self.account_id,
                    trade_size,
                    order_type="N",
                    time_in_force="K",
                    price=event.price,
                    symbol=event.symbol,
                    signal="client_short_open_long",
                    event_type="internal",
                )
            )
            self.last_order_filled = True

            if lmax_inst_position:
                # todo: either add a comment as to why this is done here or move it to a different method that makes the behaviour clear
                lmax_inst_position.tighten_cost += cost

        else:
            self.last_order_filled = False

            if lmax_inst_position:
                lmax_inst_position.notional_rejected += self.calculate_notional_rejected(
                    event
                )

        self.last_order_id = event.order_id

        return orders

    @staticmethod
    def calculate_notional_rejected(event):
        notional_rejected = (abs(event.notional_value) / 100000000) * event.rate_to_usd
        if event.internalisation_risk:
            notional_rejected = notional_rejected * event.internalisation_risk
        return notional_rejected

    @staticmethod
    def capture_tighten_cost(event: Event) -> int:
        tighten_cost: int = 0
        if np.sign(event.contract_qty) == 1:
            tighten_cost = (
                event.tob_snapshot_ask_price - event.price
                if event.tob_snapshot_ask_price != 0
                else 0
            )

        if np.sign(event.contract_qty) == -1:
            tighten_cost = (
                event.price - event.tob_snapshot_bid_price
                if event.tob_snapshot_bid_price != 0
                else 0
            )

        tighten_cost = (
                               ((abs(event.contract_qty) * tighten_cost) * event.unit_price) / 100000000
                       ) * event.rate_to_usd

        return tighten_cost

    def on_state(self, client_portfolio, lmax_portfolio, event: Event) -> List[Order]:
        switcher = {
            "trade": self.calculate_trade_signals,
            "market_data": self.calculate_market_signals,
        }
        func = switcher.get(event.event_type)
        # todo: python doesn't do class method switching very nicely (or at least IntelliJ doesn't like it)
        #       maybe clean it up a bit
        return func(lmax_portfolio, event)

    def get_name(self) -> str:
        return self.name
