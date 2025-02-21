from typing import List, Dict, AnyStr

import pandas as pd
import pytz

from .event import Event
from .event_stream.event_stream_snapshot import EventStreamSnapshot
from .matching_engine.matching_engine_default import MatchingEngineDefault
from .portfolio import Portfolio
from .order import Order
from .statistics.statistics import Stats
from .subscriptions.subscription import Subscription

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")
eastern_tz = pytz.timezone("US/Eastern")


class Backtester:
    def __init__(
            self,
            risk_manager=None,
            strategy=None,
            instrument=None,
            netting_engine="fifo",
            matching_method="mid",
            matching_engine=MatchingEngineDefault(matching_method="mid"),
            event_stream=EventStreamSnapshot(sample_rate="s"),
            subscriptions: Dict[AnyStr, Subscription] = None,
            process_portfolio=True,
            store_order_snapshot=True,
            store_trade_snapshot=True,
            store_md_snapshot=False,
            store_eod_snapshot=False,
            portfolio: Portfolio = Portfolio,
            statistics: Stats = Stats,
    ):
        self.risk_manager = risk_manager
        self.strategy = strategy
        self.instrument = instrument
        self.portfolio = portfolio(
            cash=0,
            matching_method=matching_method,
            netting_engine=netting_engine,
            calc_upnl=True,
        )
        self.matching_method = matching_method
        self.matching_engine = matching_engine
        self.subscriptions = subscriptions
        self.event_stream = event_stream
        self.unfilled_orders = []
        self.df_pnl = pd.DataFrame()
        self.statistics = statistics(self.portfolio)
        self.evt = None
        self.current_event = None
        self.process_portfolio = process_portfolio
        self.store_order_snapshot = store_order_snapshot
        self.store_trade_snapshot = store_trade_snapshot
        self.store_md_snapshot = store_md_snapshot
        self.store_eod_snapshot = store_eod_snapshot

    def get_timestamp(self):
        return self.current_event.get_timestamp()

    def get_trade_date(self):
        timestamp = self.get_timestamp()
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")

    def match_order_book(self, event):
        if len(self.unfilled_orders) > 0:
            self.unfilled_orders = [
                order for order in self.unfilled_orders if self.fill_order(order, event)
            ]

    def fill_order(self, order, event):

        trades = self.matching_engine.match_order(event, order)

        for trade in trades:

            order.unfilled_qty += order.contract_qty

            self.portfolio.on_trade(
                trade=trade,
                event=event
            )

            if self.store_trade_snapshot:
                self.statistics.update_event_snapshot(
                    order=None,
                    trade=trade,
                    event=event,
                    portfolio=self.portfolio
                )

        return False

    def on_event(self, event):

        self.current_event = event
        orders: List[Order] = []

        if self.process_portfolio:
            orders.extend(self.strategy.on_state(self.portfolio, event))

            # can move match_order_book if you want to match all order at once
            for order in orders:
                # assess the order to determine if it fits inside the scope of the Risk Mananger
                # todo: where would symbol or account come from?
                order = self.risk_manager.assess_order(
                    order, self.portfolio, None, None
                )

                # Store Orders
                if self.store_order_snapshot:
                    self.statistics.update_event_snapshot(
                        order=order,
                        trade=None,
                        event=event,
                        portfolio=self.portfolio
                    )

                if not order.cancelled:
                    self.unfilled_orders.append(order)

            # if the orders are within risk appetite, attempt to fill open orders
            self.match_order_book(event)

            # Store each Market Data Update
            if self.store_md_snapshot:
                self.portfolio.update_portfolio(event)

        if self.record_market_update(
                positions=self.portfolio.positions,
                store_md_snapshot=self.store_md_snapshot,
                store_eod_snapshot=self.store_eod_snapshot,
                event_type=event.event_type,
        ):
            self.statistics.update_event_snapshot(
                order=None,
                event=event,
                portfolio=self.portfolio
            )

    @staticmethod
    def record_market_update(
            positions, store_md_snapshot, store_eod_snapshot, event_type
    ) -> bool:
        return len(positions) != 0 and (
                (store_md_snapshot and event_type == "market_data")
                or (store_eod_snapshot and event_type == "closing_price")
        )

    def run_day_simulation(
            self,
            date,
            subscriptions
    ):

        _events = self.event_stream.generate_events(
            date=date,
            subscriptions=subscriptions
        )
        x = 1
        for row in _events.reset_index().itertuples(index=False):
            event_dict = row._asdict()
            self.evt = Event.create(event_dict)

            if self.evt is not None:
                self.on_event(self.evt)

            x += 1
