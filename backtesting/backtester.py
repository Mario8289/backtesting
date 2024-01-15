from typing import List

import pandas as pd
import pytz

from .event import Event
from .event_stream.event_stream_snapshot import EventStreamSnapshot
from .matching_engine.matching_engine_default import MatchingEngineDefault
from .portfolio import Portfolio
from .order import Order
from .statistics.statistics import Stats

utc_tz = pytz.timezone("UTC")
london_tz = pytz.timezone("Europe/London")
eastern_tz = pytz.timezone("US/Eastern")


class Backtester:
    def __init__(
            self,
            risk_manager=None,
            strategy=None,
            instrument=None,
            netting_engine={"client": "fifo", "lmax": "fifo"},
            matching_method="mid",
            matching_engine=MatchingEngineDefault(matching_method="mid"),
            event_stream=EventStreamSnapshot(sample_rate="s"),
            process_client_portfolio=False,
            process_lmax_portfolio=True,
            store_client_trade_snapshot=False,
            store_lmax_trade_snapshot=True,
            store_client_md_snapshot=False,
            store_lmax_md_snapshot=False,
            store_client_eod_snapshot=False,
            store_lmax_eod_snapshot=False,
            portfolio: Portfolio = Portfolio,
            statistics: Stats = Stats,
    ):
        self.risk_manager = risk_manager
        self.strategy = strategy
        self.instrument = instrument
        self.client_portfolio = portfolio(
            cash=0,
            matching_method=matching_method,
            netting_engine=netting_engine["client"],
            calc_upnl=True if store_client_md_snapshot else False,
        )
        self.lmax_portfolio = portfolio(
            cash=0,
            matching_method=matching_method,
            netting_engine=netting_engine["lmax"],
            calc_upnl=True,
        )
        self.matching_method = matching_method
        self.matching_engine = matching_engine
        self.event_stream = event_stream
        self.unfilled_orders = []
        self.df_pnl = pd.DataFrame()
        self.statistics = statistics(self.lmax_portfolio)
        self.evt = None
        self.current_event = None
        self.process_client_portfolio = process_client_portfolio
        self.process_lmax_portfolio = process_lmax_portfolio
        self.store_client_trade_snapshot = store_client_trade_snapshot
        self.store_lmax_trade_snapshot = store_lmax_trade_snapshot
        self.store_client_md_snapshot = store_client_md_snapshot
        self.store_lmax_md_snapshot = store_lmax_md_snapshot
        self.store_client_eod_snapshot = store_client_eod_snapshot
        self.store_lmax_eod_snapshot = store_lmax_eod_snapshot

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

        for lmax_event in trades:

            order.unfilled_qty += order.order_qty

            self.lmax_portfolio.on_trade(lmax_event)

            if self.store_lmax_trade_snapshot:
                self.statistics.update_event_snapshot(
                    ts=event.timestamp,
                    order=order,
                    event=lmax_event,
                    portfolio=self.lmax_portfolio,
                    store_md_snapshot=self.store_lmax_md_snapshot,
                    label="lmax",
                )

        return False

    def on_event(self, event):

        self.current_event = event
        orders: List[Order] = []

        if self.process_client_portfolio:
            if event.event_type == "trade":
                self.client_portfolio.on_trade(event)
            elif (
                    event.event_type == "market_data" and self.store_client_md_snapshot
            ) or (
                    event.event_type == "closing_price" and self.store_client_eod_snapshot
            ):
                self.client_portfolio.update_portfolio(event)

        if self.process_lmax_portfolio:
            if event.event_type != "closing_price":
                orders = self.strategy.on_state(
                    self.client_portfolio, self.lmax_portfolio, event
                )

                # can move match_order_book if you want to match all order at once
                for order in orders:
                    # assess the order to determine if it fits inside the scope of the Risk Mananger
                    # todo: where would symbol or account come from?
                    order = self.risk_manager.assess_order(
                        order, self.lmax_portfolio, None, None
                    )

                    if not order.cancelled:
                        self.unfilled_orders.append(order)

                # if the orders are within risk appetite, attempt to fill open orders
                self.match_order_book(event)

                # store unrealised positions per tick
                if orders and self.store_lmax_md_snapshot:
                    self.lmax_portfolio.update_portfolio(event)
            else:
                self.lmax_portfolio.update_portfolio(event)

        if self.record_market_update(
                positions=self.lmax_portfolio.positions,
                store_md_snapshot=self.store_lmax_md_snapshot,
                store_eod_snapshot=self.store_lmax_eod_snapshot,
                event_type=event.event_type,
        ):
            self.statistics.update_event_snapshot(
                ts=event.timestamp,
                event=event,
                portfolio=self.lmax_portfolio,
                store_md_snapshot=self.store_lmax_md_snapshot,
                label="lmax",
            )

        if self.record_market_update(
                positions=self.lmax_portfolio.positions,
                store_md_snapshot=self.store_client_md_snapshot,
                store_eod_snapshot=self.store_client_eod_snapshot,
                event_type=event.event_type,
        ):
            self.statistics.update_event_snapshot(
                ts=event.timestamp,
                event=event,
                portfolio=self.lmax_portfolio,
                store_md_snapshot=self.store_lmax_md_snapshot,
                label="client",
            )

        if self.store_client_trade_snapshot:
            self.statistics.update_event_snapshot(
                ts=event.timestamp,
                event=event,
                portfolio=self.client_portfolio,
                store_md_snapshot=self.store_lmax_md_snapshot,
                label="client",
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
            trades,
            venue,
            tob: pd.DataFrame = pd.DataFrame(),
            closing_prices: pd.DataFrame = pd.DataFrame(),
            account_migrations: pd.DataFrame = pd.DataFrame(),
    ):

        _events = self.event_stream.generate_events(
            date=date,
            trades=trades,
            tob=tob,
            closing_prices=closing_prices,
            account_migrations=account_migrations,
        )
        for row in _events.itertuples():
            self.evt = Event(
                datasource=row.datasource,
                order_book_id=row.order_book_id,
                unit_price=row.unit_price,
                symbol=row.symbol,
                currency=row.currency,
                contract_unit_of_measure=row.contract_unit_of_measure,
                price_increment=row.price_increment,
                timestamp=row[0],
                account_id=row.account_id,
                contract_qty=row.contract_qty,
                price=row.price,
                event_type=row.event_type,
                booking_risk=row.booking_risk,
                internalisation_risk=row.internalisation_risk,
                internalise_limit_orders=row.internalise_limit_orders,
                immediate_order=row.immediate_order,
                order_qty=row.order_qty,
                order_id=row.order_id,
                ask_price=row.ask_price,
                ask_qty=row.ask_qty,
                bid_price=row.bid_price,
                bid_qty=row.bid_qty,
                gfd=row.gfd,
                gfw=row.gfw,
                execution_id=row.execution_id,
                venue=venue,
                rate_to_usd=row.rate_to_usd,
                tob_snapshot_ask_price=row.tob_snapshot_ask_price,
                tob_snapshot_bid_price=row.tob_snapshot_bid_price,
                ask_adjustment=row.ask_adjustment,
                bid_adjustment=row.bid_adjustment,
                trading_session=row.trading_session,
            )

            if self.evt is not None:
                self.on_event(self.evt)
