import datetime as dt
import operator

import numpy as np
import pandas as pd

from ..event import Event
from ..loaders.load_price_slippage_model import PriceSlippageLoader
from ..matching_engine.base import AbstractMatchingEngine  # noqa


class MatchingEngineDistribution(AbstractMatchingEngine):
    __slots__ = (
        "name",
        "tier",
        "model",
        "matching_method",
        "model_location",
        "min_price_slippage",
    )

    def __init__(self, matching_method, tier=1):
        self.name: str = "matching_engine_distribution"
        self.matching_method: str = matching_method
        self.model_location: str = "broker/order_book_distribution/csv"
        self.tier = tier
        self.min_price_slippage = 1
        self.model = dict()

    def load_model(
            self, loader: PriceSlippageLoader, datasource_label: str, date: dt.date
    ):
        file_ob_p_d = f"{date}-order-book-depth-tier-1.csv"
        df_depth = loader.get_model(
            file=file_ob_p_d,
            prefix=self.model_location,
            datasource_label=datasource_label,
            date=date,
        )
        self.set_model_depth(df_depth)

        file_s_c = f"{date}-spread-contracts-tier-1.csv"
        df_contract = loader.get_model(
            file=file_s_c,
            prefix=self.model_location,
            datasource_label=datasource_label,
            date=date,
        )
        self.set_model_contracts(df_contract)

        file_ob_d = f"{date}-order-book-distribution-tier-1.csv"
        df_distri = loader.get_model(
            file=file_ob_d,
            prefix=self.model_location,
            datasource_label=datasource_label,
            date=date,
        )
        self.set_model_distribution(df_distri)

    def set_model_depth(self, df: pd.DataFrame) -> pd.DataFrame:
        self.model["orderbook_pip_depth"] = df
        return None

    def parse_model_contracts(self, df: pd.DataFrame) -> pd.DataFrame:
        df["spread"] = df["spread"] * 1000000
        df["quantity"] = round(df["quantity"] * 100)
        return df

    def set_model_contracts(self, df: pd.DataFrame):
        df = self.parse_model_contracts(df)
        self.model["spread_contracts"] = df
        return None

    def set_model_distribution(self, df: pd.DataFrame) -> pd.DataFrame:
        self.model["orderbook_distribution"] = df
        return None

    def get_model_views(self, instrument, spread):
        ob_distribution = (
            self.model["orderbook_distribution"]
            .loc[self.model["orderbook_distribution"]["instrument_id"] == instrument]
            .sort_values("pips_order", ascending=True)
            .reset_index(drop=True)
            .copy()
        )

        ob_pip_depth = (
            self.model["orderbook_pip_depth"]
            .loc[self.model["orderbook_pip_depth"]["instrument_id"] == instrument]
            .sort_values("pips_order", ascending=True)
            .reset_index(drop=True)
            .copy()
        )

        spread_contracts = (
            self.model["spread_contracts"]
            .loc[self.model["spread_contracts"]["instrument_id"] == instrument]
            .copy()
        )

        try:
            contracts_at_spread = spread_contracts.loc[
                spread_contracts["spread"] == spread
                ]["quantity"].iloc[0]
        except IndexError:
            if np.sign(spread) >= 1:
                spread_contracts["spread_difference"] = spread_contracts["spread"].map(
                    lambda x: spread - x
                )
            else:
                spread_contracts["spread_difference"] = spread_contracts["spread"].map(
                    lambda x: x - spread
                )
            closest_spread = spread_contracts["spread_difference"].min()
            contracts_at_spread = spread_contracts.loc[
                spread_contracts["spread_difference"] == closest_spread
                ]["quantity"].iloc[0]

        return ob_distribution, ob_pip_depth, contracts_at_spread

    def predict(self, event, order, tob_quantity, spread):
        """ Predicts an order fill below TOB based on distributions from previous slipped trades

        Parameters:
        event: event
        order: order simulating fill for
        tob_quantity: TOB contract quantity
        spread: order book spread at time of trade

        Returns:
        prices: list of prices each trade occurred at
        trade_qtys: list of contract quantities each trade occurred at
        """
        op = operator.add if np.sign(order.unfilled_qty) == 1 else operator.sub

        trades = []

        # create first trade at TOB
        trades.append(self.create_trade(event, order, order.price, tob_quantity))

        # load the model distribution model views for the order instrument
        ob_distribution, ob_pip_depth, contracts_at_spread = self.get_model_views(
            order.order_book_id, spread
        )

        # set max number of levels that can be consumed
        max_pips = ob_distribution["pips_order"].max()

        level = 1
        while order.unfilled_qty != 0 or level <= max_pips:
            # if there is not prediction for the next level then just assume each level slips 1 pip at spread quantity
            try:
                pip_depth = ob_pip_depth["pips_diff"].iloc[level]
                contracts_scaling = ob_distribution["contracts_scaled"].iloc[level]

            except IndexError:
                pip_depth = level
                contracts_scaling = 1

            contracts_pips = int(round(contracts_at_spread * contracts_scaling))

            if level == max_pips:
                contracts_pips = abs(order.unfilled_qty)

            # order consumes all liquidity at level
            if contracts_pips < abs(order.unfilled_qty):
                fill_price = op(
                    order.price, (pip_depth * event.price_increment) * 1000000
                )
                fill_qty = contracts_pips * np.sign(order.unfilled_qty)
                trades.append(self.create_trade(event, order, fill_price, fill_qty))

            # order now fully filled
            elif contracts_pips >= abs(order.unfilled_qty):
                fill_price = op(
                    order.price, (pip_depth * event.price_increment) * 1000000
                )
                trades.append(
                    self.create_trade(event, order, fill_price, order.unfilled_qty)
                )
                break

            level += 1

        return trades

    def match_order(self, event, order):

        if not order.price:
            order.price = event.get_tob_price(
                not order.is_long, match=self.matching_method, standardised=True
            )

        if order.order_type == "S":
            tob_qty = event.get_tob_quantity(order.is_long, standardised=True)

            # if order consumes more than tob then simulate slippage
            if abs(order.unfilled_qty) > abs(tob_qty):

                trades = self.predict(
                    event=event,
                    order=order,
                    tob_quantity=tob_qty,
                    spread=(event.ask_price - event.bid_price),
                )

            else:  # else fill order at TOB
                trade_qty = order.unfilled_qty
                trades = [
                    self.create_trade(
                        event, order, price=order.price, trade_qty=trade_qty
                    )
                ]

        elif order.order_type == "R":
            trade_qty = min(
                order.order_qty,
                event.get_tob_quantity(order.is_long, standardised=True),
            )

            trades = [
                self.create_trade(event, order, price=order.price, trade_qty=trade_qty)
            ]

        else:
            trade_qty = order.order_qty

            trades = [
                self.create_trade(event, order, price=order.price, trade_qty=trade_qty)
            ]

        if order.time_in_force == "K":
            order.cancelled = 1

        return trades

    @staticmethod
    def create_trade(event, order, price: int, trade_qty: int):

        trade = Event(
            order_book_id=event.order_book_id,
            unit_price=event.unit_price,
            symbol=event.symbol,
            currency=event.currency,
            contract_unit_of_measure=event.contract_unit_of_measure,
            price_increment=event.price_increment,
            timestamp=event.timestamp,
            account_id=order.account_id,
            counterparty_account_id=event.account_id,
            contract_qty=trade_qty,
            order_qty=order.order_qty,
            price=price,
            event_type=order.event_type,
            ask_price=event.ask_price,
            ask_qty=event.ask_qty,
            bid_price=event.bid_price,
            bid_qty=event.bid_qty,
            venue=event.venue,
            rate_to_usd=event.rate_to_usd,
            trading_session=event.trading_session,
        )

        order.unfilled_qty -= trade_qty

        return trade
