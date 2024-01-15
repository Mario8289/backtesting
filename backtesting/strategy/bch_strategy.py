import logging
from typing import Tuple, List

import numpy as np
from math import isclose

from ..event import Event
from ..exit_strategy.base import AbstractExitStrategy
from ..exit_strategy.exit_default import ExitDefault
from ..order import Order
from ..strategy.base import AbstractStrategy


class BCHStrategy(AbstractStrategy):
    __output_slots__ = [
        "min_directional_consensus",
        "min_consensus_buffer_factor",
        "max_ratio_per_position",
        "max_ratio_buffer_factor",
        "position_trigger",
        "position_buffer_factor",
        "risk_ratio",
        "exit_strategy",
        "follow_client",
    ]

    __slots__ = __output_slots__ + [
        "account_id",
        "position_trigger_standardised",
        "position_buffer_standardised",
    ]

    def __init__(
            self,
            account_id: int,
            min_directional_consensus: float = 0.7,
            min_consensus_buffer_factor: float = 0.8,
            max_ratio_per_position: float = 0.6,
            max_ratio_buffer_factor: float = 1.2,
            position_trigger: int = 250,
            position_buffer_factor: float = 0.9,
            follow_client: bool = False,
            risk_ratio: float = 0.2,
            exit_strategy: AbstractExitStrategy = ExitDefault(),
    ):
        self.name: str = "bch"
        self.account_id: int = account_id
        self.min_directional_consensus: float = min_directional_consensus
        self.min_consensus_buffer_factor: float = min_consensus_buffer_factor
        self.min_consensus_buffer: float = (
                min_directional_consensus * min_consensus_buffer_factor
        )
        self.max_ratio_per_position: float = max_ratio_per_position
        self.max_ratio_buffer_factor: float = max_ratio_buffer_factor
        self.max_ratio_buffer: float = (
                max_ratio_per_position * max_ratio_buffer_factor
        )
        self.position_trigger: int = position_trigger
        self.position_trigger_standardised: int = position_trigger * 100
        self.position_buffer_factor: float = position_buffer_factor
        self.position_buffer_standardised: float = (
                                                           position_trigger * 100
                                                   ) * position_buffer_factor
        self.risk_ratio: float = risk_ratio
        self.follow_client: bool = follow_client
        self.prev_pos_dir: int = 0
        self.exit_strategy: AbstractExitStrategy = exit_strategy
        self.active = True

    def profitloss_price(
            self, price, price_inc_level, stoploss_limit, takeprofit_limit, tp_op, sl_op
    ) -> Tuple[float, float]:
        sl_price = sl_op(price, ((price_inc_level * 1000000) * stoploss_limit))
        tp_price = tp_op(price, ((price_inc_level * 1000000) * takeprofit_limit))
        return sl_price, tp_price

    def get_signal_ratio(self, position_quantities) -> Tuple[bool, bool]:
        position_quantities_in_direction: List[int] = [
            x
            for x in position_quantities
            if np.sign(x) == np.sign(sum(position_quantities))
        ]
        position_ratios: List[int] = (
            [
                p / sum(position_quantities_in_direction)
                for p in position_quantities_in_direction
            ]
            if len(position_quantities) > 1
            else [1]
        )
        sig_r: bool = not any(r > self.max_ratio_per_position for r in position_ratios)
        sig_r_buf: bool = not any(r > self.max_ratio_buffer for r in position_ratios)
        return sig_r, sig_r_buf

    def get_signal_consensus(
            self, client_position, position_quantities
    ) -> Tuple[bool, bool]:
        position_is_long: List[bool] = [
            True if np.sign(x) == 1 else False for x in position_quantities
        ]
        dir: bool = True if np.sign(client_position) == 1 else False
        sig_con: bool = (
                position_is_long.count(dir) / len(position_is_long)
                > self.min_directional_consensus
        )
        sig_buf: bool = position_is_long.count(dir) / len(position_is_long) > (
            self.min_consensus_buffer
        )
        return sig_con, sig_buf

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

        if not event.untrusted:
            orders = self.exit_strategy.generate_exit_order_signal(
                event=event,
                account=self.account_id,
                avg_price=avg_price,
                tick_price=price_tick,
                position=position,
            )
            if orders:
                self.active: bool = False

        return orders

    def calculate_trade_signals(
            self, client_portfolio, lmax_portfolio, event: Event
    ) -> List[Order]:

        logger: logging.Logger = logging.getLogger("BCH TRADE SIGNALS")

        position_quantities = client_portfolio.calc_net_of_account_positions_for_symbol(
            event.order_book_id, event.venue
        )
        client_position = sum(position_quantities)

        lmax_position = sum(
            lmax_portfolio.calc_net_of_account_positions_for_symbol(
                event.order_book_id, event.venue
            )
        )

        # ratios
        signal_ratio, signal_ratio_buf = self.get_signal_ratio(position_quantities)

        # concensus
        signal_dir_consensus, signal_dir_consensus_buf = self.get_signal_consensus(
            client_position, position_quantities
        )

        # position
        position_signal_buf = self.get_position_signal_buffer(
            client_position, position_quantities
        )

        logger.debug(
            f"TRADE: {event.contract_qty}, TRIGGER: {self.position_buffer_standardised}, CLIENT POS: {client_position}, "
            f"LMAX POS: {lmax_position}, ACTIVE: {self.active}, NET: {abs(client_position) >= self.position_buffer_standardised}, "
            f"CONCEN: {signal_dir_consensus}, RATIO: {signal_ratio}, FLAT: {isclose(lmax_position, 0, abs_tol=1e-8)}, "
            f"BUFS: {position_signal_buf, signal_dir_consensus_buf, signal_ratio_buf}"
        )

        # Signal Triggers
        orders: List[Order] = []

        # after a close only actively open new position when one of the signals are outside buffer range
        if self.is_active(
                position_signal_buf, signal_dir_consensus_buf, signal_ratio_buf
        ):
            # open position
            if (
                    abs(client_position) >= self.position_buffer_standardised
                    and self.active
                    and signal_dir_consensus
                    and signal_ratio
                    and isclose(lmax_position, 0, abs_tol=1e-8)
            ):
                trade_size = self.get_open_trade_size(client_position)
                trade_signal = self.get_trade_signal("open", trade_size)

                logger.debug(
                    f"OPEN: client position {client_position}, follow_client {self.follow_client}, trade: {trade_size}"
                )

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
                        signal=trade_signal,
                        event_type="internal",
                    )
                )

            # close position
            if not isclose(lmax_position, 0, abs_tol=1e-8) and (
                    (abs(client_position) < round(self.position_buffer_standardised))
                    or not signal_ratio_buf
                    or not signal_dir_consensus_buf
            ):
                trade_size = self.get_close_trade_size(lmax_position)
                trade_signal = self.get_trade_signal("close", lmax_position)

                logger.debug(
                    f"CLOSE: client position {client_position}, follow_client {self.follow_client}, trade: {trade_size}"
                )

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
                        signal=trade_signal,
                        event_type="internal",
                    )
                )
                self.active = False

        return orders

    def get_close_trade_size(self, lmax_position):
        return lmax_position * -1

    @staticmethod
    def get_trade_signal(type, trade_size):
        return f"{type}_short_pos" if np.sign(trade_size) == -1 else f"{type}_long_pos"

    def get_open_trade_size(self, client_position):
        client_direction = np.sign(client_position)
        trade_direction = (
            client_direction if self.follow_client else client_direction * -1
        )
        trade = (self.position_trigger_standardised * self.risk_ratio) * trade_direction
        return trade

    def get_position_signal_buffer(self, client_position, position_quantities):
        if np.sign(sum(position_quantities)) == 1:
            position_signal_buf: bool = (
                True
                if client_position > round(self.position_buffer_standardised)
                else False
            )
        else:  # if np.sign(sum(position_quantities)) == -1:
            # todo: position quantities can't be 0, can it? ... if it is, we probably want it to fail anyway so we can figure out wth is happening
            position_signal_buf: bool = (
                True
                if client_position < round(self.position_buffer_standardised) * -1
                else False
            )
        return position_signal_buf

    def is_active(
            self, position_signal_buf, signal_dir_consensus_buf, signal_ratio_buf
    ) -> bool:

        if self.active:
            return self.active
        else:
            if all([position_signal_buf, signal_ratio_buf, signal_dir_consensus_buf,]):
                self.active = True

            return self.active

    def on_state(self, client_portfolio, lmax_portfolio, event: Event) -> List[Order]:
        if len(client_portfolio.positions) != 0:
            if event.event_type == "trade":
                return self.calculate_trade_signals(
                    client_portfolio, lmax_portfolio, event
                )
            elif event.event_type == "market_data":
                return self.calculate_market_signals(lmax_portfolio, event)
            else:
                return []
        else:
            return []

    def get_name(self) -> str:
        return self.name
