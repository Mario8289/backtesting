from numpy import sign
from .position import Position


class Portfolio:
    def __init__(
            self,
            cash=0,
            netting_engine="fifo",
            matching_method="mid",
            currency="USD",
            calc_upnl=False,
    ):
        self.init_cash = cash
        self.cur_cash = cash
        self.currency = currency
        self.positions = {}
        self.closed_positions = {}
        self.equity = cash
        self.realised_pnl = 0
        self.unrealised_pnl = 0
        self.total_net_position = 0
        self.inventory_contracts = {}
        self.inventory_dollars = {}
        """
        use if decide to seperate bid/ask from event in order to handle
        events that trigger a signal on different instrument. this would
        then be used as lookup with symbol_id key
        """
        self.price_handler = None
        self.netting_engine = netting_engine
        self.matching_method = matching_method
        self.calc_upnl = calc_upnl

    def calc_net_of_account_positions_for_symbol(self, symbol_id, venue):
        net_positions = []
        for p, open_positions in {
            k: v
            for (k, v) in self.positions.items()
            if k[0] == venue and k[1] == symbol_id
        }.items():

            venue, symbol_id, account = p[0], p[1], p[2]
            pos = self.positions[venue, symbol_id, account]
            net_positions.append(pos.calculate_net_contracts())

        return net_positions

    def update_portfolio(self, event):
        self.unrealised_pnl = 0
        if len(self.positions) != 0:
            for p, open_positions in {
                k: v
                for (k, v) in self.positions.items()
                if k[-2] == event.symbol_id
            }.items():
                source, symbol_id, account = p[0], p[1], p[2]
                pos = self.positions[source, symbol_id, account]

                if event.has_price:
                    evt_price = event.get_price(
                        is_long=pos.is_long(),
                        matching_method=self.matching_method,
                    )
                    pos.update_unrealised_pnl(evt_price, event.rate_to_usd)
                    self.unrealised_pnl += pos.unrealised_pnl
        else:
            pass

    def modify_position(
            self,
            trade,
            event
    ):

        if (trade.source, trade.symbol_id, trade.account_id) in self.positions:
            pos = self.positions[trade.source, trade.symbol_id, trade.account_id]
            realised_pnl = pos.on_trade(
                trade.contract_qty, trade.price, trade.rate_to_usd
            )
            self.realised_pnl += realised_pnl
            self.equity = self.realised_pnl
            self.equity += self.init_cash
            self.total_net_position += trade.contract_qty

            self.inventory_contracts[event.contract_unit_of_measure] = (
                    self.inventory_contracts.get(event.contract_unit_of_measure, 0)
                    + trade.contract_qty
            )
            self.inventory_dollars[
                event.contract_unit_of_measure
            ] = self.inventory_dollars.get(event.contract_unit_of_measure, 0) + (
                    (trade.contract_qty * trade.price * event.contract_size)
                    * trade.rate_to_usd
            )

            if self.calc_upnl:
                self.update_portfolio(trade)

            if pos.net_position == 0:
                self.closed_positions[
                    trade.source, trade.symbol_id, trade.account_id
                ] = pos
                self.positions.pop((event.source, event.symbol_id, trade.account_id))

        else:
            pass

    def get_positions_for_account(self, account_id):
        return {k: v for (k, v) in self.positions.items() if k[2] == account_id}

    def get_position(self, trade):
        # load previously closed position and reset the exit attr used to monitor position
        pos = self.closed_positions[trade.symbol_id, trade.account_id]
        pos.exit_attr = dict()
        return pos

    def add_position(
            self,
            trade,
            event
    ):
        if (trade.symbol_id, trade.account_id) not in self.positions:
            if (
                    trade.symbol_id,
                    trade.account_id,
            ) in self.closed_positions:
                pos = self.get_position(trade)
                self.closed_positions.pop(
                    (trade.symbol_id, trade.account_id)
                )
            else:
                pos = Position(
                    trade.symbol,
                    event.contract_size,
                    event.price_increment,
                    currency=event.currency,
                    contract_unit_of_measure=event.contract_unit_of_measure,
                    netting_engine=self.netting_engine,
                )
            self.total_net_position += trade.contract_qty
            self.inventory_contracts[event.contract_unit_of_measure] = (
                    self.inventory_contracts.get(event.contract_unit_of_measure, 0)
                    + trade.contract_qty
            )

            self.inventory_dollars[
                event.contract_unit_of_measure
            ] = self.inventory_dollars.get(event.contract_unit_of_measure, 0) + (
                    (trade.contract_qty * trade.price * event.contract_size)
                    * event.rate_to_usd
            )

            self.positions[event.source, trade.symbol_id, trade.account_id] = pos
            pos.on_trade(trade.contract_qty, trade.price, event.rate_to_usd)

            if self.calc_upnl:
                self.update_portfolio(trade)

        else:
            pass

    def on_trade(
            self,
            trade,
            event,
            commission=0
    ):
        if sign(trade.contract_qty) == 1:  # long
            self.cur_cash -= (
                                     (trade.contract_qty * event.contract_size * trade.price) * event.rate_to_usd
                             ) + commission
        else:  # short
            self.cur_cash += (
                                     (trade.contract_qty * event.contract_size * trade.price) * event.rate_to_usd
                             ) - commission
        if (event.source, event.symbol_id, trade.account_id) not in self.positions:
            self.add_position(trade, event)
        else:
            self.modify_position(trade, event)
