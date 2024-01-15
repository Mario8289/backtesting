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
        then be used as lookup with order_book_id key
        """
        self.price_handler = None
        self.netting_engine = netting_engine
        self.matching_method = matching_method
        self.calc_upnl = calc_upnl

    def calc_net_of_account_positions_for_symbol(self, order_book_id, venue):
        net_positions = []
        for p, open_positions in {
            k: v
            for (k, v) in self.positions.items()
            if k[0] == venue and k[1] == order_book_id
        }.items():

            venue, order_book_id, account = p[0], p[1], p[2]
            pos = self.positions[venue, order_book_id, account]
            net_positions.append(pos.calculate_net_contracts())

        return net_positions

    def update_portfolio(self, event):
        self.unrealised_pnl = 0
        if len(self.positions) != 0:
            for p, open_positions in {
                k: v
                for (k, v) in self.positions.items()
                if k[-2] == event.order_book_id
            }.items():
                venue, order_book_id, account = p[0], p[1], p[2]
                pos = self.positions[venue, order_book_id, account]

                if event.ask_price != 0 or event.bid_price != 0:
                    evt_price = event.get_tob_price(
                        is_long=pos.is_long(),
                        match=self.matching_method,
                        standardised=True,
                    )
                    pos.update_unrealised_pnl(evt_price, event.rate_to_usd)
                    self.unrealised_pnl += pos.unrealised_pnl
        else:
            pass

    def modify_position(self, event):

        if (event.venue, event.order_book_id, event.account_id) in self.positions:
            pos = self.positions[event.venue, event.order_book_id, event.account_id]
            realised_pnl = pos.on_trade(
                event.contract_qty, event.price, event.rate_to_usd
            )
            self.realised_pnl += realised_pnl
            self.equity = self.realised_pnl
            self.equity += self.init_cash
            self.total_net_position += event.contract_qty

            self.inventory_contracts[event.contract_unit_of_measure] = (
                    self.inventory_contracts.get(event.contract_unit_of_measure, 0)
                    + event.contract_qty
            )
            self.inventory_dollars[
                event.contract_unit_of_measure
            ] = self.inventory_dollars.get(event.contract_unit_of_measure, 0) + (
                    (event.contract_qty * event.price * event.unit_price)
                    * event.rate_to_usd
            )

            if self.calc_upnl:
                self.update_portfolio(event)

            if pos.net_position == 0:
                self.closed_positions[
                    event.venue, event.order_book_id, event.account_id
                ] = pos
                self.positions.pop((event.venue, event.order_book_id, event.account_id))

        else:
            pass

    def get_positions_for_account(self, account_id):
        return {k: v for (k, v) in self.positions.items() if k[2] == account_id}

    def get_position(self, event):
        # load previously closed position and reset the exit attr used to monitor position
        pos = self.closed_positions[event.venue, event.order_book_id, event.account_id]
        pos.exit_attr = dict()
        return pos

    def add_position(self, event):
        if (event.venue, event.order_book_id, event.account_id) not in self.positions:
            if (
                    event.venue,
                    event.order_book_id,
                    event.account_id,
            ) in self.closed_positions:
                pos = self.get_position(event)
                self.closed_positions.pop(
                    (event.venue, event.order_book_id, event.account_id)
                )
            else:
                pos = Position(
                    event.symbol,
                    event.unit_price,
                    event.price_increment,
                    currency=event.currency,
                    contract_unit_of_measure=event.contract_unit_of_measure,
                    netting_engine=self.netting_engine,
                )
            self.total_net_position += event.contract_qty
            self.inventory_contracts[event.contract_unit_of_measure] = (
                    self.inventory_contracts.get(event.contract_unit_of_measure, 0)
                    + event.contract_qty
            )

            self.inventory_dollars[
                event.contract_unit_of_measure
            ] = self.inventory_dollars.get(event.contract_unit_of_measure, 0) + (
                    (event.contract_qty * event.price * event.unit_price)
                    * event.rate_to_usd
            )

            self.positions[event.venue, event.order_book_id, event.account_id] = pos
            pos.on_trade(event.contract_qty, event.price, event.rate_to_usd)

            if self.calc_upnl:
                self.update_portfolio(event)

        else:
            pass

    def on_trade(self, event, commission=0):
        if event.is_long:
            self.cur_cash -= (
                                     ((event.contract_qty * event.unit_price * event.price) / 100000000)
                                     * event.rate_to_usd
                             ) + commission
        else:
            self.cur_cash += (
                                     ((event.contract_qty * event.unit_price * event.price) / 100000000)
                                     * event.rate_to_usd
                             ) - commission
        if (event.venue, event.order_book_id, event.account_id) not in self.positions:
            self.add_position(event)
        else:
            self.modify_position(event)
