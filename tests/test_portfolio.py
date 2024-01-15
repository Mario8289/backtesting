import unittest

from risk_backtesting.event import Event
from risk_backtesting.portfolio import Portfolio


class TestPortfolioFifoLifo(unittest.TestCase):
    def setUp(self):
        self.portfolio = Portfolio(
            cash=0,
            netting_engine="fifo",
            matching_method="side_of_book",
            currency="USD",
            calc_upnl=True,
        )

    def test_open_first_position_for_symbol_EURUSD(self):
        event = Event(
            timestamp=1,
            order_book_id=1,
            event_type="trade",
            account_id=2,
            contract_qty=0.1 * 100,
            price=1.11111 * 1000000,
            bid_price=1.11110 * 1000000,
            ask_price=1.11113 * 1000000,
            bid_qty=1,
            ask_qty=1,
            unit_price=1000,
            symbol="EUR/USD",
            currency="USD",
            price_increment=0.00001,
            rate_to_usd=1,
        )
        self.portfolio.on_trade(event)
        expected = 1
        result = self.portfolio.positions.__len__()
        self.assertEqual(expected, result)

    def test_inventory_for_multiple_instruments_longs(self):
        events = [
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11111,
                    "bid_price": 1.11110,
                    "currency": "USD",
                    "contract_unit_of_measure": "EUR",
                    "account_id": 1,
                    "ask_price": 1.11112,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.905,
                    "bid_price": 108.904,
                    "currency": "JPY",
                    "contract_unit_of_measure": "USD",
                    "account_id": 1,
                    "ask_price": 108.906,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11113,
                    "bid_price": 1.11112,
                    "currency": "USD",
                    "contract_unit_of_measure": "EUR",
                    "account_id": 1,
                    "ask_price": 1.11114,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.904,
                    "bid_price": 108.903,
                    "currency": "JPY",
                    "contract_unit_of_measure": "USD",
                    "account_id": 1,
                    "ask_price": 108.905,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
        ]

        for event in events:
            for symbol, event_details in event.items():
                event = Event(
                    timestamp=1,
                    order_book_id=event_details["order_book_id"],
                    event_type="trade",
                    account_id=event_details["account_id"],
                    contract_qty=0.1 * 100,
                    price=event_details["price"] * 1000000,
                    bid_price=event_details["bid_price"] * 1000000,
                    ask_price=event_details["ask_price"] * 1000000,
                    bid_qty=1,
                    ask_qty=-1,
                    unit_price=10000,
                    symbol=symbol,
                    currency=event_details["currency"],
                    contract_unit_of_measure=event_details["contract_unit_of_measure"],
                    price_increment=event_details["price_increment"],
                    rate_to_usd=event_details["rate_to_usd"],
                )

                self.portfolio.on_trade(event)

        self.assertEqual({"EUR": 20, "USD": 20}, self.portfolio.inventory_contracts)
        self.assertEqual(
            {"EUR": 222224000000, "USD": 200384280000}, self.portfolio.inventory_dollars
        )

    def test_inventory_for_multiple_instruments_mixed(self):
        events = [
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11111,
                    "bid_price": 1.11110,
                    "currency": "USD",
                    "contract_unit_of_measure": "EUR",
                    "account_id": 1,
                    "contract_qty": 0.1,
                    "ask_price": 1.11112,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.905,
                    "bid_price": 108.904,
                    "currency": "JPY",
                    "contract_unit_of_measure": "USD",
                    "account_id": 1,
                    "contract_qty": -0.1,
                    "ask_price": 108.906,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11113,
                    "bid_price": 1.11112,
                    "currency": "USD",
                    "contract_unit_of_measure": "EUR",
                    "account_id": 1,
                    "contract_qty": -0.1,
                    "ask_price": 1.11114,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.904,
                    "bid_price": 108.903,
                    "currency": "JPY",
                    "contract_unit_of_measure": "USD",
                    "account_id": 1,
                    "contract_qty": 0.1,
                    "ask_price": 108.905,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
        ]

        for event in events:
            for symbol, event_details in event.items():
                event = Event(
                    timestamp=1,
                    order_book_id=event_details["order_book_id"],
                    event_type="trade",
                    account_id=event_details["account_id"],
                    contract_qty=event_details["contract_qty"] * 1e2,
                    price=event_details["price"] * 1000000,
                    bid_price=event_details["bid_price"] * 1000000,
                    ask_price=event_details["ask_price"] * 1000000,
                    bid_qty=1,
                    ask_qty=-1,
                    unit_price=10000,
                    symbol=symbol,
                    currency=event_details["currency"],
                    contract_unit_of_measure=event_details["contract_unit_of_measure"],
                    price_increment=event_details["price_increment"],
                    rate_to_usd=event_details["rate_to_usd"],
                )

                self.portfolio.on_trade(event)

        self.assertEqual({"EUR": 0, "USD": 0}, self.portfolio.inventory_contracts)
        self.assertEqual(
            {"EUR": -2000000, "USD": -920000}, self.portfolio.inventory_dollars
        )

    def test_open_single_position_from_one_account_on_two_symbols(self):
        events = [
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11111,
                    "bid_price": 1.11110,
                    "currency": "USD",
                    "ask_price": 1.11112,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.905,
                    "bid_price": 108.904,
                    "currency": "JPY",
                    "ask_price": 108.906,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
        ]
        for event in events:
            for symbol, event_details in event.items():
                event = Event(
                    timestamp=1,
                    order_book_id=event_details["order_book_id"],
                    event_type="trade",
                    account_id=2,
                    contract_qty=0.1 * 100,
                    price=event_details["price"] * 1000000,
                    bid_price=event_details["bid_price"] * 1000000,
                    ask_price=event_details["ask_price"] * 1000000,
                    bid_qty=1,
                    ask_qty=1,
                    unit_price=10000,
                    symbol=symbol,
                    currency=event_details["currency"],
                    price_increment=event_details["price_increment"],
                    rate_to_usd=event_details["rate_to_usd"],
                )

                self.portfolio.on_trade(event)

        # test that 2 positions were created
        self.assertEqual(2, self.portfolio.positions.__len__())

        # test that unrealised pnl is only updated for position of the same symbol

        EURUSD_position = self.portfolio.positions[None, 1, 2]
        self.assertEqual(EURUSD_position.unrealised_pnl, -0.01)
        self.assertEqual(EURUSD_position.no_of_trades, 1)
        self.assertEqual(EURUSD_position.realised_pnl, 0)

        USDJPY_position = self.portfolio.positions[None, 2, 2]
        self.assertEqual(USDJPY_position.unrealised_pnl, -0.0092)
        self.assertEqual(USDJPY_position.no_of_trades, 1)
        self.assertEqual(USDJPY_position.realised_pnl, 0)

    def test_open_multiple_position_from_one_account_on_two_symbols(self):

        events = [
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11111,
                    "bid_price": 1.11110,
                    "currency": "USD",
                    "account_id": 1,
                    "ask_price": 1.11112,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.905,
                    "bid_price": 108.904,
                    "currency": "JPY",
                    "account_id": 1,
                    "ask_price": 108.906,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11113,
                    "bid_price": 1.11112,
                    "currency": "USD",
                    "account_id": 1,
                    "ask_price": 1.11114,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.904,
                    "bid_price": 108.903,
                    "currency": "JPY",
                    "account_id": 1,
                    "ask_price": 108.905,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
        ]

        for event in events:
            for symbol, event_details in event.items():
                event = Event(
                    timestamp=1,
                    order_book_id=event_details["order_book_id"],
                    event_type="trade",
                    account_id=event_details["account_id"],
                    contract_qty=0.1 * 100,
                    price=event_details["price"] * 1000000,
                    bid_price=event_details["bid_price"] * 1000000,
                    ask_price=event_details["ask_price"] * 1000000,
                    bid_qty=1,
                    ask_qty=-1,
                    unit_price=10000,
                    symbol=symbol,
                    currency=event_details["currency"],
                    price_increment=event_details["price_increment"],
                    rate_to_usd=event_details["rate_to_usd"],
                )

                self.portfolio.on_trade(event)

        # test that 2 positions were created
        self.assertEqual(2, self.portfolio.positions.__len__())

        # test that unrealised pnl is only updated for position of the same symbol

        EURUSD_position = self.portfolio.positions[None, 1, 1]
        self.assertEqual(
            0, EURUSD_position.unrealised_pnl
        )  # as pos 1 in 1 tick prof, pos 2 in 1 tick loss
        self.assertEqual(2, EURUSD_position.no_of_trades)
        self.assertEqual(0, EURUSD_position.realised_pnl)

        USDJPY_position = self.portfolio.positions[None, 2, 1]
        self.assertEqual(
            -0.0276, USDJPY_position.unrealised_pnl
        )  # as pos 1 in 2 tick in loss, pos 2 in 1 tick loss
        self.assertEqual(2, USDJPY_position.no_of_trades)
        self.assertEqual(0, USDJPY_position.realised_pnl)

    def test_net_after_adding_position(self):
        accounts = [1, 2]
        symbol = 1
        event = Event(
            timestamp=1,
            order_book_id=1,
            ask_price=1 * 1000000,
            ask_qty=1,
            bid_price=1 * 1000000,
            bid_qty=1,
            event_type="trade",
            account_id=1,
            contract_qty=0.1 * 100,
            price=1 * 1000000,
            unit_price=10000,
            rate_to_usd=1,
            symbol="testsymbol",
            price_increment=0.00001,
        )
        for account in accounts:
            event.account_id = int(account)
            self.portfolio.on_trade(event)

        event.order_book_id = 2
        self.portfolio.on_trade(event)

        expected = 20
        result = sum(
            self.portfolio.calc_net_of_account_positions_for_symbol(symbol, None)
        )
        self.assertEqual(expected, result)

    def test_unrealsed_pnl_for_market_data_event(self):
        timestamps = [2, 3, 4]
        bid_ask = [
            [1.00001, 1.00003],
            [1.00001, 1.00003],
            [1.00003, 1.00005],
        ]  # bid ask pairs
        market_events = zip(timestamps, bid_ask)

        event = Event(
            timestamp=1,
            order_book_id=1,
            price=1.00001 * 1000000,
            ask_price=1.00002 * 1000000,
            ask_qty=1,
            bid_price=1.00000 * 1000000,
            bid_qty=1,
            event_type="trade",
            account_id=2,
            contract_qty=0.1 * 100,
            unit_price=10000,
            rate_to_usd=1,
            symbol="testsymbol",
            price_increment=0.00001,
        )

        self.portfolio.on_trade(event)

        event = Event(
            timestamp=1,
            order_book_id=2,
            price=1.00001 * 1000000,
            ask_price=1.00001 * 1000000,
            ask_qty=1,
            bid_price=1.00001 * 1000000,
            bid_qty=1,
            event_type="trade",
            account_id=2,
            contract_qty=0.1 * 100,
            unit_price=10000,
            rate_to_usd=1,
            symbol="testsymbol",
            price_increment=0.00001,
        )

        self.portfolio.on_trade(event)

        for evt in market_events:
            event = Event(
                timestamp=evt[0],
                order_book_id=1,
                ask_price=evt[1][1] * 1000000,
                ask_qty=1,
                bid_price=evt[1][0] * 1000000,
                bid_qty=1,
                event_type="market_event",
                account_id=0,
                contract_qty=0.1 * 100,
                price=0 * 1000000,
                unit_price=10000,
                rate_to_usd=1,
                symbol="testsymbol",
                price_increment=0.00001,
            )

            self.portfolio.update_portfolio(event)

        self.assertEqual(0.02, self.portfolio.unrealised_pnl)
        self.assertEqual(0.02, self.portfolio.positions[None, 1, 2].unrealised_pnl)
        self.assertEqual(0, self.portfolio.positions[None, 2, 2].unrealised_pnl)

    def test_realised_pnl_for_portfolio_from_two_positions(self):
        portfolio = Portfolio(calc_upnl=True)
        timestamps = [1, 2, 1, 2]
        prices = [1.00000, 1.00003, 1.00000, 1.00003]
        accounts = [1, 2, 1, 2]
        quantities = [0.1, -0.1, 0.1, -0.1]
        tnps = zip(timestamps, prices, accounts, quantities)

        for tnp in tnps:
            event = Event(
                timestamp=tnp[0],
                order_book_id=1,
                ask_price=1.000000 * 1000000,
                ask_qty=1,
                bid_price=1.00000 * 1000000,
                bid_qty=1,
                event_type="trade",
                account_id=tnp[2],
                contract_qty=tnp[3] * 100,
                price=tnp[1] * 1000000,
                rate_to_usd=1,
                unit_price=10000,
                symbol="hi",
                price_increment=0.00001,
            )

            portfolio.on_trade(event)

        expected = 0.06
        result = round(portfolio.unrealised_pnl, 2)
        self.assertEqual(expected, result)

    def test_close_position_with_net_zero(self):
        timestamps = [1, 2]
        prices = [1.0, 1.3]
        accounts = [1, 1]
        quantities = [0.1, -0.1]
        tnps = zip(timestamps, prices, accounts, quantities)

        for tnp in tnps:
            event = Event(
                timestamp=tnp[0],
                order_book_id=1,
                ask_price=1 * 1000000,
                ask_qty=1,
                bid_price=1 * 1000000,
                bid_qty=1,
                event_type="trade",
                account_id=tnp[2],
                contract_qty=tnp[3] * 100,
                price=tnp[1] * 1000000,
                unit_price=10000,
                symbol="sname",
                price_increment=0.00001,
                rate_to_usd=1,
            )

            self.portfolio.on_trade(event)

        expected = 1
        result = self.portfolio.closed_positions.__len__()
        self.assertEqual(expected, result)

    def test_realised_pnl_from_non_USD_symbols(self):
        events = [
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.905,
                    "bid_price": 108.904,
                    "currency": "JPY",
                    "ask_price": 108.906,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                    "contract_qty": 0.1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.907,
                    "bid_price": 108.906,
                    "currency": "JPY",
                    "ask_price": 108.908,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                    "contract_qty": -0.1,
                }
            },
        ]
        for event in events:
            for symbol, event_details in event.items():
                event = Event(
                    timestamp=1,
                    order_book_id=event_details["order_book_id"],
                    event_type="trade",
                    account_id=2,
                    contract_qty=event_details["contract_qty"] * 100,
                    price=event_details["price"] * 1000000,
                    bid_price=event_details["bid_price"] * 1000000,
                    ask_price=event_details["ask_price"] * 1000000,
                    bid_qty=1,
                    ask_qty=1,
                    unit_price=10000,
                    symbol=symbol,
                    currency=event_details["currency"],
                    price_increment=event_details["price_increment"],
                    rate_to_usd=event_details["rate_to_usd"],
                )

                self.portfolio.on_trade(event)

        # test that 0 positions as closed after realising
        self.assertEqual(0, self.portfolio.positions.__len__())

        # test that unrealised pnl is only updated for position of the same symbol

        USDJPY_position = self.portfolio.closed_positions[None, 2, 2]
        self.assertEqual(USDJPY_position.unrealised_pnl, 0)
        self.assertEqual(USDJPY_position.no_of_trades, 2)
        self.assertEqual(USDJPY_position.realised_pnl, 0.0184)


class TestPortfolioAvgPrice(unittest.TestCase):
    def setUp(self):
        self.portfolio = Portfolio(
            cash=0,
            netting_engine="avg_price",
            matching_method="side_of_book",
            currency="USD",
            calc_upnl=True,
        )

    def test_open_first_position_for_symbol_EURUSD_avg_price(self):
        event = Event(
            timestamp=1,
            order_book_id=1,
            event_type="trade",
            account_id=2,
            contract_qty=0.1 * 100,
            price=1.11111 * 1000000,
            bid_price=1.11110 * 1000000,
            ask_price=1.11113 * 1000000,
            bid_qty=1,
            ask_qty=1,
            unit_price=1000,
            symbol="EUR/USD",
            currency="USD",
            price_increment=0.00001,
            rate_to_usd=1,
        )
        self.portfolio.on_trade(event)
        expected = 1
        result = self.portfolio.positions.__len__()
        self.assertEqual(expected, result)

    def test_open_single_position_from_one_account_on_two_symbols_avg_price(self):
        events = [
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11111,
                    "bid_price": 1.11110,
                    "currency": "USD",
                    "ask_price": 1.11112,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.905,
                    "bid_price": 108.904,
                    "currency": "JPY",
                    "ask_price": 108.906,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
        ]
        for event in events:
            for symbol, event_details in event.items():
                event = Event(
                    timestamp=1,
                    order_book_id=event_details["order_book_id"],
                    event_type="trade",
                    account_id=2,
                    contract_qty=0.1 * 100,
                    price=event_details["price"] * 1000000,
                    bid_price=event_details["bid_price"] * 1000000,
                    ask_price=event_details["ask_price"] * 1000000,
                    bid_qty=1,
                    ask_qty=1,
                    unit_price=10000,
                    symbol=symbol,
                    currency=event_details["currency"],
                    price_increment=event_details["price_increment"],
                    rate_to_usd=event_details["rate_to_usd"],
                )

                self.portfolio.on_trade(event)

        # test that 2 positions were created
        self.assertEqual(2, self.portfolio.positions.__len__())

        # test that unrealised pnl is only updated for position of the same symbol

        EURUSD_position = self.portfolio.positions[None, 1, 2]
        self.assertEqual(EURUSD_position.unrealised_pnl, -0.01)
        self.assertEqual(EURUSD_position.no_of_trades, 1)
        self.assertEqual(EURUSD_position.realised_pnl, 0)

        USDJPY_position = self.portfolio.positions[None, 2, 2]
        self.assertEqual(USDJPY_position.unrealised_pnl, -0.0092)
        self.assertEqual(USDJPY_position.no_of_trades, 1)
        self.assertEqual(USDJPY_position.realised_pnl, 0)

    def test_open_multiple_position_from_one_account_on_two_symbols_avg_price(self):
        events = [
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11111,
                    "bid_price": 1.11110,
                    "currency": "USD",
                    "account_id": 1,
                    "ask_price": 1.11112,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.905,
                    "bid_price": 108.904,
                    "currency": "JPY",
                    "account_id": 1,
                    "ask_price": 108.906,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
            {
                "EUR/USD": {
                    "order_book_id": 1,
                    "price": 1.11113,
                    "bid_price": 1.11112,
                    "currency": "USD",
                    "account_id": 1,
                    "ask_price": 1.11114,
                    "price_increment": 0.00001,
                    "rate_to_usd": 1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.904,
                    "bid_price": 108.903,
                    "currency": "JPY",
                    "account_id": 1,
                    "ask_price": 108.905,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                }
            },
        ]

        for event in events:
            for symbol, event_details in event.items():
                event = Event(
                    timestamp=1,
                    order_book_id=event_details["order_book_id"],
                    event_type="trade",
                    account_id=event_details["account_id"],
                    contract_qty=0.1 * 100,
                    price=event_details["price"] * 1000000,
                    bid_price=event_details["bid_price"] * 1000000,
                    ask_price=event_details["ask_price"] * 1000000,
                    bid_qty=1,
                    ask_qty=1,
                    unit_price=10000,
                    symbol=symbol,
                    currency=event_details["currency"],
                    price_increment=event_details["price_increment"],
                    rate_to_usd=event_details["rate_to_usd"],
                )

                self.portfolio.on_trade(event)

        # test that 2 positions were created
        self.assertEqual(2, self.portfolio.positions.__len__())

        # test that unrealised pnl is only updated for position of the same symbol

        EURUSD_position = self.portfolio.positions[None, 1, 1]
        self.assertEqual(
            0, EURUSD_position.unrealised_pnl
        )  # as pos 1 in 1 tick prof, pos 2 in 1 tick loss
        self.assertEqual(2, EURUSD_position.no_of_trades)
        self.assertEqual(0, EURUSD_position.realised_pnl)

        USDJPY_position = self.portfolio.positions[None, 2, 1]
        self.assertEqual(
            -0.0276, USDJPY_position.unrealised_pnl
        )  # as pos 1 in 2 tick in loss, pos 2 in 1 tick loss
        self.assertEqual(2, USDJPY_position.no_of_trades)
        self.assertEqual(0, USDJPY_position.realised_pnl)

    def test_net_after_adding_position_avg_price(self):
        accounts = [1, 2]
        symbol = 1
        event = Event(
            timestamp=1,
            order_book_id=1,
            ask_price=1 * 1000000,
            ask_qty=1,
            bid_price=1 * 1000000,
            bid_qty=1,
            event_type="trade",
            account_id=1,
            contract_qty=0.1 * 100,
            price=1 * 1000000,
            unit_price=10000,
            rate_to_usd=1,
            symbol="testsymbol",
            price_increment=0.00001,
        )

        for account in accounts:
            event.account_id = int(account)
            self.portfolio.on_trade(event)

        event.order_book_id = 2
        self.portfolio.on_trade(event)

        expected = 20
        result = sum(
            self.portfolio.calc_net_of_account_positions_for_symbol(symbol, None)
        )
        self.assertEqual(expected, result)

    def test_unrealsed_pnl_for_market_data_event_avg_price(self):
        timestamps = [2, 3, 4]
        bid_ask = [
            [1.00001, 1.00003],
            [1.00001, 1.00003],
            [1.00003, 1.00005],
        ]  # bid ask pairs
        market_events = zip(timestamps, bid_ask)

        event = Event(
            timestamp=1,
            order_book_id=1,
            price=1.00001 * 1000000,
            ask_price=1.00002 * 1000000,
            ask_qty=1,
            bid_price=1.00000 * 1000000,
            bid_qty=1,
            event_type="trade",
            account_id=2,
            contract_qty=0.1 * 100,
            unit_price=10000,
            rate_to_usd=1,
            symbol="testsymbol",
            price_increment=0.00001,
        )

        self.portfolio.on_trade(event)

        event = Event(
            timestamp=1,
            order_book_id=2,
            price=1.00001 * 1000000,
            ask_price=int(1.00001 * 1000000),
            ask_qty=1,
            bid_price=1.00001 * 1000000,
            bid_qty=1,
            event_type="trade",
            account_id=2,
            contract_qty=0.1 * 100,
            unit_price=10000,
            rate_to_usd=1,
            symbol="testsymbol",
            price_increment=0.00001,
        )

        self.portfolio.on_trade(event)

        for evt in market_events:
            event = Event(
                timestamp=evt[0],
                order_book_id=1,
                ask_price=evt[1][1] * 1000000,
                ask_qty=1,
                bid_price=evt[1][0] * 1000000,
                bid_qty=1,
                event_type="market_event",
                account_id=0,
                contract_qty=0.1 * 100,
                unit_price=10000,
                rate_to_usd=1,
                symbol="testsymbol",
                price_increment=0.00001,
            )

            self.portfolio.update_portfolio(event)

        self.assertEqual(0.02, self.portfolio.unrealised_pnl)
        self.assertEqual(0.02, self.portfolio.positions[None, 1, 2].unrealised_pnl)
        self.assertEqual(0, self.portfolio.positions[None, 2, 2].unrealised_pnl)

    def test_realised_pnl_for_portfolio_from_two_positions_avg_price(self):
        portfolio = Portfolio(calc_upnl=True)
        timestamps = [1, 2, 1, 2]
        prices = [1.00000, 1.00003, 1.00000, 1.00003]
        accounts = [1, 2, 1, 2]
        quantities = [0.1, -0.1, 0.1, -0.1]
        tnps = zip(timestamps, prices, accounts, quantities)

        for tnp in tnps:
            event = Event(
                timestamp=tnp[0],
                order_book_id=1,
                ask_price=1.000000 * 1000000,
                ask_qty=1,
                bid_price=1.00000 * 1000000,
                bid_qty=1,
                event_type="trade",
                account_id=tnp[2],
                contract_qty=tnp[3] * 100,
                price=tnp[1] * 1000000,
                rate_to_usd=1,
                unit_price=10000,
                symbol="hi",
                price_increment=0.00001,
            )

            portfolio.on_trade(event)

        expected = 0.06
        result = round(portfolio.unrealised_pnl, 2)
        self.assertEqual(expected, result)

    def test_close_position_with_net_zero_avg_price(self):
        timestamps = [1, 2]
        prices = [1.0, 1.3]
        accounts = [1, 1]
        quantities = [0.1, -0.1]
        tnps = zip(timestamps, prices, accounts, quantities)

        for tnp in tnps:
            event = Event(
                timestamp=tnp[0],
                order_book_id=1,
                ask_price=1 * 1000000,
                ask_qty=1,
                bid_price=1 * 1000000,
                bid_qty=1,
                event_type="trade",
                account_id=tnp[2],
                contract_qty=tnp[3] * 100,
                price=tnp[1] * 1000000,
                unit_price=10000,
                symbol="sname",
                contract_unit_of_measure="USD",
                price_increment=0.00001,
                rate_to_usd=1,
            )

            self.portfolio.on_trade(event)

        expected = 1
        result = self.portfolio.closed_positions.__len__()
        self.assertEqual(expected, result)

    def test_realised_pnl_from_non_USD_symbols_avg_price(self):
        events = [
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.905,
                    "bid_price": 108.904,
                    "currency": "JPY",
                    "ask_price": 108.906,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                    "contract_qty": 0.1,
                }
            },
            {
                "USD/JPY": {
                    "order_book_id": 2,
                    "price": 108.907,
                    "bid_price": 108.906,
                    "currency": "JPY",
                    "ask_price": 108.908,
                    "price_increment": 0.001,
                    "rate_to_usd": 0.0092,
                    "contract_qty": -0.1,
                }
            },
        ]
        for event in events:
            for symbol, event_details in event.items():
                event = Event(
                    timestamp=1,
                    order_book_id=event_details["order_book_id"],
                    event_type="trade",
                    account_id=2,
                    contract_qty=event_details["contract_qty"] * 100,
                    price=event_details["price"] * 1000000,
                    bid_price=event_details["bid_price"] * 1000000,
                    ask_price=event_details["ask_price"] * 1000000,
                    bid_qty=1,
                    ask_qty=1,
                    unit_price=10000,
                    symbol=symbol,
                    currency=event_details["currency"],
                    price_increment=event_details["price_increment"],
                    rate_to_usd=event_details["rate_to_usd"],
                )

                self.portfolio.on_trade(event)

        # test that 0 positions as closed after realising
        self.assertEqual(0, self.portfolio.positions.__len__())

        # test that unrealised pnl is only updated for position of the same symbol

        USDJPY_position = self.portfolio.closed_positions[None, 2, 2]
        self.assertEqual(USDJPY_position.unrealised_pnl, 0)
        self.assertEqual(USDJPY_position.no_of_trades, 2)
        self.assertEqual(USDJPY_position.realised_pnl, 0.0184)


if __name__ == "__main__":
    unittest.main()
