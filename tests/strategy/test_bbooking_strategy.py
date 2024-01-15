import datetime as dt
import unittest
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from risk_backtesting.backtester import Backtester
from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.config.simulation_config import SimulationConfig
from risk_backtesting.event import Event
from risk_backtesting.exit_strategy.exit_default import ExitDefault
from risk_backtesting.risk_manager.no_risk import NoRisk
from risk_backtesting.simulator.simulation_plan import SimulationPlan
from risk_backtesting.simulator.simulations import Simulations
from risk_backtesting.strategy import AbstractStrategy, create_strategy
from risk_backtesting.strategy.bbooking_strategy import BbookingStrategy


class TobLoaderDummy:
    def get_tob_minute(
            self,
            datasource_label: str,
            order_book: List,
            start_date: dt.datetime,
            end_date: dt.datetime,
            tier: List,
            datetimes: List[dt.datetime],
    ) -> pd.DataFrame:
        last_tob_timestamp = int(
            (start_date - dt.timedelta(seconds=1)).timestamp() * 1000000
        )
        records = len(order_book)
        return pd.DataFrame(
            {
                "datasource": [4] * records,
                "timestamp_micros": [last_tob_timestamp] * records,
                "order_book_id": order_book,
                "tier": [1] * records,
                "bid_price": [121] * records,
                "ask_price": [212] * records,
            }
        )


class DataServerDummy:
    def get_usd_rates_for_instruments(self, date: dt.date, instruments: List[int]):
        return pd.DataFrame(
            {
                "instrument_id": instruments,
                "prior_date": [date] * len(instruments),
                "currency": ["USD"] * len(instruments),
                "rate": [1] * len(instruments),
            }
        )


class BookingPlanBuilder(unittest.TestCase):
    def setUp(self):
        self.pipeline = {
            "uid": "i011",
            "version": 1,
            "lmax_account": 1463064262,
            "target_account": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
            ],
            "load_starting_positions": False,
            "level": "mark_to_market",
            "netting_engine": "fifo",
            "matching_method": "side_of_book",
            "simulator": "simulator_pool",
            "calculate_cumulative_daily_pnl": False,
            "tick_freq": "s",
            "shard": "ldprof",
            "process_client_portfolio": True,
            "store_client_trade_snapshot": True,
            "process_lmax_portfolio": False,
            "store_lmax_trade_snapshot": False,
            "excl_period": [[21, 30], [23, 30]],
        }
        self.output_config = {
            "resample_rule": None,
            "event_features": [
                "symbol",
                "order_book_id",
                "trading_session_year",
                "trading_session_month",
                "trading_session_day",
            ],
            "save": True,
            "by": None,
            "freq": "D",
            "mode": "a",
            "filesystem": "local",
            "bucket": "risk-temp",
            "directory": "/home/jovyan/work/outputs",
            "file_type": "csv",
            "store_index": False,
            "file": None,
        }
        self.auth: Dict[str, Dict[str, Any]] = {"minio": {"uri": "sdfsdf"}}

    def tearDown(self):
        self.pipeline = dict()

    def test_config_for_single_simulation(self):

        instrument = 12345
        simulations_config = {
            "sim_1": {
                "instruments": instrument,
                "strategy_parameters": {"strategy_type": "bbooking",},
                "exit_parameters": {"exit_type": "exit_default",},
                "constructor": "zip",
            },
        }

        config = BackTestingConfig(
            auth=self.auth,
            bucket="none",
            pipeline=self.pipeline,
            output=self.output_config,
        )
        config.optionally_override_running_config_parameters(
            start_date="2019-12-04", end_date="2019-12-04"
        )

        config.build_simulations_config(simulations_config, [])
        simulation_configs: Dict[str, SimulationConfig] = config.simulation_configs

        plan: SimulationPlan = self.build_plan(
            config, simulation_configs["sim_1"], [instrument],
        )

        self.assertIsInstance(plan.strategy, BbookingStrategy)
        self.assertIsInstance(plan.strategy.exit_strategy, ExitDefault)
        self.assertEqual(plan.hash, "6a047587062e1a106104122f852ca291")
        self.assertEqual(simulation_configs.__len__(), 1)

    # noinspection PyMethodMayBeStatic
    def build_plan(
            self,
            config: BackTestingConfig,
            simulation_config: SimulationConfig,
            instruments: List[int],
    ):
        strategy: AbstractStrategy = create_strategy(
            simulation_config.strategy_parameters, simulation_config.exit_parameters
        )
        # noinspection PyTypeChecker
        return Simulations.build_simulation_plan(
            config,
            None,
            None,
            simulation_config,
            instruments,
            pd.DataFrame(columns=["account_id"]),
            strategy,
            simulation_config.event_filter_string,
        )


class BbookingWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.bbook_bt = Backtester(NoRisk(), BbookingStrategy(),)
        self.unit_price = 1000
        self.pip_size = 0.00001
        self.instrument_id = 111
        self.venue = 1
        self.symbol = "symbol1"
        self.bbook_bt.process_client_portfolio = True

    def test_account_migrations_for_first_day_of_simulation(self):
        # for the first day of the simulation account_booking_risk will be empty
        self.bbook_bt.strategy.account_booking_risk = {}

        day = dt.date(2021, 9, 1)

        target_accounts = pd.DataFrame({"account_id": [123, 345, 678]})
        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int((dt.datetime.combine(day, dt.time())).timestamp() * 1000),
                                 int((dt.datetime.combine(day, dt.time())).timestamp() * 1000),
                                 int((dt.datetime.combine(day, dt.time())).timestamp() * 1000),
                                 int(
                                     (
                                             dt.datetime.combine(day, dt.time()) + dt.timedelta(hours=1)
                                     ).timestamp()
                                     * 1000
                                 ),
                                 int(
                                     (
                                             dt.datetime.combine(day, dt.time()) + dt.timedelta(hours=2)
                                     ).timestamp()
                                     * 1000
                                 ),
                                 int(
                                     (
                                             dt.datetime.combine(day, dt.time()) + dt.timedelta(hours=3)
                                     ).timestamp()
                                     * 1000
                                 ),
                             ]
                             * 2,
                "account_id": [123, 345, 678] * 4,
                "instrument_id": [
                    123,
                    123,
                    123,
                    123,
                    123,
                    123,
                    456,
                    456,
                    456,
                    456,
                    456,
                    456,
                ],
                "booking_risk": [50, 100, 100, 100, 50, 0] * 2,
            }
        )

        account_migrations = self.bbook_bt.strategy.get_account_migrations(
            day=day,
            target_accounts=target_accounts,
            shard="ldprof",
            instruments=[123, 456],
            tob_loader=TobLoaderDummy(),
            dataserver=DataServerDummy(),
            load_booking_risk_from_snapshot=True,
            load_internalisation_risk_from_snapshot=False,
            snapshot=snapshot,
        )

        self.assertEqual(6, len(account_migrations))
        self.assertEqual(
            [123, 123, 345, 345, 678, 678], list(account_migrations.account_id.values)
        )
        self.assertEqual(
            [1.0, 1.0, 0.5, 0.5, 0.0, 0.0], list(account_migrations.booking_risk.values)
        )
        # self.assertEqual(
        #     [
        #         dt.datetime(2021, 9, 1, 0, 0),
        #         dt.datetime(2021, 9, 1, 0, 0),
        #         dt.datetime(2021, 9, 1, 1, 0),
        #         dt.datetime(2021, 9, 1, 1, 0),
        #         dt.datetime(2021, 9, 1, 2, 0),
        #         dt.datetime(2021, 9, 1, 2, 0),
        #     ],
        #     list(account_migrations.index.to_pydatetime()),
        # )

    def test_account_migrations_no_migrations_as_state_is_the_same(self):
        # for the first day of the simulation account_booking_risk will be empty
        self.bbook_bt.strategy.account_booking_risk = {123: 1, 345: 0.5, 678: 0}

        day = dt.date(2021, 9, 1)

        target_accounts = pd.DataFrame({"account_id": [123, 345, 678]})

        snapshot = pd.DataFrame(
            {
                "timestamp": [
                                 int((dt.datetime.combine(day, dt.time())).timestamp() * 1000),
                                 int((dt.datetime.combine(day, dt.time())).timestamp() * 1000),
                                 int((dt.datetime.combine(day, dt.time())).timestamp() * 1000),
                             ]
                             * 2,
                "account_id": [123, 345, 678] * 2,
                "instrument_id": [123, 123, 123, 456, 456, 456],
                "booking_risk": [0.5, 1, 1] * 2,
            }
        )

        account_migrations = self.bbook_bt.strategy.get_account_migrations(
            day=day,
            target_accounts=target_accounts,
            shard="ldprof",
            instruments=[123, 456],
            tob_loader=TobLoaderDummy(),
            dataserver=DataServerDummy(),
            load_booking_risk_from_snapshot=True,
            load_internalisation_risk_from_snapshot=False,
            snapshot=snapshot,
        )

        self.assertEqual(0, len(account_migrations))

    def test_account_migrations_dont_load_booking_risk(self):
        # for the first day of the simulation account_booking_risk will be empty
        self.bbook_bt.strategy.account_booking_risk = {123: 1, 345: 0.5, 678: 0}

        day = dt.date(2021, 9, 1)

        target_accounts = pd.DataFrame(
            {"account_id": [123, 345, 678], "booking_risk": [1, 1, 1]}
        )

        snapshot = pd.DataFrame({})

        account_migrations = self.bbook_bt.strategy.get_account_migrations(
            day=day,
            target_accounts=target_accounts,
            shard="ldprof",
            instruments=[123, 456],
            tob_loader=TobLoaderDummy(),
            dataserver=DataServerDummy(),
            load_booking_risk_from_snapshot=False,
            load_internalisation_risk_from_target_accounts=False,
            load_internalisation_risk_from_snapshot=False,
            snapshot=snapshot,
        )

        self.assertEqual(
            {123: 1, 345: 0.5, 678: 0}, self.bbook_bt.strategy.account_booking_risk
        )
        self.assertEqual(0, len(account_migrations))


class BbookingTradeSignals(unittest.TestCase):
    def setUp(self):
        self.bbook_bt = Backtester(NoRisk(), BbookingStrategy())
        self.unit_price = 1000
        self.pip_size = 0.00001
        self.instrument_id = 111
        self.venue = 1
        self.symbol = "symbol1"
        self.bbook_bt.process_client_portfolio = True

    def test_open_first_position_for_score_with_100_risk(self):
        """
        client long pos triggers lmax to open short position
        :return:
        """
        self.bbook_bt.strategy.account_booking_risk = {1: 1}

        for direction in [1, -1]:

            self.bbook_bt.client_portfolio.positions = {}

            for account in [1]:
                contract_qty = 1.1
                price = 1.10000

                event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=(contract_qty * 100) * direction,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )
                self.bbook_bt.client_portfolio.add_position(event)

            orders = self.bbook_bt.strategy.calculate_trade_signals(
                self.bbook_bt.client_portfolio, self.bbook_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, event.account_id)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_trade")

    def test_open_first_position_for_score_with_partial_risk(self):
        """
        client long pos triggers lmax to open short position
        :return:
        """
        self.bbook_bt.strategy.account_booking_risk = {1: 0.8}

        for direction in [1, -1]:

            self.bbook_bt.client_portfolio.positions = {}

            for account in [1]:
                contract_qty = 1.1
                price = 1.10000

                event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=(contract_qty * 100) * direction,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )
                self.bbook_bt.client_portfolio.add_position(event)

            orders = self.bbook_bt.strategy.calculate_trade_signals(
                self.bbook_bt.client_portfolio, self.bbook_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, (event.contract_qty * 0.8) * -1)
                self.assertEqual(order.unfilled_qty, (event.contract_qty * 0.8) * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, event.account_id)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_trade")

    def test_open_first_position_for_score_with_extra_risk(self):
        """
        client long pos triggers lmax to open short position
        :return:
        """
        self.bbook_bt.strategy.account_booking_risk = {1: 1.2}

        for direction in [1, -1]:

            self.bbook_bt.client_portfolio.positions = {}

            for account in [1]:
                contract_qty = 1.1
                price = 1.10000

                event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=(contract_qty * 100) * direction,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )
                self.bbook_bt.client_portfolio.add_position(event)

            orders = self.bbook_bt.strategy.calculate_trade_signals(
                self.bbook_bt.client_portfolio, self.bbook_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, (event.contract_qty * 1.2) * -1)
                self.assertEqual(order.unfilled_qty, (event.contract_qty * 1.2) * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, event.account_id)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_trade")

    def test_open_first_position_for_account_with_open_positions_and_100_risk(self):
        """
        client long pos triggers lmax to open short position
        :return:
        """
        self.bbook_bt.strategy.account_booking_risk = {1: 1}

        for direction in [1, -1]:
            self.bbook_bt.client_portfolio.positions = dict()

            for account in [1]:
                contract_qty = 1.1
                price = 1.10000

                # add multiple trades so that booking as to open bigger initial position
                event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=(contract_qty * 100) * direction,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )
                self.bbook_bt.client_portfolio.add_position(event)
                self.bbook_bt.client_portfolio.modify_position(event)

            orders = self.bbook_bt.strategy.calculate_trade_signals(
                self.bbook_bt.client_portfolio, self.bbook_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, (event.contract_qty * 3) * -1)
                self.assertEqual(order.unfilled_qty, (event.contract_qty * 3) * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, event.account_id)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_trade")

    def test_increase_risk_for_account_with_open_positions(self):
        """
        client long pos triggers lmax to open short position
        :return:
        """
        self.bbook_bt.strategy.account_booking_risk = {1: 0.9}

        for direction in [1, -1]:
            self.bbook_bt.client_portfolio.positions = dict()
            self.bbook_bt.lmax_portfolio.positions = dict()

            for account in [1]:
                contract_qty = 1.1
                price = 1.10000

                # add multiple trades so that booking as to open bigger initial position
                event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=(contract_qty * 100) * direction,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )
                self.bbook_bt.client_portfolio.add_position(event)
                self.bbook_bt.client_portfolio.modify_position(event)

                # add lmax position at .6 risk to be increase to .9
                lmax_event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=((contract_qty * -100) * direction) * 0.6,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )

                self.bbook_bt.lmax_portfolio.add_position(lmax_event)

            orders = self.bbook_bt.strategy.calculate_trade_signals(
                self.bbook_bt.client_portfolio, self.bbook_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, (132.0 * np.sign(direction)) * -1)
                self.assertEqual(order.unfilled_qty, (132.0 * np.sign(direction)) * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, event.account_id)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_trade")

    def test_decrease_risk_for_account_with_open_positions(self):
        """
        client long pos triggers lmax to open short position
        :return:
        """
        self.bbook_bt.strategy.account_booking_risk = {1: 0.9}

        for direction in [1, -1]:
            self.bbook_bt.client_portfolio.positions = dict()
            self.bbook_bt.lmax_portfolio.positions = dict()

            for account in [1]:
                contract_qty = 1.1
                price = 1.10000

                # add multiple trades so that booking as to open bigger initial position
                event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=(contract_qty * 100) * direction,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )
                self.bbook_bt.client_portfolio.add_position(event)
                self.bbook_bt.client_portfolio.modify_position(event)

                # add lmax position at 1 risk to be decreased to .9
                lmax_event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=((contract_qty * -100) * direction),
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )

                self.bbook_bt.lmax_portfolio.add_position(lmax_event)

            orders = self.bbook_bt.strategy.calculate_trade_signals(
                self.bbook_bt.client_portfolio, self.bbook_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, (88.0 * np.sign(direction)) * -1)
                self.assertEqual(order.unfilled_qty, (88.0 * np.sign(direction)) * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, event.account_id)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_trade")


class BbookingMigrationSignals(unittest.TestCase):
    def setUp(self):
        self.bbook_bt = Backtester(
            NoRisk(), BbookingStrategy(exit_strategy=ExitDefault()),
        )
        self.unit_price = 1000
        self.pip_size = 0.00001
        self.instrument_id = 111
        self.venue = 1
        self.symbol = "symbol1"
        self.bbook_bt.process_client_portfolio = True

    def test_migrations_as_position_is_flat(self):
        """
        client long pos triggers lmax to open short position
        :return:
        """

        for direction in [1, -1]:
            account = 1

            migration_event = Event(
                account_id=account, booking_risk=1, event_type="account_migration"
            )

            orders = self.bbook_bt.strategy.calculate_migration_signals(
                self.bbook_bt.client_portfolio,
                self.bbook_bt.lmax_portfolio,
                migration_event,
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(0 == orders.__len__())
            self.assertEqual(1, self.bbook_bt.strategy.account_booking_risk[account])

    def test_increase_risk_for_account_with_open_positions_previously_no_risk(self):
        """
        client long pos triggers lmax to open short position
        :return:
        """

        for direction in [1, -1]:
            self.bbook_bt.client_portfolio.positions = dict()
            self.bbook_bt.lmax_portfolio.positions = dict()

            for account in [1]:
                contract_qty = 1.1
                price = 1.10000

                # add multiple trades so that booking as to open bigger initial position
                event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=(contract_qty * 100) * direction,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )
                self.bbook_bt.client_portfolio.add_position(event)

            ask_price = int((price + 0.00001) * 1000000)
            bid_price = int((price - 0.00001) * 1000000)

            migration_event = Event(
                account_id=account,
                booking_risk=0.9,
                ask_price=ask_price,
                bid_price=bid_price,
            )

            orders = self.bbook_bt.strategy.calculate_migration_signals(
                self.bbook_bt.client_portfolio,
                self.bbook_bt.lmax_portfolio,
                migration_event,
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, (99.0 * np.sign(direction)) * -1)
                self.assertEqual(order.unfilled_qty, (99.0 * np.sign(direction)) * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.order_book_id, event.order_book_id)
                self.assertEqual(
                    bid_price if direction == 1 else ask_price, order.price
                )
                self.assertEqual(order.account_id, event.account_id)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "M")
                self.assertEqual(order.signal, "account_migration")

    def test_increase_risk_for_account_with_open_positions_previously_had_some_risk(
            self,
    ):
        """
        client long pos triggers lmax to open short position
        :return:
        """

        for direction in [1, -1]:
            self.bbook_bt.client_portfolio.positions = dict()
            self.bbook_bt.lmax_portfolio.positions = dict()

            for account in [1]:
                contract_qty = 1.1
                price = 1.10000

                # add multiple trades so that booking as to open bigger initial position
                event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=(contract_qty * 100) * direction,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )
                self.bbook_bt.client_portfolio.add_position(event)

                lmax_event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=((contract_qty * -100) * direction) * 0.6,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )

                self.bbook_bt.lmax_portfolio.add_position(lmax_event)

            ask_price = int((price + 0.00001) * 1000000)
            bid_price = int((price - 0.00001) * 1000000)

            migration_event = Event(
                account_id=account,
                booking_risk=0.9,
                ask_price=ask_price,
                bid_price=bid_price,
            )

            orders = self.bbook_bt.strategy.calculate_migration_signals(
                self.bbook_bt.client_portfolio,
                self.bbook_bt.lmax_portfolio,
                migration_event,
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, (33.0 * np.sign(direction)) * -1)
                self.assertEqual(order.unfilled_qty, (33.0 * np.sign(direction)) * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.order_book_id, event.order_book_id)
                self.assertEqual(
                    bid_price if direction == 1 else ask_price, order.price
                )
                self.assertEqual(order.account_id, event.account_id)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "M")
                self.assertEqual(order.signal, "account_migration")

    def test_decrease_risk_for_account_with_open_positions_previously_had_some_risk(
            self,
    ):
        """
        client long pos triggers lmax to open short position
        :return:
        """

        for direction in [1, -1]:
            self.bbook_bt.client_portfolio.positions = dict()
            self.bbook_bt.lmax_portfolio.positions = dict()

            for account in [1]:
                contract_qty = 1.1
                price = 1.10000

                # add multiple trades so that booking as to open bigger initial position
                event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=(contract_qty * 100) * direction,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )
                self.bbook_bt.client_portfolio.add_position(event)

                lmax_event = Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=((contract_qty * -100) * direction) * 0.9,
                    price=price * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    event_type="trade",
                    rate_to_usd=1,
                )

                self.bbook_bt.lmax_portfolio.add_position(lmax_event)

            ask_price = int((price + 0.00001) * 1000000)
            bid_price = int((price - 0.00001) * 1000000)

            migration_event = Event(
                account_id=account,
                booking_risk=0.6,
                ask_price=ask_price,
                bid_price=bid_price,
            )

            orders = self.bbook_bt.strategy.calculate_migration_signals(
                self.bbook_bt.client_portfolio,
                self.bbook_bt.lmax_portfolio,
                migration_event,
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, (-33.0 * np.sign(direction)) * -1)
                self.assertEqual(order.unfilled_qty, (-33.0 * np.sign(direction)) * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.order_book_id, event.order_book_id)
                self.assertEqual(
                    ask_price if direction == 1 else bid_price, order.price
                )
                self.assertEqual(order.account_id, event.account_id)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "M")
                self.assertEqual(order.signal, "account_migration")


if __name__ == "__main__":
    unittest.main()
