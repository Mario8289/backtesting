import unittest
from typing import List, Dict, Any

from risk_backtesting.config.backtesting_config import (
    BackTestingConfig,
    SimulationConfig,
)
from risk_backtesting.simulator.simulation_plan import SimulationPlan
from risk_backtesting.backtester import Backtester
from risk_backtesting.event import Event
from risk_backtesting.strategy import AbstractStrategy, create_strategy
from risk_backtesting.simulator.simulations import Simulations
from risk_backtesting.exit_strategy.aggressive import Aggressive
from risk_backtesting.exit_strategy.passive import Passive
from risk_backtesting.exit_strategy.exit_default import ExitDefault
from risk_backtesting.risk_manager.no_risk import NoRisk
from risk_backtesting.strategy.internalisation_strategy import InternalisationStrategy

import pandas as pd
import datetime as dt


class DataServerDummy:
    def __init__(
            self,
            rate: float = 1,
            closing_price: float = None,
            instrument_class: str = "CURRENCY",
            contract_unit_of_measure="USD",
            unit_price: int = 10000,
    ):
        self.rate = rate
        self.instrument_class = instrument_class
        self.closing_price = closing_price
        self.contract_unit_of_measure = contract_unit_of_measure
        self.unit_price = unit_price

    def get_usd_rate_for_instruments_unit_of_measure(
            self, shard: str, date: dt.date, instruments: List[int]
    ) -> pd.DataFrame:
        df = pd.DataFrame(
            data={
                "instrument_id": instruments,
                "unit_price": [self.unit_price] * len(instruments),
                "rate": [self.rate] * len(instruments),
                "class": [self.instrument_class] * len(instruments),
                "contract_unit_of_measure": [self.contract_unit_of_measure]
                                            * len(instruments),
            }
        )
        return df

    def get_closing_prices(
            self, shard: str, instruments: List[int], start_date, end_date
    ):
        df = pd.DataFrame(
            data={
                "datasource": [4] * len(instruments),
                "trading_session": [start_date] * len(instruments),
                "price": [self.closing_price] * len(instruments),
                "order_book_id": instruments,
                "symbol": instruments,
                "unit_price": [self.unit_price] * len(instruments),
                "contract_unit_of_measure": [None] * len(instruments),
                "currency": [None] * len(instruments),
                "rate_to_usd": [None] * len(instruments),
            }
        )
        return df


class InternalisationUpdateStrategy(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        self.pipeline = dict()

    def test_update_strategy_does_nothing_if_max_pos_qty_type_is_contracts(self):
        # account_id and instruments added in the config generation
        instruments = [101, 102]
        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": 100,
            "max_pos_qty_buffer": 1,
            "allow_partial_fills": True,
            "max_pos_qty_type": "contracts",
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": instruments,
        }

        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters
        )
        day = dt.datetime(2021, 10, 11)
        strategy.update(
            shard="ldprof",
            date=day,
            instruments=instruments,
            dataserver=DataServerDummy(),
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {101: 100, 102: 100})

    # all below tests are for when the max_pos_qty_type is set to dollars
    def test_update_strategy_sets_contracts_on_first_trading_session_when_rebalance_disabled_indices(
            self,
    ):
        # account_id and instruments added in the config generation
        dollar_amount: int = int(1e6)
        max_pos_qty_type: str = "dollars"
        instruments: List[int] = [101]

        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": dollar_amount,
            "max_pos_qty_buffer": 1,
            "max_pos_qty_type": max_pos_qty_type,
            "allow_partial_fills": True,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": instruments,
        }

        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters
        )
        day = dt.datetime(2021, 10, 11)

        # ensure that the next rebalance date has not been set
        self.assertIsNone(strategy._next_rebalance_date)

        strategy.update(
            shard="ldprof",
            date=day,
            instruments=instruments,
            dataserver=DataServerDummy(
                instrument_class="EQUITY_INDEX_CFD",
                closing_price=7088.3,
                rate=1.33021,
                unit_price=1,
            ),
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {101: dollar_amount})
        self.assertEqual(strategy._max_pos_qty, {101: 106 * 1e2})
        self.assertEqual(strategy._next_rebalance_date, day)

    def test_update_strategy_sets_contracts_on_first_trading_session_when_rebalance_disabled(
            self,
    ):
        # account_id and instruments added in the config generation
        dollar_amount: int = int(1e6)
        max_pos_qty_type: str = "dollars"
        instruments: List[int] = [101, 102]

        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": dollar_amount,
            "max_pos_qty_buffer": 1,
            "max_pos_qty_type": max_pos_qty_type,
            "allow_partial_fills": True,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": instruments,
        }

        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters
        )
        day = dt.datetime(2021, 10, 11)

        # ensure that the next rebalance date has not been set
        self.assertIsNone(strategy._next_rebalance_date)

        strategy.update(
            shard="ldprof",
            date=day,
            instruments=instruments,
            dataserver=DataServerDummy(),
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {101: dollar_amount, 102: dollar_amount})
        self.assertEqual(strategy._max_pos_qty, {101: 100 * 1e2, 102: 100 * 1e2})
        self.assertEqual(strategy._next_rebalance_date, day)

    def test_update_strategy_doesnt_set_contracts_on_after_first_trading_session_when_rebalance_disabled(
            self,
    ):
        # account_id and instruments added in the config generation
        dollar_amount: int = int(1e6)
        max_pos_qty_type: str = "dollars"
        instruments: List[int] = [101, 102]

        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": dollar_amount,
            "max_pos_qty_buffer": 1,
            "allow_partial_fills": True,
            "max_pos_qty_type": max_pos_qty_type,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": instruments,
        }

        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters
        )
        day = dt.datetime(2021, 10, 11)
        next_day = dt.datetime(2021, 10, 12)

        # ensure that the next rebalance date has not been set
        strategy._next_rebalance_date = day

        strategy.update(
            shard="ldprof",
            date=next_day,
            instruments=instruments,
            dataserver=DataServerDummy(),
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {101: dollar_amount, 102: dollar_amount})
        self.assertEqual(strategy._max_pos_qty, {})
        self.assertEqual(strategy._next_rebalance_date, day)

    def test_update_strategy_sets_contracts_on_first_trading_session_when_rebalance_enabled(
            self,
    ):
        # account_id and instruments added in the config generation
        dollar_amount: int = int(1e6)
        max_pos_qty_type: str = "dollars"
        instruments: List[int] = [101, 102]

        max_pos_qty_rebalance_rate = "10d"
        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": dollar_amount,
            "max_pos_qty_buffer": 1,
            "allow_partial_fills": True,
            "max_pos_qty_type": max_pos_qty_type,
            "max_pos_qty_rebalance_rate": max_pos_qty_rebalance_rate,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": instruments,
        }

        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters
        )
        day = dt.datetime(2021, 10, 11)

        # ensure that the next rebalance date has not been set
        self.assertIsNone(strategy._next_rebalance_date)

        strategy.update(
            shard="ldprof",
            date=day,
            instruments=instruments,
            dataserver=DataServerDummy(),
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {101: dollar_amount, 102: dollar_amount})
        self.assertEqual(strategy._max_pos_qty, {101: 100 * 1e2, 102: 100 * 1e2})
        self.assertEqual(
            strategy._next_rebalance_date,
            day + pd.to_timedelta(max_pos_qty_rebalance_rate),
            )

    def test_update_strategy_doesnt_set_contracts_on_intermediate_trading_session_when_rebalance_enabled(
            self,
    ):
        # account_id and instruments added in the config generation
        dollar_amount: int = int(1e6)
        max_pos_qty_type: str = "dollars"
        instruments: List[int] = [101, 102]

        max_pos_qty_rebalance_rate = "10d"
        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": dollar_amount,
            "max_pos_qty_buffer": 1,
            "allow_partial_fills": True,
            "max_pos_qty_type": max_pos_qty_type,
            "max_pos_qty_rebalance_rate": max_pos_qty_rebalance_rate,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": instruments,
        }

        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters
        )
        day = dt.datetime(2021, 10, 11)

        strategy._next_rebalance_date = day + pd.to_timedelta(
            max_pos_qty_rebalance_rate
        )
        strategy.update(
            shard="ldprof",
            date=day + dt.timedelta(days=1),
            instruments=instruments,
            dataserver=DataServerDummy(),
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {101: dollar_amount, 102: dollar_amount})
        self.assertEqual(strategy._max_pos_qty, {})
        self.assertEqual(
            strategy._next_rebalance_date,
            day + pd.to_timedelta(max_pos_qty_rebalance_rate),
            )

    def test_update_strategy_set_contracts_on_when_next_rebalance_date_reached_when_rebalance_enabled(
            self,
    ):
        # account_id and instruments added in the config generation
        dollar_amount: int = int(1e6)
        max_pos_qty_type: str = "dollars"
        instruments: List[int] = [101, 102]
        max_pos_qty_rebalance_rate = "10d"

        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": dollar_amount,
            "max_pos_qty_buffer": 1,
            "allow_partial_fills": True,
            "max_pos_qty_type": max_pos_qty_type,
            "max_pos_qty_rebalance_rate": max_pos_qty_rebalance_rate,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": instruments,
        }

        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters
        )
        day = dt.datetime(2021, 10, 11)

        strategy._next_rebalance_date = day

        strategy.update(
            shard="ldprof",
            date=day,
            instruments=instruments,
            dataserver=DataServerDummy(),
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {101: dollar_amount, 102: dollar_amount})
        self.assertEqual(strategy._max_pos_qty, {101: 100 * 1e2, 102: 100 * 1e2})
        self.assertEqual(
            strategy._next_rebalance_date,
            day + pd.to_timedelta(max_pos_qty_rebalance_rate),
            )


class InternalisationPlanBuilder(unittest.TestCase):
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

    def test_create_strategy_single_instrument_position_limit_level_instrument(self):
        # account_id and instruments added in the config generation

        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": 100,
            "max_pos_qty_buffer": 1,
            "allow_partial_fills": True,
            "max_pos_qty_type": "contracts",
            "max_pos_qty_level": "instrument",
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": [101, 102],
        }

        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {101: 100, 102: 100})

    def test_create_strategy_single_instrument_position_limit_level_one_currency(self):
        # account_id and instruments added in the config generation

        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": 100,
            "max_pos_qty_buffer": 1,
            "allow_partial_fills": True,
            "max_pos_qty_type": "contracts",
            "max_pos_qty_level": "currency",
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": [101, 102],
        }
        symbol_currencies = {101: "EUR", 102: "EUR"}
        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters, symbol_currencies=symbol_currencies
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {"EUR": 100})

    def test_create_strategy_single_instrument_position_limit_level_two_currency(self):
        # account_id and instruments added in the config generation

        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": 100,
            "max_pos_qty_buffer": 1,
            "allow_partial_fills": True,
            "max_pos_qty_type": "contracts",
            "max_pos_qty_level": "currency",
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": [101, 102],
        }
        symbol_currencies = {101: "EUR", 102: "USD"}
        exit_parameters = {"exit_type": "exit_default"}

        strategy: AbstractStrategy = create_strategy(
            strategy_parameters, exit_parameters, symbol_currencies=symbol_currencies
        )

        self.assertIsInstance(strategy, InternalisationStrategy)
        self.assertEqual(strategy.max_pos_qty, {"EUR": 100, "USD": 100})

    def test_config_for_single_simulation_max_pos_qty_as_contracts(self):

        instrument = 12345
        simulations_config = {
            "sim_1": {
                "instruments": instrument,
                "strategy_parameters": {
                    "strategy_type": "internalisation",
                    "max_pos_qty": 100,
                    "allow_partial_fills": True,
                    "max_pos_qty_buffer": 1,
                    "max_pos_qty_type": "contracts",
                    "max_pos_qty_rebalance_rate": None,
                    "position_lifespan": None,
                },
                "exit_parameters": {"exit_type": "exit_default"},
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

        self.assertIsInstance(plan.strategy, InternalisationStrategy)
        self.assertIsInstance(plan.strategy.exit_strategy, ExitDefault)
        self.assertEqual(plan.strategy.max_pos_qty, {12345: 100})
        self.assertEqual(plan.strategy.max_pos_qty_type, "contracts")
        self.assertIsNone(plan.strategy.max_pos_qty_rebalance_rate)
        self.assertEqual(simulation_configs.__len__(), 1)
        self.assertEqual(plan.hash, "f8e846dcd083c73b452a9555c55bfe9d")

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


class InternalisationMarketSignals(unittest.TestCase):

    # these tests are to review market signals on internalsation strategy

    def setUp(self):
        self.unit_price = 1000
        self.pip_size = 0.00001
        self.instrument_id = 111
        self.venue = 1
        self.symbol = "symbol1"

        self.int_bt = Backtester(
            strategy=InternalisationStrategy(
                account_id=4,
                max_pos_qty={self.instrument_id: 100},
                allow_partial_fills=True,
                max_pos_qty_buffer=1.25,
                position_lifespan=None,
                exit_strategy=Aggressive(stoploss_limit=10, takeprofit_limit=10),
            ),
            risk_manager=NoRisk(),
            netting_engine={"client": "fifo", "lmax": "fifo"},
            matching_method="side_of_book",
            process_client_portfolio=False,
            process_lmax_portfolio=True,
            store_client_trade_snapshot=False,
            store_lmax_trade_snapshot=True,
        )

    def exit_market_signal_if_LMAX_flat(self):
        self.int_bt.strategy.matching_method = "side_of_book"

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            event_type="market_data",
            ask_price=1.40322 * 1000000,
            tob_snapshot_ask_price=1.40322 * 1000000,
            ask_qty=1000,
            bid_price=1.40320 * 1000000,
            tob_snapshot_bid_price=1.40320 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
        )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_market_signal_SL_closes_1_LMAX_short_pos_sob(self):

        # test that SL triggers when SL is exceeded by market data tick
        self.int_bt.strategy.position_lifespan = None
        self.int_bt.strategy.exit_strategy.stoploss_limit = 10
        self.int_bt.strategy.matching_method = "side_of_book"
        self.int_bt.client_portfolio.matching_method = "side_of_book"
        self.int_bt.lmax_portfolio.matching_method = "side_of_book"

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=1.1 * 100,
                price=1.40330 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            account_id=0,
            contract_qty=0,
            event_type="market_data",
            ask_price=1.40322 * 1000000,
            tob_snapshot_ask_price=1.40322 * 1000000,
            ask_qty=1000,
            bid_price=1.40320 * 1000000,
            tob_snapshot_bid_price=1.40320 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
        )

        orders = self.int_bt.strategy.calculate_market_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, round(-1.1 * 100))
            self.assertEqual(order.unfilled_qty, round(-1.1 * 100))
            self.assertEqual(order.event_type, "hedge")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "S")
            self.assertEqual(order.signal, "SL_close_position")

    def test_market_signal_SL_isnt_triggered_on_1_LMAX_short_pos_sob(self):

        # test that SL triggers when SL is exceeded by market data tick

        self.int_bt.strategy.exit_strategy.stoploss_limit = 10
        self.int_bt.strategy.matching_method = "side_of_book"

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=1.1 * 100,
                price=1.40330 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            account_id=0,
            contract_qty=0,
            event_type="market_data",
            ask_price=1.403250 * 1000000,
            ask_qty=1000,
            bid_price=1.403220 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
        )

        orders = self.int_bt.strategy.calculate_market_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_market_signal_SL_closes_1_LMAX_short_pos_mid(self):

        # test that SL triggers when SL is exceeded by market data tick

        self.int_bt.strategy.exit_strategy.stoploss_limit = 10
        self.int_bt.strategy.matching_method = "mid"

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=1.1 * 100,
                price=1.40330 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            account_id=0,
            contract_qty=0,
            event_type="market_data",
            ask_price=1.40320 * 1000000,
            tob_snapshot_ask_price=1.40320 * 1000000,
            ask_qty=1000,
            bid_price=1.40318 * 1000000,
            tob_snapshot_bid_price=1.40318 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
        )

        orders = self.int_bt.strategy.calculate_market_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, round(-1.1 * 100))
            self.assertEqual(order.unfilled_qty, round(-1.1 * 100))
            self.assertEqual(order.event_type, "hedge")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "S")
            self.assertEqual(order.signal, "SL_close_position")

    def test_market_signal_SL_not_action_as_price_untrusted(self):

        # test that SL triggers when SL is exceeded by market data tick

        self.int_bt.strategy.exit_strategy.stoploss_limit = 10
        self.int_bt.strategy.matching_method = "mid"

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=1.1 * 100,
                price=1.40330 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            account_id=0,
            contract_qty=0,
            event_type="market_data",
            ask_price=1.40320 * 1000000,
            tob_snapshot_ask_price=1.40320 * 1000000,
            ask_qty=1000,
            bid_price=1.40318 * 1000000,
            tob_snapshot_bid_price=1.40318 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
            untrusted=1,
        )

        orders = self.int_bt.strategy.calculate_market_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_gfd_signal_doenst_close_1_LMAX_short_pos_mid(self):

        # test that SL triggers when SL is exceeded by market data tick

        self.int_bt.strategy.exit_strategy.stoploss_limit = 10
        self.int_bt.strategy.matching_method = "mid"
        self.int_bt.strategy.position_lifespan = "gfd"
        self.int_bt.strategy.position_lifespan_exit_strategy = Passive(
            skew_at="same_side", skew_by=2
        )

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=1.1 * 100,
                price=1.40330 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # set the last price for the passive exit so that if it will place an order
        self.int_bt.lmax_portfolio.positions[1, 111, 4].exit_attr["lastprice"] = (
                1.40331 * 1000000
        )
        self.int_bt.lmax_portfolio.positions[1, 111, 4].exit_attr[
            "start_time"
        ] = 1536191000000000

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            account_id=0,
            contract_qty=0,
            event_type="market_data",
            ask_price=1.40331 * 1000000,
            ask_qty=1000,
            bid_price=1.40329 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
            gfd=0,
            gfw=0,
        )

        orders = self.int_bt.strategy.calculate_market_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_gfd_signal_closes_1_LMAX_short_pos_mid(self):

        # test that SL triggers when SL is exceeded by market data tick

        self.int_bt.strategy.exit_strategy.stoploss_limit = 10
        self.int_bt.strategy.matching_method = "mid"
        self.int_bt.strategy.position_lifespan = "gfd"
        self.int_bt.strategy.position_lifespan_exit_strategy = Passive(
            skew_at="same_side", skew_by=2
        )

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=1.1 * 100,
                price=1.40330 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # set the last price for the passive exit so that if it will place an order
        self.int_bt.lmax_portfolio.positions[1, 111, 4].exit_attr["lastprice"] = (
                1.40331 * 1000000
        )
        self.int_bt.lmax_portfolio.positions[1, 111, 4].exit_attr[
            "start_time"
        ] = 1536191000000000

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            account_id=0,
            contract_qty=0,
            event_type="market_data",
            ask_price=1.40331 * 1000000,
            ask_qty=1000,
            bid_price=1.40329 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
            gfd=1,
            gfw=0,
        )

        orders = self.int_bt.strategy.calculate_market_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, round(-1.1 * 100))
            self.assertEqual(order.unfilled_qty, round(-1.1 * 100))
            self.assertEqual(order.event_type, "hedge")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "passive")

    def test_gfw_signal_closes_1_LMAX_short_pos_mid(self):

        # test that SL triggers when SL is exceeded by market data tick

        self.int_bt.strategy.exit_strategy.stop_loss_limit = 10
        self.int_bt.strategy.matching_method = "mid"
        self.int_bt.strategy.position_lifespan = "gfw"
        self.int_bt.strategy.position_lifespan_exit_strategy = Passive(
            skew_at="same_side", skew_by=2
        )

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=1.1 * 100,
                price=1.40330 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # set the last price for the passive exit so that if it will place an order
        self.int_bt.lmax_portfolio.positions[1, 111, 4].exit_attr["lastprice"] = (
                1.40331 * 1000000
        )
        self.int_bt.lmax_portfolio.positions[1, 111, 4].exit_attr[
            "start_time"
        ] = 1536191000000000

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            account_id=0,
            contract_qty=0,
            event_type="market_data",
            ask_price=1.40331 * 1000000,
            ask_qty=1000,
            bid_price=1.40329 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
            gfd=0,
            gfw=1,
        )

        orders = self.int_bt.strategy.calculate_market_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, round(-1.1 * 100))
            self.assertEqual(order.unfilled_qty, round(-1.1 * 100))
            self.assertEqual(order.event_type, "hedge")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "passive")

    def test_gfw_signal_doesnt_close_1_LMAX_short_pos_mid(self):

        # test that SL triggers when SL is exceeded by market data tick

        self.int_bt.strategy.exit_strategy.stoploss_limit = 10
        self.int_bt.strategy.matching_method = "mid"
        self.int_bt.strategy.position_lifespan = "gfw"
        self.int_bt.strategy.position_lifespan_exit_strategy = Passive(
            skew_at="same_side", skew_by=2
        )

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=1.1 * 100,
                price=1.40330 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # set the last price for the passive exit so that if it will place an order
        self.int_bt.lmax_portfolio.positions[1, 111, 4].exit_attr["lastprice"] = (
                1.40331 * 1000000
        )
        self.int_bt.lmax_portfolio.positions[1, 111, 4].exit_attr[
            "start_time"
        ] = 1536191000000000

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            account_id=0,
            contract_qty=0,
            event_type="market_data",
            ask_price=1.40331 * 1000000,
            ask_qty=1000,
            bid_price=1.40329 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
            gfd=0,
            gfw=0,
        )

        orders = self.int_bt.strategy.calculate_market_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_market_signal_SL_isnt_closed_1_LMAX_short_pos_mid(self):

        # test that SL triggers when SL is exceeded by market data tick

        self.int_bt.strategy.exit_strategy.stoploss_limit = 10
        self.int_bt.lmax_portfolio.matching_method = "mid"

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=1.1 * 100,
                price=1.40330 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        event = Event(
            order_book_id=self.instrument_id,
            unit_price=self.unit_price,
            symbol=self.symbol,
            price_increment=self.pip_size,
            timestamp=1536192000000000,
            account_id=0,
            contract_qty=0,
            event_type="market_data",
            ask_price=1.40322 * 1000000,
            ask_qty=1000,
            bid_price=1.40320 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
        )

        orders = self.int_bt.strategy.calculate_market_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])


class InternaliationTradeSignals(unittest.TestCase):
    def setUp(self):
        self.lmax_account = 4
        self.stoploss_limit = 100
        self.takeprofit_limit = 10000
        self.unit_price = 1000
        self.pip_size = 0.00001
        self.instrument_id = 111
        self.venue = 1
        self.symbol = "symbol1"

        self.int_bt = Backtester(
            NoRisk(),
            InternalisationStrategy(
                account_id=self.lmax_account,
                max_pos_qty={self.instrument_id: 400},
                max_pos_qty_buffer=1.0,
                allow_partial_fills=True,
                exit_strategy=ExitDefault(),
            ),
            store_client_md_snapshot=True,
        )
        self.int_bt.strategy._max_pos_qty = {
            k: v * 1e2 for (k, v) in self.int_bt.strategy.max_pos_qty.items()
        }
        self.int_bt.process_client_portfolio = True

    def test_take_on_partial_short_position_up_to_position_limit(self):
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=1000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, -100)
            self.assertEqual(order.unfilled_qty, -100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_take_on_partial_long_position_up_to_position_limit(self):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 100)
            self.assertEqual(order.unfilled_qty, 100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_reject_due_to_order_size_on_short_position_exceeding_position_limit(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=10000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_reject_due_to_order_size_on_long_position_exceeding_position_limit(self):
        self.int_bt.strategy.allow_partial_fills = False

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_accept_trade_on_short_position_as_it_is_the_same_order(self):
        order_id: int = 2

        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.last_order_id = order_id
        self.int_bt.strategy.last_order_filled = True

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_id=order_id,
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=10000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, -110)
            self.assertEqual(order.unfilled_qty, -110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_accept_trade_on_long_position_as_it_is_the_same_order(self):
        order_id: int = 2

        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.last_order_id = order_id
        self.int_bt.strategy.last_order_filled = True

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 110)
            self.assertEqual(order.unfilled_qty, 110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_reject_trade_on_short_position_when_position_in_buffer(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-100.1 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 0.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_id=1,
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=contract_qty,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_reject_trade_on_long_position_when_position_in_buffer(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=100.1 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = -0.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_id=1,
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=contract_qty,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_accept_trade_on_short_position_due_to_buffer(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_id=1,
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=contract_qty,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, -110)
            self.assertEqual(order.unfilled_qty, -110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_accept_trade_on_long_position_due_to_buffer(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=contract_qty,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 110)
            self.assertEqual(order.unfilled_qty, 110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_take_on_partial_short_position_due_to_internalisation_risk(self):
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=1000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                internalisation_risk=0.5,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, -50)
            self.assertEqual(order.unfilled_qty, -50)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_take_on_partial_long_position_due_to_internalisation_risk(self):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        for account in [1]:
            contract_qty = -1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                internalisation_risk=0.5,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 50)
            self.assertEqual(order.unfilled_qty, 50)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_take_on_short_position_up_to_position_limit_plus_buffer(self):
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=1000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, -110)
            self.assertEqual(order.unfilled_qty, -110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

        # self.assertIsInstance(orders, List)
        # self.assertEqual(orders, [])

    def test_take_on_long_position_up_to_position_limit_plus_buffer(self):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 110)
            self.assertEqual(order.unfilled_qty, 110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_dont_take_on_short_position_as_we_are_already_at_our_position_limit(self):
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-100 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=1000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_dont_take_on_long_position_as_we_are_already_at_our_position_limit(self):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=100 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_take_on_full_short_position_that_is_bigger_than_position_limit_as_are_long(
            self,
    ):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = 120 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=190 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, -120 * 100)
            self.assertEqual(order.unfilled_qty, -120 * 100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_take_on_full_long_position_that_is_bigger_than_position_limit_as_are_short(
            self,
    ):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -120 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=-190 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, 120 * 100)
            self.assertEqual(order.unfilled_qty, 120 * 100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_dont_open_long_pos_on_client_open_short_pos_as_order_not_immediate(self):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1] * 2:
            contract_qty = -70 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=-180 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                internalise_limit_orders=0,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=0,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_take_on_limit_order_if_allow_limit_tag_is_enabled(self):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1] * 2:
            contract_qty = -70 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                    internalise_limit_orders=1,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=-180 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                internalise_limit_orders=1,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=0,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, event.contract_qty * -1)
            self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_open_short_pos_on_client_open_long_pos(self):
        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.add_position(event)

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, event.contract_qty * -1)
            self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_open_long_pos_on_client_open_short_pos(self):
        # test client short position cause lmax to open long position
        for account in [1]:
            contract_qty = -1.1 * 100
            price = 1.10000 * 1000000

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.add_position(event)

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, event.contract_qty * -1)
            self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_take_on_partial_long_pos_on_client_open_short_upto_position_limit(self):
        # test that because client short position exceeds position limit, lmax
        # do not open a long position
        self.int_bt.strategy.update_max_pos_qty(value=4, key=self.instrument_id)

        for account in [1]:
            contract_qty = -8 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        # self.assertIsInstance(orders, List)
        # self.assertEqual(orders, [])
        for order in orders:
            self.assertEqual(order.order_qty, 4 * 100)

    def test_dont_take_on_long_pos_on_client_open_short_as_we_are_at_position_limit(
            self,
    ):
        # test that because client short position exceeds position limit, lmax
        # do not open a long position
        self.int_bt.strategy.update_max_pos_qty(value=4, key=self.instrument_id)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=4 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -8 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_client_short_position_closes_lmax_open_short_position(self):
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account = 1
        lmax_account = 4
        contract_qty = int(1.1 * 100)

        # add a long client pos and the corresponding short lmax pos
        self.int_bt.client_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=client_account,
                contract_qty=contract_qty,
                price=open_price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=lmax_account,
                contract_qty=contract_qty * -1,
                price=open_price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # new short client pos come in that closes short lmax pos
        # and close the client account position
        for account, c_qty, close_price in zip(
                [1], [contract_qty * -1], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_short_open_long")

    def test_multiple_client_short_position_closes_lmax_short_position(self):
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account_a = 1
        client_account_b = 2
        lmax_account = 4
        contract_qty = int(1.1 * 100)

        # add a long client pos and the corresponding short lmax pos
        for account in [client_account_a, client_account_b]:
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            self.int_bt.lmax_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=lmax_account,
                    contract_qty=contract_qty * -1,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

        # new short client pos come in that closes short lmax pos
        # reduces client long position
        for account, c_qty, close_price in zip(
                [1], [contract_qty * -1], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_short_open_long")

    def test_client_long_position_closes_lmax_long_position(self):
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account = 1
        lmax_account = 4
        contract_qty = int(-1.1 * 100)

        # add a short client pos and the corresponding long lmax pos
        self.int_bt.client_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=client_account,
                contract_qty=contract_qty,
                price=open_price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=lmax_account,
                contract_qty=contract_qty * -1,
                price=open_price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # new long client pos come in that closes long lmax pos
        # and long the client account position
        for account, c_qty, close_price in zip(
                [1], [contract_qty * -1], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_long_open_short")

    def test_multiple_client_long_position_closes_lmax_long_position(self):
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account_a = 1
        client_account_b = 2
        lmax_account = 4
        contract_qty = int(-1.1 * 100)

        # add a short client pos and the corresponding long lmax pos
        for account in [client_account_a, client_account_b]:
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )
            self.int_bt.lmax_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=lmax_account,
                    contract_qty=contract_qty * -1,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

        # new long client pos come in that closes long lmax pos
        # reduces client short position
        for account, c_qty, close_price in zip(
                [1], [contract_qty * -1], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )
            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_long_open_short")

    def test_client_long_position_open_short_lmax_position_when_lmax_has_multi_positions(
            self,
    ):
        "test to see if position open even if the total portfolio position exceed the position limit for a single inst"
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account = 1
        lmax_account = 4
        contract_qty = int(11 * 100)

        # add a long client pos and the corresponding short lmax pos
        for ob in [self.instrument_id, 222, 223, 224, 225]:
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=ob,
                    account_id=client_account,
                    contract_qty=contract_qty,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            self.int_bt.lmax_portfolio.add_position(
                Event(
                    order_book_id=ob,
                    account_id=lmax_account,
                    contract_qty=contract_qty * -1,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

        for account, c_qty, close_price in zip(
                [1], [contract_qty], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_long_open_short")

    # test for behaviour after position limit has been updated

    def test_dont_take_on_long_position_after_rebalance_contracts_means_we_breach_position_limit_until_we_are_back_below_limit(
            self,
    ):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        day = dt.datetime(2021, 10, 12)
        self.int_bt.strategy.max_pos_qty_type = "dollars"
        self.int_bt.strategy._next_rebalance_date = day
        self.int_bt.strategy.max_pos_qty_buffer = 1
        self.int_bt.strategy.max_pos_qty = {self.instrument_id: 1e6}
        self.int_bt.strategy._max_pos_qty = {self.instrument_id: 1e2 * 1e2}

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        self.int_bt.strategy.update(
            shard="ldprof",
            date=day,
            instruments=[self.instrument_id],
            dataserver=DataServerDummy(rate=1.1),
        )

        # cannot take on client short trade as we are now over our position limit due to a change in instrument rate
        for account in [1]:
            contract_qty = -1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )
            self.assertIsInstance(orders, List)
            self.assertEqual(orders, [])

        # client long trade reduces our position below our new position limit
        for account in [1]:
            contract_qty = 10 * 100
            order_size = 10 * 100
            price = 1.10002 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertEqual(1, len(orders))
            self.assertEqual(-1000, orders[0].order_qty)

            self.int_bt.lmax_portfolio.on_trade(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=self.lmax_account,
                    contract_qty=orders[0].order_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

        # we can now internalise client short trade up to our position limit
        for account in [1]:
            contract_qty = -2 * 100
            order_size = -2 * 100
            price = 1.10003 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertEqual(1, len(orders))
            self.assertEqual(200, orders[0].order_qty)

    def test_dont_take_on_short_position_after_rebalance_contracts_means_we_breach_position_limit_until_we_are_back_below_limit(
            self,
    ):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        day = dt.datetime(2021, 10, 12)
        self.int_bt.strategy.max_pos_qty_type = "dollars"
        self.int_bt.strategy._next_rebalance_date = day
        self.int_bt.strategy.max_pos_qty_buffer = 1
        self.int_bt.strategy.max_pos_qty = {self.instrument_id: 1e6}
        self.int_bt.strategy._max_pos_qty = {self.instrument_id: 100}

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        self.int_bt.strategy.update(
            shard="ldprof",
            date=day,
            instruments=[self.instrument_id],
            dataserver=DataServerDummy(rate=1.1),
        )

        # cannot take on client short trade as we are now over our position limit due to a change in instrument rate
        for account in [1]:
            contract_qty = 1 * 100
            order_size = 1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )
            self.assertIsInstance(orders, List)
            self.assertEqual(orders, [])

        # client long trade reduces our position below our new position limit
        for account in [1]:
            contract_qty = -10 * 100
            order_size = -10 * 100
            price = 1.10002 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertEqual(1, len(orders))
            self.assertEqual(1000, orders[0].order_qty)

            self.int_bt.lmax_portfolio.on_trade(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=self.lmax_account,
                    contract_qty=orders[0].order_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

        # we can now internalise client short trade up to our positin limit
        for account in [1]:
            contract_qty = 2 * 100
            order_size = 2 * 100
            price = 1.10003 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertEqual(1, len(orders))
            self.assertEqual(-200, orders[0].order_qty)


class InternaliationTradeSignalsCurrencyLimits(unittest.TestCase):
    def setUp(self):
        self.lmax_account = 4
        self.stoploss_limit = 100
        self.takeprofit_limit = 10000
        self.unit_price = 1000
        self.pip_size = 0.00001
        self.instrument_id = 1001
        self.currency = "EUR"
        self.venue = 1
        self.symbol = "symbol1"

        self.int_bt = Backtester(
            NoRisk(),
            InternalisationStrategy(
                account_id=self.lmax_account,
                max_pos_qty={self.currency: 400},
                max_pos_qty_buffer=1.0,
                max_pos_qty_level="currency",
                allow_partial_fills=True,
                exit_strategy=ExitDefault(),
            ),
            store_client_md_snapshot=True,
        )
        self.int_bt.strategy._max_pos_qty = {
            k: v * 1e2 for (k, v) in self.int_bt.strategy.max_pos_qty.items()
        }
        self.int_bt.process_client_portfolio = True

    def test_take_on_partial_short_position_up_to_position_limit(self):
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    currency=self.currency,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                currency=self.currency,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=1000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(-100, order.order_qty)
            self.assertEqual(-100, order.unfilled_qty)
            self.assertEqual("internal", order.event_type)
            self.assertEqual(event.symbol, order.symbol)
            self.assertEqual(event.price, order.price)
            self.assertEqual(4, order.account_id)
            self.assertEqual("K", order.time_in_force)
            self.assertEqual("N", order.order_type)
            self.assertEqual("client_long_open_short", order.signal)

    def test_take_on_partial_long_position_up_to_position_limit(self):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    contract_unit_of_measure=self.currency,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 100)
            self.assertEqual(order.unfilled_qty, 100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_take_on_partial_short_position_up_to_position_limit_different_instrument(
            self,
    ):
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=1234566,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    currency=self.currency,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                currency=self.currency,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=1000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(-100, order.order_qty)
            self.assertEqual(-100, order.unfilled_qty)
            self.assertEqual("internal", order.event_type)
            self.assertEqual(event.symbol, order.symbol)
            self.assertEqual(event.price, order.price)
            self.assertEqual(4, order.account_id)
            self.assertEqual("K", order.time_in_force)
            self.assertEqual("N", order.order_type)
            self.assertEqual("client_long_open_short", order.signal)

    def test_take_on_partial_long_position_up_to_position_limit_different_instrument(
            self,
    ):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=9877589,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    contract_unit_of_measure=self.currency,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 100)
            self.assertEqual(order.unfilled_qty, 100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_reject_due_to_order_size_on_short_position_exceeding_position_limit(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                contract_unit_of_measure=self.currency,
                price=price,
                event_type="trade",
                order_qty=10000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_reject_due_to_order_size_on_long_position_exceeding_position_limit(self):
        self.int_bt.strategy.allow_partial_fills = False

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                contract_unit_of_measure=self.currency,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_reject_due_to_order_size_on_short_position_on_different_instrument_exceeding_position_limit(
            self,
    ):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=123456,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                contract_unit_of_measure=self.currency,
                price=price,
                event_type="trade",
                order_qty=10000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_reject_due_to_order_size_on_long_position_on_different_instrument_exceeding_position_limit(
            self,
    ):
        self.int_bt.strategy.allow_partial_fills = False

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=203045,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                contract_unit_of_measure=self.currency,
                symbol=self.symbol,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_accept_trade_on_short_position_as_it_is_the_same_order(self):
        order_id: int = 2

        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.last_order_id = order_id
        self.int_bt.strategy.last_order_filled = True

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_id=order_id,
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=10000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, -110)
            self.assertEqual(order.unfilled_qty, -110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_accept_trade_on_long_position_as_it_is_the_same_order(self):
        order_id: int = 2

        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.last_order_id = order_id
        self.int_bt.strategy.last_order_filled = True

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 110)
            self.assertEqual(order.unfilled_qty, 110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_reject_trade_on_short_position_when_position_in_buffer(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-100.1 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                contract_unit_of_measure=self.currency,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 0.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_id=1,
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=contract_qty,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_reject_trade_on_long_position_when_position_in_buffer(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=100.1 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = -0.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_id=1,
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=contract_qty,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertEqual([], orders)

    def test_accept_trade_on_short_position_due_to_buffer(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_id=1,
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=contract_qty,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, -110)
            self.assertEqual(order.unfilled_qty, -110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_accept_trade_on_long_position_due_to_buffer(self):
        self.int_bt.strategy.allow_partial_fills = False
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=contract_qty,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 110)
            self.assertEqual(order.unfilled_qty, 110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_take_on_partial_short_position_due_to_internalisation_risk(self):
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=1000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                internalisation_risk=0.5,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, -50)
            self.assertEqual(order.unfilled_qty, -50)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_take_on_partial_long_position_due_to_internalisation_risk(self):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        for account in [1]:
            contract_qty = -1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                internalisation_risk=0.5,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 50)
            self.assertEqual(order.unfilled_qty, 50)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_take_on_short_position_up_to_position_limit_plus_buffer(self):
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.instrument_id)
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=1000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, -110)
            self.assertEqual(order.unfilled_qty, -110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

        # self.assertIsInstance(orders, List)
        # self.assertEqual(orders, [])

    def test_take_on_long_position_up_to_position_limit_plus_buffer(self):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)
        self.int_bt.strategy.max_pos_qty_buffer = 1.1

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )
        for order in orders:
            self.assertEqual(order.order_qty, 110)
            self.assertEqual(order.unfilled_qty, 110)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_dont_take_on_short_position_as_we_are_already_at_our_position_limit(self):
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-100 * 100,
                price=1.12345,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=1000 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_dont_take_on_long_position_as_we_are_already_at_our_position_limit(self):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=100 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -1.1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_take_on_full_short_position_that_is_bigger_than_position_limit_as_are_long(
            self,
    ):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = 120 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.currency,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=190 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, -120 * 100)
            self.assertEqual(order.unfilled_qty, -120 * 100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_take_on_full_long_position_that_is_bigger_than_position_limit_as_are_short(
            self,
    ):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -120 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=-190 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, 120 * 100)
            self.assertEqual(order.unfilled_qty, 120 * 100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_take_on_full_short_position_that_is_bigger_than_position_limit_as_are_long_different_instrument(
            self,
    ):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=23453,
                account_id=self.lmax_account,
                contract_qty=50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = 120 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.currency,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=190 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, -120 * 100)
            self.assertEqual(order.unfilled_qty, -120 * 100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_take_on_full_long_position_that_is_bigger_than_position_limit_as_are_short_different_instrument(
            self,
    ):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=49325,
                account_id=self.lmax_account,
                contract_qty=-50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -120 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=-190 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, 120 * 100)
            self.assertEqual(order.unfilled_qty, 120 * 100)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_dont_open_long_pos_on_client_open_short_pos_as_order_not_immediate(self):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1] * 2:
            contract_qty = -70 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    contract_unit_of_measure=self.currency,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=-180 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                internalise_limit_orders=0,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=0,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_take_on_limit_order_if_allow_limit_tag_is_enabled(self):
        # client long pos triggers lmax to open short position

        self.int_bt.strategy.update_max_pos_qty(value=100, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-50 * 100,
                price=1.10001 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1] * 2:
            contract_qty = -70 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                    internalise_limit_orders=1,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=-180 * 100,
                ask_price=price,
                tob_snapshot_ask_price=price,
                internalise_limit_orders=1,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=0,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, event.contract_qty * -1)
            self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_open_short_pos_on_client_open_long_pos(self):
        # client long pos triggers lmax to open short position
        for account in [1]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.add_position(event)

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, event.contract_qty * -1)
            self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_long_open_short")

    def test_open_long_pos_on_client_open_short_pos(self):
        # test client short position cause lmax to open long position
        for account in [1]:
            contract_qty = -1.1 * 100
            price = 1.10000 * 1000000

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.add_position(event)

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, event.contract_qty * -1)
            self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.price, event.price)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "client_short_open_long")

    def test_take_on_partial_long_pos_on_client_open_short_upto_position_limit(self):
        # test that because client short position exceeds position limit, lmax
        # do not open a long position
        self.int_bt.strategy.update_max_pos_qty(value=4, key=self.currency)

        for account in [1]:
            contract_qty = -8 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        for order in orders:
            self.assertEqual(order.order_qty, 4 * 100)

    def test_dont_take_on_long_pos_on_client_open_short_as_we_are_at_position_limit(
            self,
    ):
        # test that because client short position exceeds position limit, lmax
        # do not open a long position
        self.int_bt.strategy.update_max_pos_qty(value=4, key=self.currency)

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=4 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        for account in [1]:
            contract_qty = -8 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

        orders = self.int_bt.strategy.calculate_trade_signals(
            self.int_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_client_short_position_closes_lmax_open_short_position(self):
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account = 1
        lmax_account = 4
        contract_qty = int(1.1 * 100)

        # add a long client pos and the corresponding short lmax pos
        self.int_bt.client_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=client_account,
                contract_qty=contract_qty,
                price=open_price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=lmax_account,
                contract_qty=contract_qty * -1,
                price=open_price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # new short client pos come in that closes short lmax pos
        # and close the client account position
        for account, c_qty, close_price in zip(
                [1], [contract_qty * -1], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_short_open_long")

    def test_multiple_client_short_position_closes_lmax_short_position(self):
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account_a = 1
        client_account_b = 2
        lmax_account = 4
        contract_qty = int(1.1 * 100)

        # add a long client pos and the corresponding short lmax pos
        for account in [client_account_a, client_account_b]:
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            self.int_bt.lmax_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=lmax_account,
                    contract_qty=contract_qty * -1,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

        # new short client pos come in that closes short lmax pos
        # reduces client long position
        for account, c_qty, close_price in zip(
                [1], [contract_qty * -1], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_short_open_long")

    def test_client_long_position_closes_lmax_long_position(self):
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account = 1
        lmax_account = 4
        contract_qty = int(-1.1 * 100)

        # add a short client pos and the corresponding long lmax pos
        self.int_bt.client_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=client_account,
                contract_qty=contract_qty,
                price=open_price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=lmax_account,
                contract_qty=contract_qty * -1,
                price=open_price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        # new long client pos come in that closes long lmax pos
        # and long the client account position
        for account, c_qty, close_price in zip(
                [1], [contract_qty * -1], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_long_open_short")

    def test_multiple_client_long_position_closes_lmax_long_position(self):
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account_a = 1
        client_account_b = 2
        lmax_account = 4
        contract_qty = int(-1.1 * 100)

        # add a short client pos and the corresponding long lmax pos
        for account in [client_account_a, client_account_b]:
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )
            self.int_bt.lmax_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=lmax_account,
                    contract_qty=contract_qty * -1,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

        # new long client pos come in that closes long lmax pos
        # reduces client short position
        for account, c_qty, close_price in zip(
                [1], [contract_qty * -1], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )
            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_long_open_short")

    def test_client_long_position_open_short_lmax_position_when_lmax_has_multi_positions(
            self,
    ):
        "test to see if position open even if the total portfolio position exceed the position limit for a single inst"
        ts = 1536192000000000
        open_price = 1.10000 * 1000000
        client_account = 1
        lmax_account = 4
        contract_qty = int(11 * 100)

        # add a long client pos and the corresponding short lmax pos
        for ob in [self.instrument_id, 222, 223, 224, 225]:
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=ob,
                    account_id=client_account,
                    contract_qty=contract_qty,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            self.int_bt.lmax_portfolio.add_position(
                Event(
                    order_book_id=ob,
                    account_id=lmax_account,
                    contract_qty=contract_qty * -1,
                    price=open_price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

        for account, c_qty, close_price in zip(
                [1], [contract_qty], [1.10050 * 1000000]
        ):
            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=ts,
                account_id=account,
                contract_qty=c_qty,
                price=close_price,
                event_type="trade",
                ask_price=close_price,
                tob_snapshot_ask_price=close_price,
                ask_qty=0,
                bid_price=close_price,
                tob_snapshot_bid_price=close_price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            self.int_bt.client_portfolio.modify_position(event)

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertTrue(orders.__len__() != 0)

            for order in orders:
                self.assertEqual(order.order_qty, event.contract_qty * -1)
                self.assertEqual(order.unfilled_qty, event.contract_qty * -1)
                self.assertEqual(order.event_type, "internal")
                self.assertEqual(order.symbol, event.symbol)
                self.assertEqual(order.price, event.price)
                self.assertEqual(order.account_id, 4)
                self.assertEqual(order.time_in_force, "K")
                self.assertEqual(order.order_type, "N")
                self.assertEqual(order.signal, "client_long_open_short")

    # test for behaviour after position limit has been updated

    def test_dont_take_on_long_position_after_rebalance_contracts_means_we_breach_position_limit_until_we_are_back_below_limit(
            self,
    ):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        day = dt.datetime(2021, 10, 12)
        self.int_bt.strategy.max_pos_qty_type = "dollars"
        self.int_bt.strategy._next_rebalance_date = day
        self.int_bt.strategy.max_pos_qty_buffer = 1
        self.int_bt.strategy.max_pos_qty = {self.currency: 1e6}
        self.int_bt.strategy._max_pos_qty = {self.currency: 1e2 * 1e2}

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        self.int_bt.strategy.update(
            shard="ldprof",
            date=day,
            instruments=[self.instrument_id],
            dataserver=DataServerDummy(
                rate=1.1, contract_unit_of_measure=self.currency
            ),
        )

        # cannot take on client short trade as we are now over our position limit due to a change in instrument rate
        for account in [1]:
            contract_qty = -1 * 100
            order_size = -1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )
            self.assertIsInstance(orders, List)
            self.assertEqual(orders, [])

        # client long trade reduces our position below our new position limit
        for account in [1]:
            contract_qty = 10 * 100
            order_size = 10 * 100
            price = 1.10002 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertEqual(1, len(orders))
            self.assertEqual(-1000, orders[0].order_qty)

            self.int_bt.lmax_portfolio.on_trade(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=self.lmax_account,
                    contract_qty=orders[0].order_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

        # we can now internalise client short trade up to our position limit
        for account in [1]:
            contract_qty = -2 * 100
            order_size = -2 * 100
            price = 1.10003 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertEqual(1, len(orders))
            self.assertEqual(200, orders[0].order_qty)

    def test_dont_take_on_short_position_after_rebalance_contracts_means_we_breach_position_limit_until_we_are_back_below_limit(
            self,
    ):
        # we should open a position with a partial position of 1 contract to reach our limit

        # client long pos triggers lmax to open short position
        day = dt.datetime(2021, 10, 12)
        self.int_bt.strategy.max_pos_qty_type = "dollars"
        self.int_bt.strategy._next_rebalance_date = day
        self.int_bt.strategy.max_pos_qty_buffer = 1
        self.int_bt.strategy.max_pos_qty = {self.currency: 1e6}
        self.int_bt.strategy._max_pos_qty = {self.currency: 100}

        # add initial position for the lmax position
        self.int_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=self.lmax_account,
                contract_qty=-99 * 100,
                price=1.09000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                venue=self.venue,
            )
        )

        self.int_bt.strategy.update(
            shard="ldprof",
            date=day,
            instruments=[self.instrument_id],
            dataserver=DataServerDummy(
                rate=1.1, contract_unit_of_measure=self.currency
            ),
        )

        # cannot take on client short trade as we are now over our position limit due to a change in instrument rate
        for account in [1]:
            contract_qty = 1 * 100
            order_size = 1000 * 100
            price = 1.10000 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )
            self.assertIsInstance(orders, List)
            self.assertEqual([], orders)

        # client long trade reduces our position below our new position limit
        for account in [1]:
            contract_qty = -10 * 100
            order_size = -10 * 100
            price = 1.10002 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertEqual(1, len(orders))
            self.assertEqual(1000, orders[0].order_qty)

            self.int_bt.lmax_portfolio.on_trade(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=self.lmax_account,
                    contract_qty=orders[0].order_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

        # we can now internalise client short trade up to our position limit
        for account in [1]:
            contract_qty = 2 * 100
            order_size = 2 * 100
            price = 1.10003 * 1000000
            self.int_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=price,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    contract_unit_of_measure=self.currency,
                    venue=self.venue,
                )
            )

            event = Event(
                order_book_id=self.instrument_id,
                unit_price=self.unit_price,
                symbol=self.symbol,
                contract_unit_of_measure=self.currency,
                price_increment=self.pip_size,
                timestamp=1536192000000000,
                account_id=account,
                contract_qty=contract_qty,
                price=price,
                event_type="trade",
                order_qty=order_size,
                ask_price=price,
                tob_snapshot_ask_price=price,
                ask_qty=0,
                bid_price=price,
                tob_snapshot_bid_price=price,
                bid_qty=0,
                immediate_order=1,
                venue=self.venue,
            )

            orders = self.int_bt.strategy.calculate_trade_signals(
                self.int_bt.lmax_portfolio, event
            )

            self.assertIsInstance(orders, List)
            self.assertEqual(1, len(orders))
            self.assertEqual(-200, orders[0].order_qty)


if __name__ == "__main__":
    unittest.main()

