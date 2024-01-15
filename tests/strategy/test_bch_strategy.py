import unittest
from typing import Any, Dict, List

import pandas as pd

from risk_backtesting.backtester import Backtester
from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.config.simulation_config import SimulationConfig
from risk_backtesting.event import Event
from risk_backtesting.exit_strategy.aggressive import Aggressive
from risk_backtesting.risk_manager.no_risk import NoRisk
from risk_backtesting.simulator.simulation_plan import SimulationPlan
from risk_backtesting.simulator.simulations import Simulations
from risk_backtesting.strategy import AbstractStrategy, create_strategy
from risk_backtesting.strategy.bch_strategy import BCHStrategy


class BchPlanBuilder(unittest.TestCase):
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
                "strategy_parameters": {
                    "strategy_type": "bch",
                    "min_directional_consensus": 0.7,
                    "min_consensus_buffer_factor": 0.8,
                    "max_ratio_per_position": 0.6,
                    "max_ratio_buffer_factor": 1.2,
                    "position_trigger": 250,
                    "position_buffer_factor": 0.9,
                    "follow_client": False,
                    "event_filter_string": None,
                },
                "exit_parameters": {
                    "exit_type": "aggressive",
                    "takeprofit_limit": 50,
                    "stoploss_limit": 70,
                },
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

        self.assertIsInstance(plan.strategy, BCHStrategy)
        self.assertIsInstance(plan.strategy.exit_strategy, Aggressive)
        self.assertEqual(plan.hash, "1d053b5bdfb00c0ee6741b0bdee8bc47")
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


class BchEntrySignals(unittest.TestCase):
    def setUp(self):
        self.unit_price = 1000
        self.pip_size = 0.00001
        self.instrument_id = 111
        self.venue = 1
        self.symbol = "symbol1"

        self.bch_bt = Backtester(
            risk_manager=NoRisk(),
            strategy=BCHStrategy(
                account_id=4,
                min_directional_consensus=0.6,
                min_consensus_buffer_factor=0.8,
                max_ratio_per_position=0.6,
                max_ratio_buffer_factor=1.2,
                position_trigger=3,
                position_buffer_factor=0.7,
                exit_strategy=Aggressive(stoploss_limit=100, takeprofit_limit=200),
            ),
            matching_method="side_of_book",
        )

    def test_open_short_pos_on_client_open_long_pos(self):

        for account in [1, 2, 3]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.bch_bt.client_portfolio.add_position(
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
                ask_qty=0,
                bid_price=price,
                bid_qty=0,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(order.order_qty, round(-0.6 * 100))
            self.assertEqual(order.unfilled_qty, round(-0.6 * 100))
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "open_short_pos")

    def test_open_long_pos_on_client_open_long_pos(self):

        for qty, account in zip([-2, -3, -2], [1, 1, 3]):
            contract_qty = qty * 100
            price = 1.10000 * 1000000
            self.bch_bt.client_portfolio.add_position(
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
                account_id=1,
                contract_qty=3.5,
                price=price,
                event_type="trade",
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(order.order_qty, round(0.6 * 100))
            self.assertEqual(order.unfilled_qty, round(0.6 * 100))
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "open_long_pos")

    def test_open_short_pos_on_client_last_event_short(self):

        # first, all client account are long but ratio too high

        for account, contract_qty in zip([1, 2, 3], [100, 2, 1]):
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.add_position(
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
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        # check that lmax is not going to open any position
        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

        # now the client account ratio falls to an acceptable level

        for account, contract_qty in zip([1], [-99]):
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.modify_position(
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
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        # now lmax get signal to take a short position
        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(order.order_qty, round(-0.6 * 100))
            self.assertEqual(order.unfilled_qty, round(-0.6 * 100))
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "open_short_pos")

    def test_open_long_pos_on_client_last_event_long(self):

        # first, all client account are long but ratio too high

        for account, contract_qty in zip([1, 2, 3], [-100, -2, -1]):
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.add_position(
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
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        # check that lmax is not going to open any position
        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

        # now the client account ratio falls to an acceptable level

        for account, contract_qty in zip([1], [99]):
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.modify_position(
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
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        # now lmax get signal to take a short position
        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(order.order_qty, round(0.6 * 100))
            self.assertEqual(order.unfilled_qty, round(0.6 * 100))
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "open_long_pos")

    def test_not_open_short_pos_due_to_net_false(self):

        for account in [1, 2, 3]:
            contract_qty = 0.5
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.add_position(
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
                account_id=1,
                contract_qty=1,
                price=price,
                event_type="trade",
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_open_short_pos_due_to_concensus_false(self):

        for account, contract_qty in zip([1, 2, 3, 5, 6, 7], [-1, -1, -1, -1, 4, 5]):
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.add_position(
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
                account_id=1,
                contract_qty=1,
                price=price,
                event_type="trade",
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_open_short_pos_due_to_ratio_false(self):

        for account, contract_qty in zip([1, 2, 3], [1, 1, 10]):
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.add_position(
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
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_open_long_pos_due_to_net_false(self):

        for account in [1, 2, 3]:
            contract_qty = -0.5
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.add_position(
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
            contract_qty=1,
            price=price,
            event_type="trade",
            ask_price=1.2,
            ask_qty=1,
            bid_price=1.0,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_open_long_pos_due_to_concensus_false(self):

        for account, contract_qty in zip(
                [1, 2, 3, 5, 6, 7], [x * -1 for x in [-1, -1, -1, -1, 4, 5]]
        ):
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.add_position(
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
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_open_long_pos_due_to_ratio_false(self):

        for account, contract_qty in zip([1, 2, 3], [-1, -1, -10]):
            price = 1.10000 * 1000000
            contract_qty = contract_qty * 100
            self.bch_bt.client_portfolio.add_position(
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
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_open_long_pos_if_lmax_port_long(self):
        contract_qty = 1 * 100
        price = 1.10000 * 1000000
        lmax_account = 4
        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=lmax_account,
                contract_qty=contract_qty,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1, 2, 3]:
            self.bch_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty * -1,
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
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_open_short_pos_if_lmax_port_short(self):
        contract_qty = -1
        price = 1.10000 * 1000000
        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=contract_qty,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account in [1, 2, 3]:
            contract_qty = 1.1 * 100
            price = 1.10000 * 1000000
            self.bch_bt.client_portfolio.add_position(
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
                contract_qty=-1,
                price=price,
                event_type="trade",
                ask_price=1.2,
                ask_qty=1,
                bid_price=1.0,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)


class BchExitSignals(unittest.TestCase):
    def setUp(self):
        self.unit_price = 1000
        self.pip_size = 0.00001
        self.instrument_id = 111
        self.venue = 1
        self.symbol = "symbol1"

        self.bch_bt = Backtester(
            risk_manager=NoRisk(),
            strategy=BCHStrategy(
                account_id=4,
                min_directional_consensus=0.6,
                min_consensus_buffer_factor=0.8,
                max_ratio_per_position=0.6,
                max_ratio_buffer_factor=1.2,
                position_trigger=3,
                position_buffer_factor=0.7,
                exit_strategy=Aggressive(stoploss_limit=100, takeprofit_limit=200),
            ),
            matching_method="side_of_book",
        )

    def test_not_close_short_pos_all_cond_still_hold(self):
        price = 1.10000 * 1000000

        for account, contract_qty in zip([1, 2, 3], [1] * 3):
            contract_qty = contract_qty * 100

            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        account, qty, price = 5, 1 * 100, 1.10000 * 1000000
        self.bch_bt.client_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=account,
                contract_qty=qty,
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
            contract_qty=qty,
            price=price,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_close_long_pos_all_cond_still_hold(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [-1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        account, qty, price = 1, -1 * 100, 1.10000 * 1000000
        self.bch_bt.client_portfolio.modify_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=account,
                contract_qty=qty,
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
            contract_qty=qty,
            price=price,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_close_short_pos_due_to_net_buffer(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        account, qty = 5, -0.9 * 100
        self.bch_bt.client_portfolio.add_position(
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
            contract_qty=qty,
            price=price,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_close_short_pos_due_to_concensus_buffer(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account, contract_qty in zip([5, 6, 7], [-0.1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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
                ask_price=1.2 * 1000000,
                ask_qty=1,
                bid_price=1.0 * 1000000,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_close_short_pos_due_to_ratio_buffer(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # this turns position ratio to between .6 and buffer .72
        account, qty = 1, 3.5 * 100
        self.bch_bt.client_portfolio.modify_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=account,
                contract_qty=qty,
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
            contract_qty=qty,
            price=price,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_close_long_pos_due_to_net_buffer(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [-1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        account, qty = 5, 0.9 * 100
        self.bch_bt.client_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=account,
                contract_qty=qty,
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
            contract_qty=qty,
            price=price,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_close_long_pos_due_to_concensus_buffer(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [-1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account, contract_qty in zip([5, 6, 7], [0.1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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
                ask_price=1.2 * 1000000,
                ask_qty=1,
                bid_price=1.0 * 1000000,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_not_close_long_pos_due_to_ratio_buffer(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [-1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # this turns position ratio to between .6 and buffer .72
        account, qty = 1, -3.5 * 100
        self.bch_bt.client_portfolio.modify_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=account,
                contract_qty=qty,
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
            contract_qty=qty,
            price=price,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 0)

    def test_close_short_pos_due_to_net_false(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        account, qty = 1, -1 * 100
        self.bch_bt.client_portfolio.modify_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=account,
                contract_qty=qty,
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
            contract_qty=qty,
            price=price,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(
                order.order_qty, round(self.bch_bt.strategy.position_trigger * 100)
            )
            self.assertEqual(
                order.unfilled_qty, round(self.bch_bt.strategy.position_trigger * 100)
            )
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "close_short_pos")

    def test_close_short_pos_due_to_concensus_false(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account, contract_qty in zip([5, 6, 7, 8], [-0.1 * 100] * 4):
            self.bch_bt.client_portfolio.add_position(
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
                ask_price=1.2 * 1000000,
                ask_qty=1,
                bid_price=1.0 * 1000000,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(
                order.order_qty, round(self.bch_bt.strategy.position_trigger * 100)
            )
            self.assertEqual(
                order.unfilled_qty, round(self.bch_bt.strategy.position_trigger * 100)
            )
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "close_short_pos")

    def test_close_short_pos_due_to_ratio_false(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # this turns position ratio to between .6 and buffer .72
        account, qty = 1, 5.5 * 100
        self.bch_bt.client_portfolio.modify_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=account,
                contract_qty=qty,
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
            contract_qty=qty,
            price=price,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(
                order.order_qty, round(self.bch_bt.strategy.position_trigger * 100)
            )
            self.assertEqual(
                order.unfilled_qty, round(self.bch_bt.strategy.position_trigger * 100)
            )
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "close_short_pos")

    def test_close_long_pos_due_to_net_false(self):
        price = 1.10000 * 1000000
        for account, contract_qty in zip([1, 2, 3], [-1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
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

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=3 * 100,
                price=price,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        account, qty = 5, 3 * 100
        self.bch_bt.client_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=account,
                contract_qty=qty,
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
            contract_qty=qty,
            price=price,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(
                order.order_qty, round(self.bch_bt.strategy.position_trigger * 100) * -1
            )
            self.assertEqual(
                order.unfilled_qty,
                round(self.bch_bt.strategy.position_trigger * 100) * -1,
                )
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "close_long_pos")

    def test_close_long_pos_due_to_concensus_false(self):

        for account, contract_qty in zip([1, 2, 3], [-1 * 100] * 3):
            self.bch_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty,
                    price=1.10000 * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=3 * 100,
                price=1.10000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        for account, contract_qty in zip([5, 6, 7, 8], [0.1] * 4):
            self.bch_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty * 100,
                    price=1.10000 * 1000000,
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
                price=1.10000 * 1000000,
                event_type="trade",
                ask_price=1.2 * 1000000,
                ask_qty=1,
                bid_price=1.0 * 1000000,
                bid_qty=1,
                venue=self.venue,
            )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(
                order.order_qty, round(self.bch_bt.strategy.position_trigger * 100) * -1
            )
            self.assertEqual(
                order.unfilled_qty,
                round(self.bch_bt.strategy.position_trigger * 100) * -1,
                )
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "close_long_pos")

    def test_close_long_pos_due_to_ratio_false(self):

        for account, contract_qty in zip([1, 2, 3], [-1] * 3):
            self.bch_bt.client_portfolio.add_position(
                Event(
                    order_book_id=self.instrument_id,
                    account_id=account,
                    contract_qty=contract_qty * 100,
                    price=1.10000 * 1000000,
                    unit_price=self.unit_price,
                    price_increment=self.pip_size,
                    symbol=self.symbol,
                    venue=self.venue,
                )
            )

        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=3,
                contract_qty=3 * 100,
                price=1.10000 * 1000000,
                unit_price=self.unit_price,
                price_increment=self.pip_size,
                symbol=self.symbol,
                venue=self.venue,
            )
        )

        # this turns position ratio to between .6 and buffer .72
        account, qty = 1, -4.5
        self.bch_bt.client_portfolio.modify_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=account,
                contract_qty=qty * 100,
                price=1.10000 * 1000000,
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
            contract_qty=qty,
            price=1.10000 * 1000000,
            event_type="trade",
            ask_price=1.2 * 1000000,
            ask_qty=1,
            bid_price=1.0 * 1000000,
            bid_qty=1,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.on_state(
            self.bch_bt.client_portfolio, self.bch_bt.lmax_portfolio, event
        )

        self.assertEqual(len(orders), 1)

        for order in orders:
            self.assertEqual(
                order.order_qty, round(self.bch_bt.strategy.position_trigger * 100) * -1
            )
            self.assertEqual(
                order.unfilled_qty,
                round(self.bch_bt.strategy.position_trigger * 100) * -1,
                )
            self.assertEqual(order.event_type, "internal")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "N")
            self.assertEqual(order.signal, "close_long_pos")


class BchMarketSignals(unittest.TestCase):
    def setUp(self):
        self.unit_price = 1000
        self.pip_size = 0.00001
        self.instrument_id = 111
        self.venue = 1
        self.symbol = "symbol1"

        self.bch_bt = Backtester(
            risk_manager=NoRisk(),
            strategy=BCHStrategy(
                account_id=4,
                min_directional_consensus=0.6,
                min_consensus_buffer_factor=0.8,
                max_ratio_per_position=0.6,
                max_ratio_buffer_factor=1.2,
                position_trigger=3,
                position_buffer_factor=0.7,
                exit_strategy=Aggressive(stoploss_limit=100, takeprofit_limit=200),
            ),
            matching_method="side_of_book",
        )

    def exit_market_signal_if_LMAX_flat(self):
        self.bch_bt.strategy.stoploss_limit = 10
        self.bch_bt.strategy.matching_method = "side_of_book"

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

        orders = self.bch_bt.strategy.calculate_trade_signals(
            self.bch_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_market_signal_SL_closes_LMAX_long(self):

        # test that SL triggers when SL is exceeded by market data tick
        self.bch_bt.strategy.position_lifespan = None
        self.bch_bt.strategy.exit_strategy.stoploss_limit = 10
        self.bch_bt.strategy.matching_method = "side_of_book"
        self.bch_bt.client_portfolio.matching_method = "side_of_book"
        self.bch_bt.lmax_portfolio.matching_method = "side_of_book"
        self.bch_bt.lmax_portfolio.add_position(
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

        orders = self.bch_bt.strategy.calculate_market_signals(
            self.bch_bt.lmax_portfolio, event
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

    def test_market_signal_SL_closes_LMAX_short(self):

        # test that SL triggers when SL is exceeded by market data tick
        self.bch_bt.strategy.position_lifespan = None
        self.bch_bt.strategy.exit_strategy.stoploss_limit = 10
        self.bch_bt.strategy.matching_method = "side_of_book"
        self.bch_bt.client_portfolio.matching_method = "side_of_book"
        self.bch_bt.lmax_portfolio.matching_method = "side_of_book"
        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-1.1 * 100,
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
            ask_price=1.40360 * 1000000,
            tob_snapshot_ask_price=1.40360 * 1000000,
            ask_qty=1000,
            bid_price=1.40340 * 1000000,
            tob_snapshot_bid_price=1.40340 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.calculate_market_signals(
            self.bch_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, round(1.1 * 100))
            self.assertEqual(order.unfilled_qty, round(1.1 * 100))
            self.assertEqual(order.event_type, "hedge")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "S")
            self.assertEqual(order.signal, "SL_close_position")

    def test_market_signal_SL_isnt_triggered_when_LMAX_has_position(self):

        self.bch_bt.strategy.exit_strategy.stoploss_limit = 10
        self.bch_bt.strategy.matching_method = "side_of_book"

        self.bch_bt.lmax_portfolio.add_position(
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

        orders = self.bch_bt.strategy.calculate_market_signals(
            self.bch_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertEqual(orders, [])

    def test_market_signal_TP_closes_LMAX_long(self):

        # test that SL triggers when SL is exceeded by market data tick
        self.bch_bt.strategy.position_lifespan = None
        self.bch_bt.strategy.exit_strategy.takeprofit_limit = 10
        self.bch_bt.strategy.matching_method = "side_of_book"
        self.bch_bt.client_portfolio.matching_method = "side_of_book"
        self.bch_bt.lmax_portfolio.matching_method = "side_of_book"
        self.bch_bt.lmax_portfolio.add_position(
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
            ask_price=1.40342 * 1000000,
            tob_snapshot_ask_price=1.40342 * 1000000,
            ask_qty=1000,
            bid_price=1.40340 * 1000000,
            tob_snapshot_bid_price=1.40340 * 1000000,
            price=0,
            bid_qty=1000,
            venue=self.venue,
        )

        orders = self.bch_bt.strategy.calculate_market_signals(
            self.bch_bt.lmax_portfolio, event
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
            self.assertEqual(order.order_type, "R")
            self.assertEqual(order.signal, "TP_close_position")

    def test_market_signal_TP_closes_LMAX_short(self):

        # test that SL triggers when SL is exceeded by market data tick
        self.bch_bt.strategy.position_lifespan = None
        self.bch_bt.strategy.exit_strategy.takeprofit_limit = 10
        self.bch_bt.strategy.matching_method = "side_of_book"
        self.bch_bt.client_portfolio.matching_method = "side_of_book"
        self.bch_bt.lmax_portfolio.matching_method = "side_of_book"
        self.bch_bt.lmax_portfolio.add_position(
            Event(
                order_book_id=self.instrument_id,
                account_id=4,
                contract_qty=-1.1 * 100,
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

        orders = self.bch_bt.strategy.calculate_market_signals(
            self.bch_bt.lmax_portfolio, event
        )

        self.assertIsInstance(orders, List)
        self.assertTrue(orders.__len__() != 0)

        for order in orders:
            self.assertEqual(order.order_qty, round(1.1 * 100))
            self.assertEqual(order.unfilled_qty, round(1.1 * 100))
            self.assertEqual(order.event_type, "hedge")
            self.assertEqual(order.symbol, event.symbol)
            self.assertEqual(order.account_id, 4)
            self.assertEqual(order.time_in_force, "K")
            self.assertEqual(order.order_type, "R")
            self.assertEqual(order.signal, "TP_close_position")


if __name__ == "__main__":
    unittest.main()
