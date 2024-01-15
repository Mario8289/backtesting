import os
import shutil
import unittest
from tempfile import mkdtemp
from typing import Any, Dict, List

import pandas as pd
import yaml

from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.config.backtesting_config_factory import BackTestingConfigFactory
from risk_backtesting.filesystems import FILESYSTEM_TYPE_LOCAL


# noinspection SpellCheckingInspection
class TestBacktestConfigFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory: BackTestingConfigFactory = BackTestingConfigFactory()

        self.scenario: str = "bob"

        self.temp_dir: str = mkdtemp()
        self.auth_path: str = os.path.join(self.temp_dir, "auth.yaml")
        self.scenario_path: str = os.path.join(self.temp_dir, "scenarios")
        self.bob_scenario_path: str = os.path.join(self.scenario_path, self.scenario)
        os.makedirs(self.bob_scenario_path, exist_ok=True)

        self.pipeline_path: str = os.path.join(self.bob_scenario_path, "pipeline.yaml")
        self.simulations_path: str = os.path.join(
            self.bob_scenario_path, "simulations.yaml"
        )
        self.output_path: str = os.path.join(self.bob_scenario_path, "output.yaml")
        self.target_accounts_path: str = os.path.join(
            self.bob_scenario_path, "target_accounts.csv"
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    # noinspection PyMethodMayBeStatic
    def write_yaml(self, content: Dict[str, Any], path: str):
        with open(path, "w") as yml:
            yaml.dump(content, yml)

    # noinspection PyMethodMayBeStatic
    def write_lines(self, content: List[str], path: str):
        with open(path, "w") as fil:
            for line in content:
                fil.write(f"{line}\n")

    def write_auth(self, nomad: bool = False):

        if nomad:
            self.write_yaml(
                {
                    "creds": {
                        "minio": {"export": {"key": "nomad", "secret": "something"}},
                        "dataserver": {"token": "something"},
                    },
                    "uris": {
                        "minio": {
                            "export": {"blue": "somewhere", "green": "somewhere"},
                            "archive": {"blue": "somewhere", "green": "somewhere"},
                        },
                        "dataserver": {"blue": "somewhere", "green": "somewhere"},
                    },
                },
                self.auth_path,
            )
        else:
            self.write_yaml(
                {
                    "input": {"minio": {"key": "something", "secret": "or-other"}},
                    "output": {"minio": {"key": "something", "secret": "or-other"}},
                    "dataserver": {"username": "someone", "password": "something"},
                },
                self.auth_path,
            )

    def test_write_auth_from_jupyter(self):
        self.write_auth(nomad=False)
        auth = self.factory._build_auth(
            auth_path=self.auth_path,
            minio_uri=None,
            dataserver_uri=None,
            colour="green",
            nomad=False,
        )
        self.assertEqual(
            {
                "input": {"minio": {"key": "something", "secret": "or-other"}},
                "output": {"minio": {"key": "something", "secret": "or-other"}},
                "dataserver": {"username": "someone", "password": "something"},
            },
            auth,
        )

    def test_write_auth_from_nomad(self):
        self.write_auth(nomad=True)
        auth = self.factory._build_auth(
            auth_path=self.auth_path,
            minio_uri=None,
            dataserver_uri=None,
            colour="green",
            nomad=True,
        )
        self.assertEqual(
            {
                "input": {
                    "minio": {"key": "nomad", "secret": "something", "uri": "somewhere"}
                },
                "output": {
                    "minio": {"key": "nomad", "secret": "something", "uri": "somewhere"}
                },
                "config": {
                    "minio": {"key": "nomad", "secret": "something", "uri": "somewhere"}
                },
                "dataserver": {"token": "something", "uri": "somewhere"},
            },
            auth,
        )

    def test_should_load_config_paths_from_scenario(self):
        # noinspection PyTypeChecker
        self.factory.configure_paths(
            filesystem_type=FILESYSTEM_TYPE_LOCAL,
            bucket=None,
            scenario_path=self.scenario_path,
            scenario=self.scenario,
            pipeline_path=None,
            simulations_path=None,
            output_path=None,
            target_accounts_path=None,
        )

        self.assertEqual(self.pipeline_path, self.factory.pipeline_path)
        self.assertEqual(self.simulations_path, self.factory.simulations_config_path)
        self.assertEqual(self.output_path, self.factory.output_config_path)
        self.assertEqual(self.target_accounts_path, self.factory.target_account_path)

    def test_should_override_scenario_paths_if_specified(self):
        # noinspection PyTypeChecker
        self.factory.configure_paths(
            filesystem_type=FILESYSTEM_TYPE_LOCAL,
            bucket=None,
            scenario_path=self.scenario_path,
            scenario=self.scenario,
            pipeline_path=None,
            simulations_path="/tmp/path/to/bob.yaml",
            output_path=None,
            target_accounts_path=None,
        )

        self.assertEqual(self.pipeline_path, self.factory.pipeline_path)
        self.assertEqual("/tmp/path/to/bob.yaml", self.factory.simulations_config_path)
        self.assertEqual(self.output_path, self.factory.output_config_path)
        self.assertEqual(self.target_accounts_path, self.factory.target_account_path)

    def test_build_config_raise_value_error_when_no_target_accounts_and_not_loading_from_snapshot(
            self,
    ):
        self.write_yaml({"shard": "ldprof"}, self.pipeline_path)
        self.write_yaml({"save": False}, self.output_path)
        self.write_yaml(
            {
                "strat_1": {
                    "instruments": [1],
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
                "strat_2": {
                    "instruments": [2],
                    "load_target_accounts_from_snapshot": True,
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
                "strat_3": {
                    "instruments": [3],
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
            },
            self.simulations_path,
        )
        self.write_auth(nomad=False)

        with self.assertRaises(ValueError) as ve:
            # noinspection PyTypeChecker
            self.factory.build(
                auth_path=self.auth_path,
                minio_uri=None,
                dataserver_uri=None,
                filesystem_type=FILESYSTEM_TYPE_LOCAL,
                bucket=None,
                scenario_path=self.scenario_path,
                scenario=self.scenario,
                pipeline_path=None,
                simulations_path=None,
                output_path=None,
                target_accounts_path=None,
                num_cores=1,
                num_batches=1,
                start_date="2020-10-01",
                end_date="2020-10-01",
                simulations_filter=[],
                colour=None,
                nomad=False,
            )

        exc = ve.exception
        self.assertEqual(
            "No accounts provided for simulations ['strat_1', 'strat_3']", exc.args[0]
        )

    def test_build_config_ignore_account_ids_if_loading_from_snapshot(self,):
        self.write_yaml({"shard": "ldprof"}, self.pipeline_path)
        self.write_yaml({"save": False}, self.output_path)
        self.write_lines(["account_id", "12663", "15297"], self.target_accounts_path)
        self.write_yaml(
            {
                "strat_1": {
                    "instruments": [1],
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
                "strat_2": {
                    "instruments": [2],
                    "load_target_accounts_from_snapshot": True,
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
                "strat_3": {
                    "instruments": [3],
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
            },
            self.simulations_path,
        )
        self.write_auth(nomad=False)

        # noinspection PyTypeChecker
        config: BackTestingConfig = self.factory.build(
            auth_path=self.auth_path,
            minio_uri=None,
            dataserver_uri=None,
            filesystem_type=FILESYSTEM_TYPE_LOCAL,
            bucket=None,
            scenario_path=self.scenario_path,
            scenario=self.scenario,
            pipeline_path=None,
            simulations_path=None,
            output_path=None,
            target_accounts_path=None,
            num_cores=1,
            num_batches=1,
            start_date="2020-10-01",
            end_date="2020-10-01",
            simulations_filter=[],
            colour=None,
            nomad=False,
        )

        pd.testing.assert_frame_equal(
            pd.DataFrame({"account_id": [12663, 15297]}),
            config.simulation_configs["strat_1"].target_accounts,
        )

        self.assertIsNone(config.simulation_configs["strat_2"].target_accounts)

        pd.testing.assert_frame_equal(
            pd.DataFrame({"account_id": [12663, 15297]}),
            config.simulation_configs["strat_3"].target_accounts,
        )

    def test_build_config_raises_value_error_when_simulation_has_no_instrument_and_not_loading_from_snapshot(
            self,
    ):
        self.write_yaml({"shard": "ldprof"}, self.pipeline_path)
        self.write_yaml({"save": False}, self.output_path)
        self.write_lines(["account_id", "12663", "15297"], self.target_accounts_path)
        self.write_yaml(
            {
                "strat_1": {
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
                "strat_2": {
                    "load_target_accounts_from_snapshot": True,
                    "load_instruments_from_snapshot": True,
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
            },
            self.simulations_path,
        )
        self.write_auth(nomad=False)

        with self.assertRaises(ValueError) as ev:
            # noinspection PyTypeChecker
            self.factory.build(
                auth_path=self.auth_path,
                minio_uri=None,
                dataserver_uri=None,
                filesystem_type=FILESYSTEM_TYPE_LOCAL,
                bucket=None,
                scenario_path=self.scenario_path,
                scenario=self.scenario,
                pipeline_path=None,
                simulations_path=None,
                output_path=None,
                target_accounts_path=None,
                num_cores=1,
                num_batches=1,
                start_date="2020-10-01",
                end_date="2020-10-01",
                simulations_filter=[],
                colour=None,
                nomad=False,
            )

        exc = ev.exception
        self.assertEqual(
            "No instruments provided for simulations ['strat_1']", exc.args[0]
        )

    def test_build_config_ignore_instrument_ids_when_loading_from_snapshot(self,):
        self.write_yaml({"shard": "ldprof"}, self.pipeline_path)
        self.write_yaml({"save": False}, self.output_path)
        self.write_lines(["account_id", "12663", "15297"], self.target_accounts_path)
        self.write_yaml(
            {
                "strat_1": {
                    "instruments": [1],
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
                "strat_2": {
                    "instruments": [2],
                    "load_target_accounts_from_snapshot": True,
                    "load_instruments_from_snapshot": True,
                    "strategy_parameters": {
                        "strategy_type": "internalisation",
                        "max_pos_qty": 100,
                        "max_pos_qty_buffer": 1.25,
                    },
                    "exit_parameters": {
                        "exit_type": "aggressive",
                        "takeprofit_limit": 25,
                        "stoploss_limit": 25,
                    },
                    "risk_parameters": {"risk_type": "no_risk"},
                },
            },
            self.simulations_path,
        )
        self.write_auth(nomad=False)

        # noinspection PyTypeChecker
        config: BackTestingConfig = self.factory.build(
            auth_path=self.auth_path,
            minio_uri=None,
            dataserver_uri=None,
            filesystem_type=FILESYSTEM_TYPE_LOCAL,
            bucket=None,
            scenario_path=self.scenario_path,
            scenario=self.scenario,
            pipeline_path=None,
            simulations_path=None,
            output_path=None,
            target_accounts_path=None,
            num_cores=1,
            num_batches=1,
            start_date="2020-10-01",
            end_date="2020-10-01",
            simulations_filter=[],
            colour=None,
            nomad=False,
        )

        self.assertEqual([1], config.simulation_configs["strat_1"].instruments)
        self.assertEqual(0, len(config.simulation_configs["strat_2"].instruments))


if __name__ == "__main__":
    unittest.main()
