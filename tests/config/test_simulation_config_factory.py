import unittest

from risk_backtesting.config.simulation_config_factory import SimulationConfigFactory


# noinspection SpellCheckingInspection
class TestSimulationConfigFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory: SimulationConfigFactory = SimulationConfigFactory()

    def tearDown(self) -> None:
        pass

    def test_build_list_of_simulations_internalisation_dollar_exposure_single_instrument_zip(
            self,
    ):
        exit_parameters = {"exit_type": "exit_default"}
        risk_parameters = {"risk_type": "no_risk"}
        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": 1e6,
            "max_pos_qty_buffer": 1,
            "max_pos_qty_type": "dollar",
            "allow_partial_fills": True,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": [1234],
        }

        sims = self.factory._build_list_of_simulations(
            transform_type="zip",
            exit_parameters=list(exit_parameters.values()),
            risk_parameters=list(risk_parameters.values()),
            strategy_parameters=list(strategy_parameters.values()),
        )

        print(sims)
        self.assertEqual(1, len(sims))
        self.assertEqual(
            (
                "internalisation",
                1000000.0,
                1,
                "dollar",
                True,
                None,
                None,
                12345678,
                1234,
                "exit_default",
                "no_risk",
            ),
            sims[0],
        )

    def test_build_list_of_simulations_internalisation_dollar_exposure_single_instruments_product(
            self,
    ):
        exit_parameters = {"exit_type": "exit_default"}
        risk_parameters = {"risk_type": "no_risk"}
        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": 1e6,
            "max_pos_qty_buffer": 1,
            "max_pos_qty_type": "dollar",
            "allow_partial_fills": True,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": [1234],
        }

        sims = self.factory._build_list_of_simulations(
            transform_type="product",
            exit_parameters=list(exit_parameters.values()),
            risk_parameters=list(risk_parameters.values()),
            strategy_parameters=list(strategy_parameters.values()),
        )

        self.assertEqual(1, len(sims))
        self.assertEqual(
            (
                "internalisation",
                1000000.0,
                1,
                "dollar",
                True,
                None,
                None,
                12345678,
                1234,
                "exit_default",
                "no_risk",
            ),
            sims[0],
        )

    def test_build_list_of_simulations_internalisation_dollar_exposure_multiple_instruments_zip(
            self,
    ):
        exit_parameters = {"exit_type": "exit_default"}
        risk_parameters = {"risk_type": "no_risk"}
        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": 1e6,
            "max_pos_qty_buffer": 1,
            "max_pos_qty_type": "dollar",
            "allow_partial_fills": True,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": [1234, 5678],
        }

        sims = self.factory._build_list_of_simulations(
            transform_type="zip",
            exit_parameters=list(exit_parameters.values()),
            risk_parameters=list(risk_parameters.values()),
            strategy_parameters=list(strategy_parameters.values()),
        )

        self.assertEqual(2, len(sims))
        for i, instrument in enumerate([1234, 5678]):
            self.assertEqual(
                (
                    "internalisation",
                    1000000.0,
                    1,
                    "dollar",
                    True,
                    None,
                    None,
                    12345678,
                    instrument,
                    "exit_default",
                    "no_risk",
                ),
                sims[i],
            )

    def test_build_list_of_simulations_internalisation_dollar_exposure_multiple_instruments_product(
            self,
    ):
        exit_parameters = {"exit_type": "exit_default"}
        risk_parameters = {"risk_type": "no_risk"}
        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": 1e6,
            "max_pos_qty_buffer": 1,
            "max_pos_qty_type": "dollar",
            "allow_partial_fills": True,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": [1234, 5678],
        }

        sims = self.factory._build_list_of_simulations(
            transform_type="product",
            exit_parameters=list(exit_parameters.values()),
            risk_parameters=list(risk_parameters.values()),
            strategy_parameters=list(strategy_parameters.values()),
        )

        self.assertEqual(2, len(sims))
        for i, instrument in enumerate([1234, 5678]):
            self.assertEqual(
                (
                    "internalisation",
                    1000000.0,
                    1,
                    "dollar",
                    True,
                    None,
                    None,
                    12345678,
                    instrument,
                    "exit_default",
                    "no_risk",
                ),
                sims[i],
            )

    def test_build_list_of_simulations_internalisation_dollar_exposure_multiple_instruments_exposures_zip(
            self,
    ):
        exit_parameters = {"exit_type": "exit_default"}
        risk_parameters = {"risk_type": "no_risk"}
        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": [1e6, 5e6],
            "max_pos_qty_buffer": 1,
            "max_pos_qty_type": "dollar",
            "allow_partial_fills": True,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": [1234, 5678],
        }

        sims = self.factory._build_list_of_simulations(
            transform_type="zip",
            exit_parameters=list(exit_parameters.values()),
            risk_parameters=list(risk_parameters.values()),
            strategy_parameters=list(strategy_parameters.values()),
        )

        self.assertEqual(2, len(sims))

        for i, (instrument, position_limit) in enumerate(zip([1234, 5678], [1e6, 5e6])):
            self.assertEqual(
                (
                    "internalisation",
                    position_limit,
                    1,
                    "dollar",
                    True,
                    None,
                    None,
                    12345678,
                    instrument,
                    "exit_default",
                    "no_risk",
                ),
                sims[i],
            )

    def test_build_list_of_simulations_internalisation_dollar_exposure_multiple_instruments_exposures_product(
            self,
    ):
        exit_parameters = {"exit_type": "exit_default"}
        risk_parameters = {"risk_type": "no_risk"}
        strategy_parameters = {
            "strategy_type": "internalisation",
            "max_pos_qty": [1e6, 5e6],
            "max_pos_qty_buffer": 1,
            "max_pos_qty_type": "dollar",
            "allow_partial_fills": True,
            "max_pos_qty_rebalance_rate": None,
            "position_lifespan": None,
            "account_id": 12345678,
            "instruments": [1234, 5678],
        }

        sims = self.factory._build_list_of_simulations(
            transform_type="product",
            exit_parameters=list(exit_parameters.values()),
            risk_parameters=list(risk_parameters.values()),
            strategy_parameters=list(strategy_parameters.values()),
        )

        self.assertEqual(4, len(sims))
        print(sims)
        for i, (instrument, position_limit) in enumerate(
                zip([1234, 5678] * 2, [1e6, 1e6, 5e6, 5e6])
        ):
            self.assertEqual(
                (
                    "internalisation",
                    position_limit,
                    1,
                    "dollar",
                    True,
                    None,
                    None,
                    12345678,
                    instrument,
                    "exit_default",
                    "no_risk",
                ),
                sims[i],
            )


if __name__ == "__main__":
    unittest.main()
