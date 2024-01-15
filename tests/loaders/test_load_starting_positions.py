import datetime as dt
import unittest
from typing import List

import pandas as pd

from risk_backtesting.loaders.load_starting_positions import StartingPositionsLoader
from risk_backtesting.loaders.load_starting_positions import (
    load_open_positions_with_risk,
)


class StartingPositionsDummyLoader(StartingPositionsLoader):
    def __init__(self, starting_positions: pd.DataFrame):
        self.starting_positions = starting_positions

    def get_opening_positions(
            self,
            datasource_label: str,
            start_date: dt.date,
            end_date: dt.date,
            instruments: List,
            accounts: List,
            schema=None,
    ):
        return self.starting_positions


class TestGetOpeningPositions(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self) -> None:
        pass

    def test_get_opening_positions(self):
        pass

    def test_load_starting_position_when_loading_booking_risk_from_snapshot(self):
        date = dt.date(2021, 4, 21)

        starting_positions = pd.DataFrame(
            {
                "shard": ["ldprof"] * 2,
                "symbol": ["A/USD"] * 2,
                "datasource": [4] * 2,
                "next_trading_day": [date] * 2,
                "account_id": [1, 2],
                "position": [-1] * 2,
                "instrument_id": [12345] * 2,
                "unit_price": [10000] * 2,
                "price_increment": [1e-05] * 2,
                "currency": ["USD"] * 2,
                "open_cost": [1.10003 * 1 * 10000] * 2,
            }
        )

        snapshot = pd.DataFrame(
            {
                "timestamp": [dt.datetime.combine(date, dt.time())] * 2,
                "account_id": [1, 2],
                "datasource": [4] * 2,
                "booking_risk": [100, 50],
                "internalisation_risk": [0] * 2,
                "instrument_id": [12345] * 2,
            }
        )
        positions, total_net_position = load_open_positions_with_risk(
            loader=StartingPositionsDummyLoader(starting_positions=starting_positions),
            datasource_label="ldprof",
            start_date=date,
            end_date=date,
            account=[1, 2],
            invert_position=True,
            instrument=[12345],
            netting_engine="fifo",
            load_booking_risk=True,
            load_internalisation_risk=False,
            snapshot=snapshot,
        )

        self.assertEqual(150, total_net_position)
        self.assertEqual(100, positions[1, 12345, 1].open_positions[0].quantity)
        self.assertEqual(50, positions[1, 12345, 2].open_positions[0].quantity)

    def test_load_starting_position_when_loading_internalisation_risk_from_snapshot(
            self,
    ):
        date = dt.date(2021, 4, 21)

        starting_positions = pd.DataFrame(
            {
                "shard": ["ldprof"] * 2,
                "symbol": ["A/USD"] * 2,
                "datasource": [4] * 2,
                "next_trading_day": [date] * 2,
                "account_id": [1, 2],
                "position": [-1] * 2,
                "instrument_id": [12345] * 2,
                "unit_price": [10000] * 2,
                "price_increment": [1e-05] * 2,
                "currency": ["USD"] * 2,
                "open_cost": [1.10003 * 1 * 10000] * 2,
            }
        )

        snapshot = pd.DataFrame(
            {
                "timestamp": [dt.datetime.combine(date, dt.time())] * 2,
                "account_id": [1, 2],
                "datasource": [4] * 2,
                "internalisation_risk": [100, 50],
                "booking_risk": [0] * 2,
                "instrument_id": [12345] * 2,
            }
        )
        positions, total_net_position = load_open_positions_with_risk(
            loader=StartingPositionsDummyLoader(starting_positions=starting_positions),
            datasource_label="ldprof",
            start_date=date,
            end_date=date,
            account=[1, 2],
            invert_position=True,
            instrument=[12345],
            netting_engine="fifo",
            load_booking_risk=False,
            load_internalisation_risk=True,
            snapshot=snapshot,
        )

        self.assertEqual(150, total_net_position)
        self.assertEqual(100, positions[1, 12345, 1].open_positions[0].quantity)
        self.assertEqual(50, positions[1, 12345, 2].open_positions[0].quantity)


if __name__ == "__main__":
    unittest.main()
