import unittest

from risk_backtesting.config.backtesting_output_config import BackTestingOutputConfig


class TestOutputConfig(unittest.TestCase):
    def test_output_config_event_features_if_not_cumulative_daily_pnl(self,):
        output = {
            "resample_rule": "summary",
            "event_features": ["symbol", "order_book_id"],
            "save": True,
            "by": None,
            "freq": "D",
            "mode": "a",
            "filesystem": "local",
            "bucket": None,
            "directory": "/home/jovyan/work/outputs",
            "file_type": "csv",
            "store_index": False,
            "file": None,
        }

        config = BackTestingOutputConfig(output, calculate_cumulative_daily_pnl=False)

        self.assertEqual(
            config.event_features, ["order_book_id", "symbol", "trading_session"],
        )

    def test_output_config_event_features_if_cumulative_daily_pnl(self):
        output = {
            "resample_rule": None,
            "event_features": ["symbol", "order_book_id"],
            "save": True,
            "by": None,
            "freq": "D",
            "mode": "a",
            "filesystem": "local",
            "bucket": None,
            "directory": "/home/jovyan/work/outputs",
            "file_type": "csv",
            "store_index": False,
            "file": None,
        }

        config = BackTestingOutputConfig(output, calculate_cumulative_daily_pnl=True)

        self.assertEqual(
            config.event_features, ["order_book_id", "symbol"],
        )


if __name__ == "__main__":
    unittest.main()
