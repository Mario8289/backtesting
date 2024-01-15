from typing import Dict, Any, List

from risk_backtesting.config.type_parser import parse_bool


class BackTestingOutputConfig:
    """
    this class sets all of the configuration required to run a backtest from the input yaml file and optional parsed
    arguments to RunSimulation.
    """

    def __init__(self, config: Dict[str, Any], calculate_cumulative_daily_pnl: bool):
        self.resample_rule: str = config.get("resample_rule")
        self.event_features: List[str] = self._build_event_features(
            config.get("event_features", ["symbol", "order_book_id", "account_id"]),
            calculate_cumulative_daily_pnl,
        )
        self.metrics: List[str] = config.get(
            "metrics",
            [
                "performance_overview",
                "trading_actions_breakdown",
                "inventory_overview",
            ],
        )
        self.save: bool = parse_bool(config.get("save"))
        self.save_per_simulation: bool = parse_bool(config.get("save_per_simulation"))
        self.by: str = config.get("by")
        self.freq: str = config.get("freq")
        self.mode: str = config.get("mode")
        self.bucket: str = config.get("bucket")
        self.directory: str = config.get("directory")
        self.file: str = config.get("file")
        self.store_index: bool = config.get("store_index")
        self.filesystem_type: str = config.get("filesystem")

    @staticmethod
    def _build_event_features(
            features: List[str], calculate_cumulative_daily_pnl: bool
    ) -> List[str]:
        # as metric are calculate cumulatively by the backtester the event_features property is used to reverse this
        # to give a value per event. [1, 3, 5] --> [1, 2, 2]
        if calculate_cumulative_daily_pnl:
            return list(sorted(set(features)))
        return list(sorted(set(features + ["trading_session",])))
