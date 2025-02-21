from typing import Dict, Any, List, AnyStr

from backtesting.config.type_parser import parse_bool


class BackTestingOutputConfig:
    """
    This class sets all of the configuration required to run a backtest from the input YAML file and optional parsed
    arguments to RunSimulation.
    """

    def __init__(
            self,
            datastore: AnyStr,
            datastore_parameters: Dict[AnyStr, Any],
            resample_rule: AnyStr,
            save: bool,
            save_per_simulation: bool,
            by: str,
            freq: str,
            mode: str,
            file: str,
            calculate_cumulative_daily_pnl: bool = False,
            store_index: bool = True,
            event_features: List[AnyStr] = ["symbol", "order_book_id", "account_id"],
            metrics: List[AnyStr] = ["performance_overview", "trading_actions_breakdown", "inventory_overview"],

    ):
        self.datastore: str = datastore
        self.datastore_parameters: Dict[AnyStr, Any] = datastore_parameters
        self.resample_rule: str = resample_rule
        self.event_features: List[str] = self._build_event_features(event_features, calculate_cumulative_daily_pnl)
        self.metrics: List[str] = metrics
        self.save: bool = save
        self.save_per_simulation: bool = save_per_simulation
        self.by: str = by
        self.freq: str = freq
        self.mode: str = mode
        self.file: str = file
        self.store_index: bool = store_index

    @classmethod
    def create(cls, config: Dict[str, Any], calculate_cumulative_daily_pnl: bool):
        config.update({
            'calculate_cumulative_daily_pnl': calculate_cumulative_daily_pnl
        })
        return cls(**config)

    @staticmethod
    def _build_event_features(
            features: List[str], calculate_cumulative_daily_pnl: bool
    ) -> List[str]:
        # as metric are calculate cumulatively by the backtester the event_features property is used to reverse this
        # to give a value per event. [1, 3, 5] --> [1, 2, 2]
        if calculate_cumulative_daily_pnl:
            return list(sorted(set(features)))
        return list(sorted(set(features + ["trading_session",])))
