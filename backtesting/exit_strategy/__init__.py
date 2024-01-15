from typing import Type, Any, Dict

from risk_backtesting.exit_strategy.aggressive import Aggressive
from risk_backtesting.exit_strategy.base import AbstractExitStrategy
from risk_backtesting.exit_strategy.chaser import Chaser
from risk_backtesting.exit_strategy.exit_default import ExitDefault
from risk_backtesting.exit_strategy.profit_running import ProfitRunning
from risk_backtesting.exit_strategy.trailing_stoploss import TrailingStopLoss


def determine_exit_strategy_constructor(exit_type: str) -> Type[AbstractExitStrategy]:
    if "aggressive" == exit_type:
        return Aggressive
    elif "trailing_stoploss" == exit_type:
        return TrailingStopLoss
    elif "chaser" == exit_type:
        return Chaser
    elif "profit_running" == exit_type:
        return ProfitRunning
    elif "exit_default" == exit_type:
        return ExitDefault
    else:
        raise ValueError(f"Invalid exit strategy reference {exit_type}")


def create_exit_strategy(exit_parameters: Dict[str, Any]) -> AbstractExitStrategy:
    constructor: Type[AbstractExitStrategy] = determine_exit_strategy_constructor(
        exit_parameters.get("exit_type")
    )
    return constructor.create(**exit_parameters)
