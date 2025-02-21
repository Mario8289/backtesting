from typing import Type, Any, Dict

from backtesting.exit_strategy.aggressive import Aggressive
from backtesting.exit_strategy.base import AbstractExitStrategy
from backtesting.exit_strategy.chaser import Chaser
from backtesting.exit_strategy.no_exit import NoExit
from backtesting.exit_strategy.profit_running import ProfitRunning
from backtesting.exit_strategy.trailing_stoploss import TrailingStopLoss


def determine_exit_strategy_constructor(exit_type: str) -> Type[AbstractExitStrategy]:
    if "aggressive" == exit_type:
        return Aggressive
    elif "trailing_stoploss" == exit_type:
        return TrailingStopLoss
    elif "chaser" == exit_type:
        return Chaser
    elif "profit_running" == exit_type:
        return ProfitRunning
    elif "no_exit" == exit_type:
        return NoExit
    else:
        raise ValueError(f"Invalid exit strategy reference {exit_type}")


def create_exit_strategy(exit_parameters: Dict[str, Any]) -> AbstractExitStrategy:
    constructor: Type[AbstractExitStrategy] = determine_exit_strategy_constructor(
        exit_parameters.get("exit_type")
    )
    return constructor.create(**exit_parameters)
