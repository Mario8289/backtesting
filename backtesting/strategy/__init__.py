from copy import deepcopy
from typing import Dict, Any, Type

from backtesting.exit_strategy import create_exit_strategy
from backtesting.strategy.base import AbstractStrategy

from backtesting.strategy.strategy_default import StrategyDefault
from backtesting.strategy.oscillator import Oscillator
from backtesting.strategy.dca import DCA


def determine_strategy_constructor(strategy_type: str) -> Type[AbstractStrategy]:
    if "strategy_default" == strategy_type:
        return StrategyDefault
    elif "oscillator" == strategy_type:
        return Oscillator
    elif "dca" == strategy_type:
        return DCA
    else:
        raise ValueError(f"Invalid strategy type {strategy_type}")


def create_strategy(
        strategy_parameters: Dict[str, Any],
        exit_strategy_parameters: Dict[str, Any],
        **kwargs,
) -> AbstractStrategy:
    constructor: Type[AbstractStrategy] = determine_strategy_constructor(
        strategy_parameters.get("strategy_type")
    )
    parameters: Dict[str, Any] = deepcopy(strategy_parameters)
    parameters["exit_strategy"] = create_exit_strategy(exit_strategy_parameters)
    parameters["metadata"] = kwargs

    return constructor.create(**parameters)
