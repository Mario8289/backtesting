from copy import deepcopy
from typing import Dict, Any, Type

from risk_backtesting.exit_strategy import create_exit_strategy
from risk_backtesting.strategy.base import AbstractStrategy
from risk_backtesting.strategy.bbooking_profiler_strategy import (
    BbookingProfilerStrategy,
)
from risk_backtesting.strategy.bbooking_strategy import BbookingStrategy
from risk_backtesting.strategy.bch_strategy import BCHStrategy
from risk_backtesting.strategy.internalisation_strategy import InternalisationStrategy
from risk_backtesting.strategy.strategy_default import StrategyDefault


def determine_strategy_constructor(strategy_type: str) -> Type[AbstractStrategy]:
    if "bbooking" == strategy_type:
        return BbookingStrategy
    elif "internalisation" == strategy_type:
        return InternalisationStrategy
    elif "bch" == strategy_type:
        return BCHStrategy
    elif "strategy_default" == strategy_type:
        return StrategyDefault
    elif "bbooking_profiler":
        return BbookingProfilerStrategy
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

    if parameters.get("position_lifespan"):
        lifespan_exit_parameters: Dict[str, Any] = parameters[
            "position_lifespan_exit_parameters"
        ]
        parameters["position_lifespan_exit_strategy"] = create_exit_strategy(
            lifespan_exit_parameters
        )

    parameters["metadata"] = kwargs

    return constructor.create(**parameters)
