from copy import deepcopy
from typing import Dict, Any, Type

from risk_backtesting.model.base import AbstractModel
from risk_backtesting.model.profiling_ranks import ProfilingRankModel


def determine_model_constructor(model: str) -> Type[AbstractModel]:
    if "profiling_ranks" == model:
        return ProfilingRankModel
    else:
        raise ValueError(f"Invalid strategy type {model}")


def create_model(model_parameters: Dict[str, Any]) -> AbstractModel:
    constructor: Type[AbstractModel] = determine_model_constructor(
        model_parameters.get("model")
    )

    parameters: Dict[str, Any] = deepcopy(model_parameters)

    return constructor.create(**parameters)
