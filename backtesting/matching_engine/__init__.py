from typing import Any, Dict

from risk_backtesting.matching_engine.base import AbstractMatchingEngine
from risk_backtesting.matching_engine.matching_engine_default import (
    MatchingEngineDefault,
)
from risk_backtesting.matching_engine.matching_engine_distribution import (
    MatchingEngineDistribution,
)
from risk_backtesting.matching_engine.matching_engine_tob import MatchingEngineTob


def get_matching_engine(matching_engine_reference):
    if matching_engine_reference == "matching_engine_default":
        return MatchingEngineDefault
    elif matching_engine_reference == "matching_engine_tob":
        return MatchingEngineTob
    elif matching_engine_reference == "matching_engine_distribution":
        return MatchingEngineDistribution
    else:
        raise KeyError(f"invalid matching engine reference {matching_engine_reference}")


def create_matching_engine(
        matching_engine_params: Dict[str, Any], matching_method: str
) -> AbstractMatchingEngine:
    constructor = get_matching_engine(
        matching_engine_params.get("matching_engine_type")
    )

    # initialise matching_engine with properties
    constructor_properties: Dict[str, Any] = {
        k: v
        for (k, v) in matching_engine_params.items()
        if k in [x if x[0] != "_" else x[1:] for x in constructor.__slots__]
    }

    if "matching_method" in constructor.__slots__:
        constructor_properties["matching_method"] = matching_method

    return constructor(**constructor_properties)
