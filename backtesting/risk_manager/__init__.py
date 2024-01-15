from typing import Any, Dict, Type

from risk_backtesting.risk_manager.base import AbstractRiskManager
from risk_backtesting.risk_manager.no_risk import NoRisk


def determine_risk_manager_constructor(
        risk_manager_type: str,
) -> Type[AbstractRiskManager]:
    if "no_risk" == risk_manager_type:
        return NoRisk
    else:
        raise ValueError(f"Invalid Risk Manager Type {risk_manager_type}")


def create_risk_manager(parameter: Dict[str, Any]) -> AbstractRiskManager:
    constructor: Type[AbstractRiskManager] = determine_risk_manager_constructor(
        parameter.get("risk_type")
    )
    return constructor()
