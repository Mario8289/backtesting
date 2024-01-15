from ..risk_manager.base import AbstractRiskManager  # noqa


class NoRisk(AbstractRiskManager):
    __slots__ = "name"

    def __init__(self):
        super().__init__()
        self.name: str = "no_risk"

    def assess_order(self, order, portfolio, symbol, account):
        return order
