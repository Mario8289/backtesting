from abc import ABCMeta, abstractmethod


class AbstractRiskManager(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def assess_order(self, order, portfolio, symbol, account):
        raise NotImplementedError("Should implement assess_order()")
