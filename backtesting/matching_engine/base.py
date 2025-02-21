from abc import ABCMeta, abstractmethod
from datetime import date


class AbstractMatchingEngine(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    def load_model(
            self, loader, datasource_label: str, date: date
    ):
        pass

    @abstractmethod
    def match_order(self, event, order):
        raise NotImplementedError("Should implement match_order()")
