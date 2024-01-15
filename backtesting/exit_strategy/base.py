from abc import ABCMeta, abstractmethod


class AbstractExitStrategy(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @classmethod
    def create(cls, **kwargs):
        properties = {
            k: v
            for (k, v) in kwargs.items()
            if k in [x if x[0] != "_" else x[1:] for x in cls.__slots__]
        }
        return cls(**properties)

    @abstractmethod
    def generate_exit_order_signal(
            self, event, account, avg_price, tick_price, position
    ):
        raise NotImplementedError("Should implement start backtester")
