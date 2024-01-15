from abc import ABCMeta, abstractmethod, ABC


class AbstractModel(ABC):
    __metaclass__ = ABCMeta

    @classmethod
    def create(cls, **kwargs):
        properties = {
            k: v
            for (k, v) in kwargs.items()
            if k in [x if x[0] != "_" else x[1:] for x in cls.__slots__]
        }
        return cls(**properties)

    @abstractmethod
    def train(self):
        """
        handles the current state of the backtester
        """
        raise NotImplementedError("Should implement on_state()")

    @abstractmethod
    def predict(self):
        """
        handles the current state of the backtester
        """
        raise NotImplementedError("Should implement on_state()")
