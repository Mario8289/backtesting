from abc import ABCMeta, abstractmethod


class AbstractStatistics(object):
    """
    Statistics is an abstract class providing an interface for
    all inherited statistic classes (live, historic, custom, etc).
    The goal of a Statistics object is to keep a record of useful
    information about one or many trading strategies as the strategy
    is running. This is done by hooking into the main event loop and
    essentially updating the object according to portfolio performance
    over time.
    Ideally, Statistics should be subclassed according to the strategies
    and timeframes-traded by the user. Different trading strategies
    may require different metrics or frequencies-of-metrics to be updated,
    however the example given is suitable for longer timeframes.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def update_event_snapshot(self):
        """
        Update all the statistics according to values of the portfolio
        and open positions. This should be called from within the
        event loop.
        """
        raise NotImplementedError("Should implement update()")
