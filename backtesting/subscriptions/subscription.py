from abc import ABCMeta, abstractmethod, ABC

from pandas import DataFrame
from typing import Dict, Any, AnyStr


def set_dtypes(df: DataFrame, schema: Dict[AnyStr, Any]):
    try:
        for col in df.columns:
            dtype = schema.get(col)
            if dtype:
                df[col] = df[col].astype(dtype)
    except ValueError as e:
        raise e
    except TypeError as e:
        raise e
    except Exception as e:
        raise e
    return df


class Subscription(ABC):
    __slots__ = [
        'load_by_session'
    ]

    def __init__(self, load_by_session=True):
        self.load_by_session = load_by_session

    __metaclass__ = ABCMeta

    @classmethod
    def create(cls, **kwargs):
        attributes = {
            k: v
            for (k, v) in kwargs.items()
            if k in [x if x[0] != "_" else x[1:] for x in cls.__slots__]
        }

        obj = cls(**attributes)
        obj.subscribe()
        return obj

    @abstractmethod
    def subscribe(self):
        pass
