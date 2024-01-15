from datetime import datetime, date
from typing import Any, Union


def parse_bool(maybe_bool: Any) -> bool:
    if isinstance(maybe_bool, bool):
        return maybe_bool
    if isinstance(maybe_bool, str):
        return "true" == maybe_bool.lower() or "t" == maybe_bool.lower()
    if (
            maybe_bool
    ):  # this way, anything that is None fails, as do empty dicts/lists/sets
        return True
    return False


def parse_date(value: Union[str, date, datetime]) -> date:
    if isinstance(value, date):
        return value
    elif isinstance(value, datetime):
        return value.date()
    elif value:
        return datetime.strptime(value, "%Y-%m-%d").date()
    else:
        return None
