from typing import Dict, Any

from backtesting.event_stream.event_stream import EventStream
from backtesting.event_stream.event_stream_sample import EventStreamSample
from backtesting.event_stream.event_stream_snapshot import EventStreamSnapshot
from backtesting.event_stream.event_stream_no_sample import EventStreamNoSample


def determine_event_stream_constructor(event_stream_reference):
    if event_stream_reference == "event_stream_snapshot":
        return EventStreamSnapshot
    elif event_stream_reference == "event_stream_sample":
        return EventStreamSample
    elif event_stream_reference == "event_stream_no_sample":
        return EventStreamNoSample
    else:
        raise KeyError(f"invalid event stream reference {event_stream_reference}")


def create_event_stream(event_stream_params: Dict[str, Any]) -> EventStream:
    constructor = determine_event_stream_constructor(
        event_stream_params.get("event_stream_type")
    )

    constructor_properties: Dict[str, Any] = {
        k: v
        for (k, v) in event_stream_params.items()
        if k in [x if x[0] != "_" else x[1:] for x in constructor.__slots__]
    }

    return constructor(**constructor_properties)
