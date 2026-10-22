"""
Signals module exports.
"""

from .handler import (
    SignalHandler,
    signal,
    set_signal_handler,
    get_signal_handler,
    SignalError,
    valid_signals,
)

__all__ = [
    "SignalHandler",
    "signal",
    "set_signal_handler",
    "get_signal_handler",
    "SignalError",
    "valid_signals",
]