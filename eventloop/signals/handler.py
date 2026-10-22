"""
Signal handling for async event loop.
"""

from __future__ import annotations

import signal
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Optional

from ..core.types import GodLoopError
from ..core.loop import EventLoop
from ..core.context import current_loop

__all__ = [
    "SignalHandler",
    "signal",
    "set_signal_handler",
    "get_signal_handler",
    "SignalError",
]


class SignalError(GodLoopError):
    """Signal handling errors."""
    pass


class SignalHandler:
    """
    Async signal handler.
    
    Provides a way to handle signals asynchronously in the event loop.
    """
    
    __slots__ = ("_loop", "_handlers", "_original_handlers")
    
    def __init__(self, loop: EventLoop | None = None) -> None:
        self._loop = loop or current_loop()
        self._handlers: dict[int, list[Callable[..., Any]]] = {}
        self._original_handlers: dict[int, Callable[..., Any] | None] = {}
    
    def add_handler(
        self,
        sig: int,
        callback: Callable[..., Any],
        *args: Any,
    ) -> None:
        """Add a signal handler."""
        if sig not in self._handlers:
            self._handlers[sig] = []
            # Save original handler
            try:
                self._original_handlers[sig] = signal.signal(sig, self._signal_handler)
            except (ValueError, OSError):
                # Signal not supported
                pass
        
        self._handlers[sig].append((callback, args))
    
    def remove_handler(self, sig: int, callback: Callable[..., Any] | None = None) -> bool:
        """Remove a signal handler."""
        if sig not in self._handlers:
            return False
        
        if callback is None:
            # Remove all handlers for this signal
            del self._handlers[sig]
            # Restore original handler
            if sig in self._original_handlers:
                try:
                    signal.signal(sig, self._original_handlers[sig])
                except (ValueError, OSError):
                    pass
                del self._original_handlers[sig]
            return True
        
        # Remove specific callback
        handlers = self._handlers[sig]
        for i, (cb, args) in enumerate(handlers):
            if cb is callback:
                handlers.pop(i)
                break
        else:
            return False
        
        if not handlers:
            return self.remove_handler(sig)
        
        return True
    
    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Internal signal handler - called from signal context."""
        handlers = self._handlers.get(signum, [])
        for callback, args in handlers:
            # Schedule in event loop
            self._loop.call_soon_threadsafe(callback, *args)
    
    def get_handlers(self, sig: int) -> list[Callable[..., Any]]:
        """Get all handlers for a signal."""
        return [cb for cb, _ in self._handlers.get(sig, [])]
    
    def clear(self) -> None:
        """Remove all signal handlers."""
        for sig in list(self._handlers.keys()):
            self.remove_handler(sig)


# Global signal handler management
_global_signal_handler: SignalHandler | None = None


def signal(
    sig: int,
    handler: Callable[..., Any] | int | None,
    *,
    loop: EventLoop | None = None,
) -> Callable[..., Any] | int | None:
    """
    Set a signal handler (asyncio-compatible).
    
    Args:
        sig: Signal number
        handler: Callable handler, signal.SIG_IGN, signal.SIG_DFL, or None
        loop: Event loop to use
    """
    global _global_signal_handler
    loop = loop or current_loop()
    
    if _global_signal_handler is None:
        _global_signal_handler = SignalHandler(loop)
    
    if handler is None or handler == signal.SIG_DFL:
        # Restore default
        old_handlers = _global_signal_handler.get_handlers(sig)
        _global_signal_handler.remove_handler(sig)
        if old_handlers:
            return old_handlers[0]
        return signal.SIG_DFL
    
    if handler == signal.SIG_IGN:
        # Ignore signal
        old_handlers = _global_signal_handler.get_handlers(sig)
        _global_signal_handler.remove_handler(sig)
        try:
            return signal.signal(sig, signal.SIG_IGN)
        except (ValueError, OSError):
            return signal.SIG_IGN
    
    if callable(handler):
        old_handlers = _global_signal_handler.get_handlers(sig)
        _global_signal_handler.add_handler(sig, handler)
        if old_handlers:
            return old_handlers[0]
        return signal.SIG_DFL
    
    raise TypeError("Handler must be callable, SIG_IGN, SIG_DFL, or None")


def set_signal_handler(
    sig: int,
    callback: Callable[..., Any],
    *args: Any,
    loop: EventLoop | None = None,
) -> None:
    """Set a signal handler with arguments."""
    loop = loop or current_loop()
    handler = SignalHandler(loop)
    handler.add_handler(sig, callback, *args)


def get_signal_handler(sig: int, loop: EventLoop | None = None) -> Callable[..., Any] | None:
    """Get the current signal handler."""
    global _global_signal_handler
    if _global_signal_handler is None:
        return None
    handlers = _global_signal_handler.get_handlers(sig)
    return handlers[0] if handlers else None


def valid_signals() -> set[int]:
    """Get set of valid signal numbers for this platform."""
    if sys.platform == "win32":
        return {signal.SIGINT, signal.SIGTERM, signal.SIGBREAK}
    else:
        # All signals except SIGKILL and SIGSTOP
        return set(range(1, signal.NSIG))