"""
Pipe and Unix socket connection utilities.
"""

from .connection import open_pipe, open_unix_connection

__all__ = [
    "open_pipe",
    "open_unix_connection",
]