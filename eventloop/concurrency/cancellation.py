"""
Cancellation scope implementation.
"""

from .task_group import CancellationScope, CancellationError, shield

__all__ = [
    "CancellationScope",
    "CancellationError",
    "shield",
]