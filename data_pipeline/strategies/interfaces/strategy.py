"""Base interface for strategy implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Strategy(ABC):
    """
    The Strategy interface declares operations common to all algorithms.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute strategy behavior.

        Args:
            context: Runtime context or payload used by the strategy.
        """
        pass
