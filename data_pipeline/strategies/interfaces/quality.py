"""Base interface for data quality strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod


class DataQualityStrategy(ABC):
    """Contract for layer-specific quality checks."""

    @abstractmethod
    def validate(self, data, context: dict) -> None:
        """Validate data quality and raise on critical failures.

        Args:
            data: Dataset to validate.
            context: Runtime context with optional environment metadata.
        """
        pass
