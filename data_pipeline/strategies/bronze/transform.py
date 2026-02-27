"""Bronze transform strategy."""

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy


class BronzeTransform(Strategy):
    """Apply bronze-level transformations.

    The current implementation is pass-through by design.
    """

    def __init__(self):
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-bronze-transform")

    def execute(self, data, context):
        """Return input data without changes.

        Args:
            data: Extracted records.
            context: Runtime context.

        Returns:
            Unmodified input data.
        """
        self.logger.info("Bronze transform pass-through", records=len(data))
        return data
