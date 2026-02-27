"""Gold extraction strategy for reading silver Delta tables."""

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy


class GoldExtract(Strategy):
    """Extract input data for the gold layer."""

    def __init__(self):
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-gold-extract")

    def execute(self, context):
        """Load silver Delta dataset from object storage.

        Args:
            context: Runtime context containing Spark session and source path.

        Returns:
            Spark DataFrame from silver layer.
        """
        spark = context["spark"]
        source_bucket = context["source_bucket"]
        source_path = context["source_path"]
        self.logger.info(
            "Gold extract started",
            source_bucket=source_bucket,
            source_path=source_path,
        )

        df = spark.read.format("delta").load(
            f"s3a://{source_bucket}/{source_path}"
        )
        self.logger.info("Gold extract finished")

        return df
