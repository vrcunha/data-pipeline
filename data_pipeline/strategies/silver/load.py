"""Silver load strategy for Delta table persistence."""

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy


class SilverLoad(Strategy):
    """Persist silver DataFrame to Delta format."""

    def __init__(self):
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-silver-load")

    def execute(self, df, context):
        """Write silver data to object storage in Delta format.

        Args:
            df: Spark DataFrame ready for silver output.
            context: Runtime context with destination bucket and path.

        Returns:
            None
        """
        destination_bucket = context["destination_bucket"]
        destination_path = context["destination_path"]
        execution_date = context["execution_date"]
        self.logger.info(
            "Silver load started",
            destination_bucket=destination_bucket,
            destination_path=destination_path,
            execution_date=execution_date,
        )

        (
            df.write.format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere", f"_execution_date = DATE '{execution_date}'"
            )
            .partitionBy("_execution_date", "country", "state_province")
            .save(f"s3a://{destination_bucket}/{destination_path}")
        )
        self.logger.info("Silver load finished")
        return None
