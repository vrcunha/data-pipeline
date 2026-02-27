"""Gold transformation strategy for analytical aggregations."""

from pyspark.sql.functions import count

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy


class GoldTransform(Strategy):
    """Build aggregated metrics for the gold layer."""

    def __init__(self):
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-gold-transform")

    def execute(self, df, context):
        """Aggregate brewery counts by location and type.

        Args:
            df: Input Spark DataFrame from silver layer.
            context: Runtime context metadata.

        Returns:
            Aggregated Spark DataFrame.
        """
        self.logger.info("Gold transform started")
        df_agg = (
            df.groupBy("state", "city", "brewery_type")
            .agg(count("*").alias("brewery_count"))
            .orderBy("state", "city", "brewery_type")
        )
        self.logger.info("Gold transform finished")
        return df_agg
