"""Gold transformation strategy for analytical aggregations."""

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy


class GoldTransform(Strategy):
    """Build aggregated metrics for the gold layer."""

    def __init__(self) -> None:
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-gold-transform")

    def execute(self, df: DataFrame, context: dict[str, Any]) -> DataFrame:
        """
        Build a long-format aggregated gold table with
        multiple aggregation levels.

        Args:
            df: Input Spark DataFrame from silver layer.
            context: Runtime context metadata.

        Returns:
            Aggregated Spark DataFrame.
        """
        self.logger.info("Gold transform started")

        city_level = (
            df.groupBy("country", "state", "city", "brewery_type")
            .agg(count("*").alias("brewery_count"))
            .withColumn("aggregation_level", lit("city_type"))
        )

        state_level = (
            df.groupBy("country", "state", "brewery_type")
            .agg(count("*").alias("brewery_count"))
            .withColumn("city", lit(None).cast("string"))
            .withColumn("aggregation_level", lit("state_type"))
        )

        country_level = (
            df.groupBy("country", "brewery_type")
            .agg(count("*").alias("brewery_count"))
            .withColumn("state", lit(None).cast("string"))
            .withColumn("city", lit(None).cast("string"))
            .withColumn("aggregation_level", lit("country_type"))
        )

        global_level = (
            df.groupBy("brewery_type")
            .agg(count("*").alias("brewery_count"))
            .withColumn("country", lit(None).cast("string"))
            .withColumn("state", lit(None).cast("string"))
            .withColumn("city", lit(None).cast("string"))
            .withColumn("aggregation_level", lit("global_type"))
        )

        combined = (
            city_level.unionByName(state_level)
            .unionByName(country_level)
            .unionByName(global_level)
        )

        gold_long = (
            combined.withColumn("metric_name", lit("brewery_count"))
            .withColumn("metric_value", col("brewery_count").cast("long"))
            .drop("brewery_count")
        )

        self.logger.info("Gold transform finished")
        return gold_long
