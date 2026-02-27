"""Gold load strategy for curated Delta output."""

from delta.tables import DeltaTable

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy


class GoldLoad(Strategy):
    """Persist gold DataFrame to destination storage with upsert."""

    def __init__(self):
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-gold-load")

    def execute(self, df, context):
        """Upsert gold data into a Delta table.

        Args:
            df: Spark DataFrame ready for gold output.
            context: Runtime context with destination settings.

        Returns:
            None
        """
        spark = context["spark"]
        destination_bucket = context["destination_bucket"]
        destination_path = context["destination_path"]
        upsert_keys = context["upsert_keys"]
        self.logger.info(
            "Gold load started",
            destination_bucket=destination_bucket,
            destination_path=destination_path,
            upsert_keys=upsert_keys,
        )

        missing_keys = [key for key in upsert_keys if key not in df.columns]
        if missing_keys:
            raise ValueError(
                f"Upsert keys not found in DataFrame columns: {missing_keys}"
            )

        target_path = f"s3a://{destination_bucket}/{destination_path}"
        merge_condition = " AND ".join(
            [f"target.`{key}` <=> source.`{key}`" for key in upsert_keys]
        )

        if DeltaTable.isDeltaTable(spark, target_path):
            target = DeltaTable.forPath(spark, target_path)
            (
                target.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            df.write.format("delta").mode("overwrite").save(target_path)
        self.logger.info("Gold load finished")
        return None
