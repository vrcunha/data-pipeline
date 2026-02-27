"""Silver extraction strategy for reading bronze data from object storage."""

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy


class SilverExtract(Strategy):
    """Extract silver input data from bronze JSON files."""

    def __init__(self):
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-silver-extract")

    def execute(self, context):
        """Read bronze data into a Spark DataFrame with schema enforcement.

        Args:
            context: Runtime context containing Spark session and source path.

        Returns:
            Spark DataFrame with bronze records.
        """
        spark = context["spark"]
        source_bucket = context["source_bucket"]
        source_path = context["source_path"]
        self.logger.info(
            "Silver extract started",
            source_bucket=source_bucket,
            source_path=source_path,
        )

        _schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("brewery_type", StringType(), True),
                StructField("address_1", StringType(), True),
                StructField("address_2", StringType(), True),
                StructField("address_3", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state_province", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("country", StringType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("phone", StringType(), True),
                StructField("website_url", StringType(), True),
                StructField("state", StringType(), True),
                StructField("street", StringType(), True),
            ]
        )
        df = (
            spark.read.schema(_schema)
            .option("multiLine", "true")
            .format("json")
            .load(f"s3a://{source_bucket}/{source_path}")
        )
        self.logger.info("Silver extract finished")

        return df
