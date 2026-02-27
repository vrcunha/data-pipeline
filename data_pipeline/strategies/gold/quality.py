"""Gold data quality rules powered by PyDeequ."""

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import DataQualityStrategy


class GoldQuality(DataQualityStrategy):
    """Validate aggregated gold dataset integrity."""

    def __init__(self):
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-gold-quality")

    def validate(self, data, context: dict) -> None:
        """Run gold-level quality checks and fail on critical issues.

        Args:
            data: Spark DataFrame to validate.
            context: Runtime context containing Spark session.

        Raises:
            RuntimeError: If quality checks fail.
        """
        spark = context["spark"]
        self.logger.info("Gold quality validation started")

        check = (
            Check(spark, CheckLevel.Error, "Gold Data Quality Checks")
            .hasSize(lambda x: x > 0)
            .isComplete("state")
            .isComplete("city")
            .isComplete("brewery_type")
            .isNonNegative("brewery_count")
        )

        result = VerificationSuite(spark).onData(data).addCheck(check).run()
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        result_df.show(truncate=False)

        if str(result.status) != "Success":
            raise RuntimeError("Gold Data Quality Checks Failed.")
        self.logger.info("Gold quality validation passed")
