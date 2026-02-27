"""Silver data quality rules powered by PyDeequ."""

from typing import Any

from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationResult, VerificationSuite

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import DataQualityStrategy


class SilverQuality(DataQualityStrategy):
    """Validate silver dataset integrity and types."""

    def __init__(self) -> None:
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-silver-quality")

    def validate(self, data: Any, context: dict[str, Any]) -> None:
        """Run silver-level quality checks and fail on critical issues.

        Args:
            data: Spark DataFrame to validate.
            context: Runtime context containing Spark session.

        Raises:
            RuntimeError: If quality checks fail.
        """
        spark = context["spark"]
        self.logger.info("Silver quality validation started")

        check = (
            Check(spark, CheckLevel.Error, "Silver Data Quality Checks")
            .hasSize(lambda x: x > 0)
            .isComplete("id")
            .isComplete("brewery_type")
            .isComplete("city")
            .isComplete("state_province")
            .isComplete("country")
            .hasUniqueness(["id"], lambda x: x >= 1.0)
            .hasDataType(
                "latitude",
                ConstrainableDataTypes.Fractional,
                lambda x: x == 1.0,
            )
            .hasDataType(
                "longitude",
                ConstrainableDataTypes.Fractional,
                lambda x: x == 1.0,
            )
        )

        result = VerificationSuite(spark).onData(data).addCheck(check).run()
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        result_df.show(truncate=False)

        if str(result.status) != "Success":
            raise RuntimeError("Silver Data Quality Checks Failed.")
        self.logger.info("Silver quality validation passed")
