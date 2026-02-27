"""Gold data quality rules powered by PyDeequ."""

from typing import Any

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import DataQualityStrategy


class GoldQuality(DataQualityStrategy):
    """Validate aggregated gold dataset integrity."""

    def __init__(self) -> None:
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-gold-quality")

    def validate(self, data: Any, context: dict[str, Any]) -> None:
        """
        Data quality validation for Gold Long semantic model.
        Supports multi-grain aggregation with conditional nullability.

        Expected schema (long format):
            state
            city
            brewery_type
            metric_name
            metric_value

        Args:
            data: Spark DataFrame to validate.
            context: Runtime context containing Spark session.

        Raises:
            RuntimeError: If quality checks fail.
        """

        spark = context["spark"]
        self.logger.info("Gold quality validation started")

        check = (
            Check(
                spark,
                CheckLevel.Error,
                "Gold LONG Structural & Business Checks",
            )
            .hasSize(lambda x: x > 0)
            .isComplete("metric_name")
            .isComplete("metric_value")
            .isComplete("aggregation_level")
            .isNonNegative("metric_value")
            .isContainedIn("metric_name", ["brewery_count"])
            .satisfies(
                "(aggregation_level != 'city_type') OR city IS NOT NULL",
                "city_required_for_city_level",
            )
            .satisfies(
                "(aggregation_level NOT IN ('city_type','state_type')) OR state IS NOT NULL",  # NOQA
                "state_required_when_expected",
            )
            .satisfies(
                "(aggregation_level = 'global_type') OR country IS NOT NULL",
                "country_required_except_global",
            )
            .hasUniqueness(
                [
                    "aggregation_level",
                    "country",
                    "state",
                    "city",
                    "brewery_type",
                    "metric_name",
                ],
                lambda x: x == 1.0,
            )
        )

        result = VerificationSuite(spark).onData(data).addCheck(check).run()
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
        result_df.show(truncate=False)

        if str(result.status) != "Success":
            raise RuntimeError("Gold Data Quality Checks Failed.")

        self.logger.info("Gold LONG quality validation passed")
