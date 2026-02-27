"""Core ETL orchestration logic."""

from typing import Any

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import DataQualityStrategy, Strategy


class ETL:
    """Run extract, transform, quality, and load strategies in sequence."""

    def __init__(
        self,
        extract: Strategy,
        transform: Strategy,
        load: Strategy,
        job_name: str,
        quality_strategy: DataQualityStrategy,
    ):
        """Initialize ETL with concrete strategy implementations.

        Args:
            extract: Strategy responsible for data extraction.
            transform: Strategy responsible for data transformation.
            load: Strategy responsible for data persistence.
            job_name: Logical name used for structured logging.
            quality_strategy: Strategy responsible for validating data quality.
        """
        self.extract = extract
        self.transform = transform
        self.load = load
        self.quality = quality_strategy
        self.logger = StructuredLogger(f"etl-{job_name}")
        self.job_name = job_name

    def run(self, context: dict[str, Any]) -> None:
        """Execute the ETL pipeline.

        Args:
            context: Runtime context with connection details and execution
                parameters shared across strategies.
        """

        self.logger.info("ETL started", job=self.job_name, stage="start")

        raw = self.extract.execute(context)
        self.logger.info("Stage completed", job=self.job_name, stage="extract")

        transformed = self.transform.execute(raw, context)
        self.logger.info(
            "Stage completed", job=self.job_name, stage="transform"
        )

        self.quality.validate(transformed, context)
        self.logger.info("Stage completed", job=self.job_name, stage="quality")

        self.load.execute(transformed, context)
        self.logger.info("Stage completed", job=self.job_name, stage="load")

        self.logger.info("ETL finished", job=self.job_name, stage="finish")
