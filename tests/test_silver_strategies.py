"""Unit tests for silver strategies."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.strategies import (
    SilverExtract,
    SilverLoad,
    SilverQuality,
    SilverTransform,
)


def test_silver_extract_reads_json_from_s3a() -> None:
    """Ensure silver extract reads from expected S3 path."""
    strategy = SilverExtract()
    df_mock = MagicMock()
    spark_mock = MagicMock()
    reader = MagicMock()
    spark_mock.read = reader
    reader.schema.return_value = reader
    reader.option.return_value = reader
    reader.format.return_value = reader
    reader.load.return_value = df_mock
    context = {
        "spark": spark_mock,
        "source_bucket": "bronze",
        "source_path": "x/",
    }

    result = strategy.execute(context)

    reader.load.assert_called_once_with("s3a://bronze/x/")
    assert result is df_mock


def test_silver_transform_clean_postal_code_calls_with_column() -> None:
    """Ensure postal code cleanup applies transformation on target column."""
    strategy = SilverTransform()
    df_mock = MagicMock()

    strategy.clean_postal_code(df_mock, "postal_code")

    df_mock.withColumn.assert_called_once()
    args, _ = df_mock.withColumn.call_args
    assert args[0] == "postal_code"


def test_silver_transform_execute_runs_successfully() -> None:
    """Ensure full silver transform pipeline executes without exceptions."""
    strategy = SilverTransform()
    df_mock = MagicMock()
    df_mock.columns = ["id", "postal_code", "longitude", "latitude"]
    df_mock.withColumnRenamed.return_value = df_mock
    df_mock.withColumn.return_value = df_mock
    df_mock.dropDuplicates.return_value = df_mock
    df_mock.show.return_value = None
    context = {"execution_date": "2026-02-26"}

    result = strategy.execute(df_mock, context)

    assert result is df_mock
    df_mock.withColumn.assert_called()


def test_silver_load_writes_delta_partitioned() -> None:
    """Ensure silver load writes partitioned Delta output."""
    strategy = SilverLoad()
    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer
    df_mock = MagicMock()
    df_mock.write = writer
    context = {
        "destination_bucket": "silver",
        "destination_path": "openbrewerydb/",
        "execution_date": "2026-02-26",
    }

    strategy.execute(df_mock, context)

    writer.save.assert_called_once_with("s3a://silver/openbrewerydb/")


def test_silver_quality_raises_on_failed_checks() -> None:
    """Ensure silver quality raises when checks report failure."""
    strategy = SilverQuality()

    class _FailedSuite:
        def __init__(self, spark) -> None:
            self.spark = spark

        def onData(self, data: Any) -> "_FailedSuite":
            return self

        def addCheck(self, check: Any) -> "_FailedSuite":
            return self

        def run(self) -> Any:
            class _Result:
                status = "Error"

            return _Result()

    with patch(
        "data_pipeline.strategies.silver.quality.VerificationSuite",
        _FailedSuite,
    ):
        with pytest.raises(RuntimeError):
            strategy.validate(data=MagicMock(), context={"spark": MagicMock()})
