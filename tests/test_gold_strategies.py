"""Unit tests for gold strategies."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.strategies import (
    GoldExtract,
    GoldLoad,
    GoldQuality,
    GoldTransform,
)


def test_gold_extract_reads_delta_from_s3a() -> None:
    """Ensure gold extract reads Delta dataset from expected path."""
    strategy = GoldExtract()
    df_mock = MagicMock()
    spark_mock = MagicMock()
    reader = MagicMock()
    spark_mock.read = reader
    reader.format.return_value = reader
    reader.load.return_value = df_mock
    context = {
        "spark": spark_mock,
        "source_bucket": "silver",
        "source_path": "x/",
    }

    result = strategy.execute(context)

    reader.load.assert_called_once_with("s3a://silver/x/")
    assert result is df_mock


def test_gold_transform_groups_and_orders() -> None:
    """Ensure gold transform builds the expected chained operations."""
    strategy = GoldTransform()
    df_mock = MagicMock()
    grouped = MagicMock()
    aggregated = MagicMock()
    with_column = MagicMock()
    combined = MagicMock()
    final_df = MagicMock()

    df_mock.groupBy.return_value = grouped
    grouped.agg.return_value = aggregated
    aggregated.withColumn.return_value = with_column
    with_column.unionByName.return_value = combined
    combined.unionByName.return_value = combined
    combined.withColumn.return_value = combined
    combined.drop.return_value = final_df

    result = strategy.execute(df_mock, {})

    assert result is final_df
    assert df_mock.groupBy.call_count >= 1


def test_gold_load_writes_delta_on_first_run() -> None:
    """Ensure gold load writes Delta table when target does not exist."""
    strategy = GoldLoad()
    spark_mock = MagicMock()
    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    df_mock = MagicMock()
    df_mock.columns = ["state", "city", "brewery_type", "brewery_count"]
    df_mock.withColumn.return_value = df_mock
    df_mock.write = writer
    context = {
        "spark": spark_mock,
        "destination_bucket": "gold",
        "destination_path": "openbrewerydb/",
        "upsert_keys": ["state", "city", "brewery_type"],
    }

    with patch(
        "data_pipeline.strategies.gold.load.DeltaTable"
    ) as delta_table_mock:
        delta_table_mock.isDeltaTable.return_value = False
        strategy.execute(df_mock, context)

    writer.save.assert_called_once_with("s3a://gold/openbrewerydb/")


def test_gold_load_merges_when_target_exists() -> None:
    """Ensure gold load performs merge upsert when Delta table exists."""
    strategy = GoldLoad()
    spark_mock = MagicMock()
    df_mock = MagicMock()
    df_mock.columns = ["state", "city", "brewery_type", "brewery_count"]
    df_mock.withColumn.return_value = df_mock
    source_alias = MagicMock()
    df_mock.alias.return_value = source_alias

    merge_builder = MagicMock()
    merge_builder.whenMatchedUpdateAll.return_value = merge_builder
    merge_builder.whenNotMatchedInsertAll.return_value = merge_builder

    target_alias = MagicMock()
    target_alias.merge.return_value = merge_builder

    target_table = MagicMock()
    target_table.alias.return_value = target_alias

    context = {
        "spark": spark_mock,
        "destination_bucket": "gold",
        "destination_path": "openbrewerydb/",
        "upsert_keys": ["state", "city", "brewery_type"],
    }

    with patch(
        "data_pipeline.strategies.gold.load.DeltaTable"
    ) as delta_table_mock:
        delta_table_mock.isDeltaTable.return_value = True
        delta_table_mock.forPath.return_value = target_table
        strategy.execute(df_mock, context)

    target_alias.merge.assert_called_once()
    merge_builder.execute.assert_called_once()


def test_gold_quality_raises_on_failed_checks() -> None:
    """Ensure gold quality raises when checks report failure."""
    strategy = GoldQuality()

    class _FailedSuite:
        def __init__(self, spark: Any) -> None:
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
        "data_pipeline.strategies.gold.quality.VerificationSuite",
        _FailedSuite,
    ):
        with pytest.raises(RuntimeError):
            strategy.validate(data=MagicMock(), context={"spark": MagicMock()})
