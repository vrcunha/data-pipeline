"""Spark ETL entrypoint for silver and gold layers."""

import argparse
import json

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from data_pipeline.etl import ETL
from data_pipeline.strategies.registry import StrategyRegistry

load_dotenv()


def load_context(context: dict, spark: SparkSession) -> dict:
    """Attach Spark session to the runtime context.

    Args:
        context: Runtime context created from CLI inputs.
        spark: Active Spark session.

    Returns:
        Updated context dictionary containing the Spark session.
    """
    context["spark"] = spark
    return context


def main() -> None:
    """Parse CLI arguments and run ETL for the selected layer."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--layer", required=True, choices=["bronze", "silver", "gold"]
    )
    parser.add_argument("--source", required=True)
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--context", required=True)
    args = parser.parse_args()

    extra_context = vars(args).copy()
    extra_context.pop("context", None)
    context = {**extra_context, **json.loads(args.context)}

    strategies = StrategyRegistry.get(args.layer)
    spark = SparkSession.builder.appName(
        f"Processing ELT from {args.layer}"
    ).getOrCreate()
    context = load_context(context, spark)

    etl = ETL(
        extract=strategies.extract,
        transform=strategies.transform,
        load=strategies.load,
        quality_strategy=strategies.quality,
        job_name=f"{args.layer}-{args.source}",
    )

    try:
        etl.run(context=context)
    finally:
        try:
            spark.sparkContext._gateway.shutdown_callback_server()
        except Exception:
            pass
        spark.stop()


if __name__ == "__main__":
    main()
