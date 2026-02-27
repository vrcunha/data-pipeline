"""Bronze ETL entrypoint for API ingestion jobs."""

import argparse
import json

from dotenv import load_dotenv

from data_pipeline.etl import ETL
from data_pipeline.strategies.registry import StrategyRegistry

load_dotenv()


def main() -> None:
    """Parse CLI arguments and run ETL using bronze strategies."""
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

    etl = ETL(
        extract=strategies.extract,
        transform=strategies.transform,
        load=strategies.load,
        quality_strategy=strategies.quality,
        job_name=f"{args.layer}-{args.source}",
    )
    etl.run(context=context)


if __name__ == "__main__":
    main()
