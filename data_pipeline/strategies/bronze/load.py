"""Bronze load strategy for writing raw JSON to object storage."""

import json
import os
import uuid
from datetime import date
from typing import Any

import boto3
from botocore.config import Config
from dotenv import load_dotenv

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy

load_dotenv()


class BronzeLoad(Strategy):
    def __init__(self) -> None:
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-bronze-load")

    def s3_client(self) -> Any:
        """Create an S3-compatible client for MinIO/AWS endpoints."""
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("AWS_S3_ENDPOINT"),
            aws_access_key_id=os.getenv("AWS_S3_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_S3_SECRET_KEY"),
            config=Config(signature_version="s3v4"),
            region_name=os.getenv("AWS_S3_REGION"),
        )

    def execute(
        self, data: list[dict[str, Any]], context: dict[str, Any]
    ) -> None:
        """Persist extracted data as a JSON file in the bronze bucket.

        Args:
            data: Extracted record list.
            context: Runtime context with source and destination settings.

        Returns:
            None
        """
        s3 = self.s3_client()
        execution_date = context.get(
            "execution_date", date.today().isoformat()
        )
        file_key = f"{context['source']}/{execution_date}/{uuid.uuid4()}.json"
        self.logger.info(
            "Bronze load started",
            destination_bucket=context["destination_bucket"],
            file_key=file_key,
            records=len(data),
        )

        s3.put_object(
            Bucket=context["destination_bucket"],
            Key=file_key,
            Body=json.dumps(data, indent=4, ensure_ascii=False),
            ContentType="application/json",
        )
        self.logger.info("Bronze load finished", file_key=file_key)
        return None
