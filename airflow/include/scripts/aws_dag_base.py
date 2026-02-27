"""Reusable AWS helpers for Airflow DAGs."""

from __future__ import annotations

import json
from typing import Mapping

from airflow.exceptions import AirflowException


def parse_json_string_list(raw_value: str, variable_name: str) -> list[str]:
    """Parse a JSON string list from an Airflow Variable."""
    try:
        values = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise AirflowException(
            f"{variable_name} must be a JSON list. "
            'Example: ["subnet-abc", "subnet-def"]'
        ) from error

    if not isinstance(values, list) or not all(
        isinstance(item, str) for item in values
    ):
        raise AirflowException(
            f"{variable_name} must be a JSON list of strings."
        )

    return values


def validate_required_variables(required: Mapping[str, str]) -> None:
    """Validate required runtime variables."""
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise AirflowException(
            "Missing required Airflow Variables: " + ", ".join(missing)
        )


def validate_aws_runtime_config(
    required: Mapping[str, str],
    ecs_subnet_ids: list[str],
) -> None:
    """Validate required variables and ECS networking settings."""
    validate_required_variables(required)
    if not ecs_subnet_ids:
        raise AirflowException(
            "Missing required Airflow Variables: AWS_ECS_SUBNET_IDS"
        )
