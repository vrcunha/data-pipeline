"""Reusable AWS helpers for Airflow DAGs."""

from __future__ import annotations

import json
from typing import Mapping

import boto3
from airflow.exceptions import AirflowException

from include.scripts.dag_base import send_alert_discord


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


def aws_infra_health_check(
    *,
    region_name: str,
    ecs_cluster_name: str,
    webhook_url: str | None = None,
) -> None:
    """Validate AWS accessibility and ECS cluster status."""
    unhealthy: list[str] = []

    try:
        sts = boto3.client("sts", region_name=region_name)
        sts.get_caller_identity()
    except Exception as error:
        unhealthy.append(f"sts: {error}")

    try:
        ecs = boto3.client("ecs", region_name=region_name)
        response = ecs.describe_clusters(clusters=[ecs_cluster_name])
        clusters = response.get("clusters", [])
        status = clusters[0].get("status") if clusters else "NOT_FOUND"
        if status != "ACTIVE":
            unhealthy.append(f"ecs cluster status: {status}")
    except Exception as error:
        unhealthy.append(f"ecs: {error}")

    if unhealthy:
        details = "\n".join(unhealthy)
        send_alert_discord(
            webhook_url,
            "Infra Health",
            "AWS service health check failed",
            f"AWS health check failed:\n{details}",
        )
        raise AirflowException(f"AWS health check failed: {unhealthy}")
