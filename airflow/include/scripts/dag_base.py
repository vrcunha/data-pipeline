"""Reusable base helpers for Airflow DAGs."""

from __future__ import annotations

import logging
from typing import Mapping

import requests
from airflow.exceptions import AirflowException


def send_alert_discord(
    webhook_url: str | None,
    alert_type: str,
    subject: str,
    body: str,
) -> None:
    """Send an alert message to Discord."""
    if not webhook_url:
        logging.warning("DISCORD_WEBHOOK_URL is not set. Alert skipped.")
        return

    message = f"**[Airflow][{alert_type}] {subject}**\n{body}"

    try:
        response = requests.post(
            webhook_url,
            json={"content": message[:1900]},
            timeout=10,
        )
        response.raise_for_status()
    except Exception as error:  # pragma: no cover - network side effect
        logging.warning("Failed to send Discord alert: %s", error)


def infra_health_check(
    checks: Mapping[str, str],
    webhook_url: str | None = None,
) -> None:
    """Validate service endpoints before data processing."""
    unhealthy: list[str] = []

    for service, url in checks.items():
        try:
            response = requests.get(url, timeout=10)
            if response.status_code >= 400:
                unhealthy.append(f"{service}: HTTP {response.status_code}")
        except Exception as error:  # pragma: no cover - network side effect
            unhealthy.append(f"{service}: {error}")

    if unhealthy:
        details = "\n".join(unhealthy)
        send_alert_discord(
            webhook_url,
            "Infra Health",
            "Service health check failed",
            f"Infra health check failed:\n{details}",
        )
        raise AirflowException(f"Infra health check failed: {unhealthy}")


def task_failure_discord_callback(
    context: dict,
    webhook_url: str | None = None,
) -> None:
    """Send Discord alert when a task fails."""
    task_instance = context.get("task_instance")
    dag_id = task_instance.dag_id if task_instance else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    exception = context.get("exception")
    log_url = task_instance.log_url if task_instance else "N/A"

    subject = f"{dag_id}.{task_id}"
    message = (
        "Airflow Task Failure\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run ID: {run_id}\n"
        f"Exception: {exception}\n"
        f"Log URL: {log_url}"
    )
    send_alert_discord(webhook_url, "Failure", subject, message)


def sla_miss_discord_callback(
    dag,
    task_list,
    blocking_task_list,
    slas,
    blocking_tis,
    webhook_url: str | None = None,
) -> None:
    """Send warning and Discord alert when SLA misses are reported."""
    logging.warning(
        "SLA missed detected for DAG %s. Tasks: %s",
        dag.dag_id,
        task_list,
    )

    subject = f"{dag.dag_id}"
    message = (
        "Airflow SLA Missed\n"
        f"DAG: {dag.dag_id}\n"
        f"Tasks: {task_list}\n"
        f"Blocking Tasks: {blocking_task_list}\n"
        f"SLA Records: {slas}\n"
        f"Blocking TIs: {blocking_tis}"
    )
    send_alert_discord(webhook_url, "SLA Missed", subject, message)
