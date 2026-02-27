"""OpenBreweryDB ELT DAG for AWS resources provisioned by Terraform.

This DAG mirrors the local lakehouse flow with managed AWS services:

1. Bronze: ECS Fargate task runs the bronze extractor container.
2. Silver: AWS Glue job processes and validates silver data.
3. Gold: AWS Glue job aggregates and validates gold data.
4. Catalog: Glue crawlers refresh the Data Catalog for Athena queries.
"""

import json
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import (
    GlueCrawlerOperator,
)
from include.scripts.aws_dag_base import (
    parse_json_string_list,
    validate_aws_runtime_config,
)
from include.scripts.dag_base import (
    sla_miss_discord_callback,
    task_failure_discord_callback,
)

OWNER = "victor.cunha"
LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")
EXECUTION_DATE = "{{ data_interval_end.strftime('%Y-%m-%d') }}"

TABLE_NAME = Variable.get("TABLE_NAME", default_var="breweries")
AWS_REGION = Variable.get("AWS_REGION", default_var="us-east-1")
DISCORD_WEBHOOK_URL = Variable.get("DISCORD_WEBHOOK_URL", default_var="")
AWS_CONN_ID = Variable.get("AWS_CONN_ID", default_var="aws_default")

LIST_BREWERIES_URL = Variable.get(
    "LIST_BREWERIES_URL",
    default_var="https://api.openbrewerydb.org/v1/breweries",
)
METADATA_BREWERIES_URL = Variable.get(
    "METADATA_BREWERIES_URL",
    default_var="https://api.openbrewerydb.org/v1/breweries/meta",
)

AWS_ECS_CLUSTER_NAME = Variable.get("AWS_ECS_CLUSTER_NAME", default_var="")
AWS_ECS_TASK_DEFINITION_ARN = Variable.get(
    "AWS_ECS_TASK_DEFINITION_ARN",
    default_var="",
)
AWS_ECS_SUBNET_IDS_RAW = Variable.get("AWS_ECS_SUBNET_IDS", default_var="[]")
AWS_ECS_SECURITY_GROUP_ID = Variable.get(
    "AWS_ECS_SECURITY_GROUP_ID",
    default_var="",
)
AWS_ECS_BRONZE_CONTAINER_NAME = Variable.get(
    "AWS_ECS_BRONZE_CONTAINER_NAME",
    default_var="bronze",
)
AWS_ECS_ASSIGN_PUBLIC_IP = Variable.get(
    "AWS_ECS_ASSIGN_PUBLIC_IP",
    default_var="ENABLED",
)

AWS_GLUE_JOB_SILVER = Variable.get("AWS_GLUE_JOB_SILVER")
AWS_GLUE_JOB_GOLD = Variable.get("AWS_GLUE_JOB_GOLD")
AWS_GLUE_CRAWLER_SILVER = Variable.get("AWS_GLUE_CRAWLER_SILVER")
AWS_GLUE_CRAWLER_GOLD = Variable.get("AWS_GLUE_CRAWLER_GOLD")

AWS_S3_BRONZE_BUCKET = Variable.get("AWS_S3_BRONZE_BUCKET")
AWS_S3_SILVER_BUCKET = Variable.get("AWS_S3_SILVER_BUCKET")
AWS_S3_GOLD_BUCKET = Variable.get("AWS_S3_GOLD_BUCKET")
AWS_ECS_SUBNET_IDS = parse_json_string_list(
    AWS_ECS_SUBNET_IDS_RAW,
    "AWS_ECS_SUBNET_IDS",
)


BRONZE_CONTEXT = json.dumps(
    {
        "source": TABLE_NAME,
        "list_breweries_url": LIST_BREWERIES_URL,
        "metadata_breweries_url": METADATA_BREWERIES_URL,
        "destination_bucket": AWS_S3_BRONZE_BUCKET,
    }
)
SILVER_CONTEXT = json.dumps(
    {
        "source_bucket": AWS_S3_BRONZE_BUCKET,
        "source_path": f"{TABLE_NAME}/{EXECUTION_DATE}/",
        "destination_bucket": AWS_S3_SILVER_BUCKET,
        "destination_path": f"{TABLE_NAME}/",
    }
)
GOLD_CONTEXT = json.dumps(
    {
        "source_bucket": AWS_S3_SILVER_BUCKET,
        "source_path": f"{TABLE_NAME}/",
        "destination_bucket": AWS_S3_GOLD_BUCKET,
        "destination_path": f"{TABLE_NAME}/",
        "upsert_keys": ["gold_hashkey"],
    }
)

REQUIRED_VARIABLES = {
    "AWS_ECS_CLUSTER_NAME": AWS_ECS_CLUSTER_NAME,
    "AWS_ECS_TASK_DEFINITION_ARN": AWS_ECS_TASK_DEFINITION_ARN,
    "AWS_ECS_SECURITY_GROUP_ID": AWS_ECS_SECURITY_GROUP_ID,
    "AWS_GLUE_JOB_SILVER": AWS_GLUE_JOB_SILVER,
    "AWS_GLUE_JOB_GOLD": AWS_GLUE_JOB_GOLD,
    "AWS_GLUE_CRAWLER_SILVER": AWS_GLUE_CRAWLER_SILVER,
    "AWS_GLUE_CRAWLER_GOLD": AWS_GLUE_CRAWLER_GOLD,
    "AWS_S3_BRONZE_BUCKET": AWS_S3_BRONZE_BUCKET,
    "AWS_S3_SILVER_BUCKET": AWS_S3_SILVER_BUCKET,
    "AWS_S3_GOLD_BUCKET": AWS_S3_GOLD_BUCKET,
}


default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=6),
    "on_failure_callback": partial(
        task_failure_discord_callback,
        webhook_url=DISCORD_WEBHOOK_URL,
    ),
    "sla": timedelta(minutes=45),
}


@dag(
    dag_id=Path(__file__).stem,
    schedule_interval=None,
    start_date=datetime(2026, 2, 27, tzinfo=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=8),
    sla_miss_callback=partial(
        sla_miss_discord_callback,
        webhook_url=DISCORD_WEBHOOK_URL,
    ),
    tags=["aws", "bronze", "silver", "gold", "lakehouse", TABLE_NAME],
)
def openbrewerydb_aws_dag() -> None:
    """Build the AWS lakehouse DAG graph and task dependencies."""
    dag.doc_md = __doc__

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    validate_config = PythonOperator(
        task_id="validate_runtime_config",
        python_callable=validate_aws_runtime_config,
        op_kwargs={
            "required": REQUIRED_VARIABLES,
            "ecs_subnet_ids": AWS_ECS_SUBNET_IDS,
        },
    )

    bronze = EcsRunTaskOperator(
        task_id=f"bronze__{TABLE_NAME}",
        aws_conn_id=AWS_CONN_ID,
        region=AWS_REGION,
        cluster=AWS_ECS_CLUSTER_NAME,
        task_definition=AWS_ECS_TASK_DEFINITION_ARN,
        launch_type="FARGATE",
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": AWS_ECS_SUBNET_IDS,
                "securityGroups": [AWS_ECS_SECURITY_GROUP_ID],
                "assignPublicIp": AWS_ECS_ASSIGN_PUBLIC_IP,
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": AWS_ECS_BRONZE_CONTAINER_NAME,
                    "command": [
                        "bronze.py",
                        "--layer",
                        "bronze",
                        "--source",
                        TABLE_NAME,
                        "--execution_date",
                        EXECUTION_DATE,
                        "--context",
                        BRONZE_CONTEXT,
                    ],
                }
            ]
        },
        wait_for_completion=True,
        do_xcom_push=False,
    )

    silver = GlueJobOperator(
        task_id=f"silver__{TABLE_NAME}",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        job_name=AWS_GLUE_JOB_SILVER,
        script_args={
            "--layer": "silver",
            "--source": TABLE_NAME,
            "--execution_date": EXECUTION_DATE,
            "--context": SILVER_CONTEXT,
        },
        wait_for_completion=True,
    )

    silver_crawler = GlueCrawlerOperator(
        task_id=f"catalog_silver__{TABLE_NAME}",
        aws_conn_id=AWS_CONN_ID,
        config={"Name": AWS_GLUE_CRAWLER_SILVER},
    )

    gold = GlueJobOperator(
        task_id=f"gold__{TABLE_NAME}",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        job_name=AWS_GLUE_JOB_GOLD,
        script_args={
            "--layer": "gold",
            "--source": TABLE_NAME,
            "--execution_date": EXECUTION_DATE,
            "--context": GOLD_CONTEXT,
        },
        wait_for_completion=True,
    )

    gold_crawler = GlueCrawlerOperator(
        task_id=f"catalog_gold__{TABLE_NAME}",
        aws_conn_id=AWS_CONN_ID,
        config={"Name": AWS_GLUE_CRAWLER_GOLD},
    )

    start >> validate_config >> bronze >> silver >> gold >> end
    silver >> silver_crawler
    gold >> gold_crawler


openbrewerydb_aws_dag()
