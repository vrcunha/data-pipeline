"""OpenBreweryDB ELT DAG.

This DAG orchestrates a three-layer pipeline:

1. Bronze: API extraction and raw JSON landing in MinIO.
2. Silver: Spark transformation with Delta output and quality checks.
3. Gold: Spark aggregation with Delta output and quality checks.

Monitoring behavior:
- Task failure notifications to Discord.
- SLA miss warning and Discord notifications.
- Infra health guardrail before ingestion (MinIO, Spark Master, Trino).

Notification target:
- Discord webhook from environment variable `DISCORD_WEBHOOK_URL`.
"""

import json
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.providers.docker.operators.docker import DockerOperator
from include.scripts.dag_base import (
    infra_health_check,
    sla_miss_discord_callback,
    task_failure_discord_callback,
)

OWNER = "victor.cunha"
TABLE_NAME = "breweries"

EXECUTION_DATE = "{{ data_interval_end.strftime('%Y-%m-%d') }}"
BRONZE_CONTEXT = json.dumps(
    {
        "source": TABLE_NAME,
        "list_breweries_url": "https://api.openbrewerydb.org/v1/breweries",
        "metadata_breweries_url": "https://api.openbrewerydb.org/v1/breweries/meta",
        "destination_bucket": "bronze",
    }
)
SILVER_CONTEXT = json.dumps(
    (
        {
            "source_bucket": "bronze",
            "source_path": f"{TABLE_NAME}/{EXECUTION_DATE}/",
            "destination_bucket": "silver",
            "destination_path": f"{TABLE_NAME}/",
        }
    )
)
GOLD_CONTEXT = json.dumps(
    {
        "source_bucket": "silver",
        "source_path": f"{TABLE_NAME}/",
        "destination_bucket": "gold",
        "destination_path": f"{TABLE_NAME}/",
        "upsert_keys": ["state", "city", "brewery_type"],
    }
)
LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")

DISCORD_WEBHOOK_URL = Variable.get("DISCORD_WEBHOOK_URL")
SPARK_MASTER_URL = Variable.get("SPARK_MASTER_URL")
AWS_S3_ENDPOINT = Variable.get("AWS_S3_ENDPOINT")
AWS_S3_ACCESS_KEY = Variable.get("AWS_S3_ACCESS_KEY")
AWS_S3_SECRET_KEY = Variable.get("AWS_S3_SECRET_KEY")
AWS_S3_REGION = Variable.get("AWS_S3_REGION")
DOCKER_BRONZE_IMAGE = Variable.get("DOCKER_BRONZE_IMAGE")
DOCKER_NETWORK_MODE = Variable.get("DOCKER_NETWORK_MODE")
DOCKER_URL = Variable.get("DOCKER_URL")

INFRA_HEALTH_CHECKS = {
    "minio": "http://minio:9000/minio/health/live",
    "spark_master": "http://spark-master:8080/json/",
    "trino": "http://trino:8080/v1/info",
}


default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=5),
    "on_failure_callback": partial(
        task_failure_discord_callback,
        webhook_url=DISCORD_WEBHOOK_URL,
    ),
    "sla": timedelta(minutes=30),
}


@dag(
    dag_id=Path(__file__).stem,
    schedule_interval=None,
    start_date=datetime(2026, 2, 21, tzinfo=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=6),
    sla_miss_callback=partial(
        sla_miss_discord_callback,
        webhook_url=DISCORD_WEBHOOK_URL,
    ),
    tags=["bronze", "silver", "gold", "lakehouse", TABLE_NAME],
)
def brewery_dag():
    """Build the OpenBreweryDB DAG graph and task dependencies."""
    dag.doc_md = __doc__

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    infra_health = PythonOperator(
        task_id="infra_health_check",
        python_callable=partial(
            infra_health_check,
            checks=INFRA_HEALTH_CHECKS,
            webhook_url=DISCORD_WEBHOOK_URL,
        ),
    )

    bronze = DockerOperator(
        task_id=f"bronze__{TABLE_NAME}",
        image=DOCKER_BRONZE_IMAGE,
        docker_url=DOCKER_URL,
        api_version="auto",
        auto_remove="success",
        network_mode=DOCKER_NETWORK_MODE,
        command=[
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
        environment={
            "AWS_S3_ENDPOINT": AWS_S3_ENDPOINT,
            "AWS_S3_ACCESS_KEY": AWS_S3_ACCESS_KEY,
            "AWS_S3_SECRET_KEY": AWS_S3_SECRET_KEY,
            "AWS_S3_REGION": AWS_S3_REGION,
        },
        force_pull=False,
    )

    silver = SparkSubmitOperator(
        task_id=f"silver__{TABLE_NAME}",
        application="/opt/spark/jobs/main.py",
        conn_id="spark_conn",
        conf={
            "spark.jars.packages": "io.delta:delta-sharing-spark_2.12:3.2.0,com.amazon.deequ:deequ:2.0.7-spark-3.5,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",  # NOQA
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",  # NOQA
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",  # NOQA
            "spark.hadoop.fs.s3a.endpoint": AWS_S3_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": AWS_S3_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": AWS_S3_SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        application_args=[
            "--layer",
            "silver",
            "--source",
            TABLE_NAME,
            "--execution_date",
            EXECUTION_DATE,
            "--context",
            SILVER_CONTEXT,
        ],
        verbose=True,
    )

    register_silver_table = BashOperator(
        task_id=f"register_silver_table__{TABLE_NAME}",
        bash_command=(
            "bash /opt/spark/jobs/register_table_trino.sh "
            f"silver {TABLE_NAME}"
        ),
    )

    gold = SparkSubmitOperator(
        task_id=f"gold__{TABLE_NAME}",
        application="/opt/spark/jobs/main.py",
        conn_id="spark_conn",
        conf={
            "spark.jars.packages": "io.delta:delta-sharing-spark_2.12:3.2.0,com.amazon.deequ:deequ:2.0.7-spark-3.5,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",  # NOQA
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",  # NOQA
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",  # NOQA
            "spark.hadoop.fs.s3a.endpoint": AWS_S3_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": AWS_S3_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": AWS_S3_SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        application_args=[
            "--layer",
            "gold",
            "--source",
            TABLE_NAME,
            "--execution_date",
            EXECUTION_DATE,
            "--context",
            GOLD_CONTEXT,
        ],
        verbose=True,
    )

    register_gold_table = BashOperator(
        task_id=f"register_gold_table__{TABLE_NAME}",
        bash_command=(
            "bash /opt/spark/jobs/register_table_trino.sh "
            f"gold {TABLE_NAME}"
        ),
    )

    start >> infra_health >> bronze >> silver >> gold >> end
    silver >> register_silver_table
    gold >> register_gold_table


brewery_dag()
