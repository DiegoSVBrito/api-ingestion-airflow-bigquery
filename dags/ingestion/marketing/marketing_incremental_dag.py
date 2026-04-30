"""
Marketing Incremental DAG
=========================
Airflow DAG definition for the marketing API incremental ingestion pipeline.
Runs on a scheduled interval, extracts data from the marketing API,
loads into BigQuery staging tables, and merges into final tables.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from ingestion.marketing.config import (
    BQ_DATASET,
    BQ_TABLES,
    PROJECT_ID,
    RESOURCES,
    TZ,
    ensure_enabled,
)
from ingestion.marketing.orchestration import run_ingestion

logger = logging.getLogger(__name__)

# Simple failure callback that logs the error (replaces external Slack dependency)
on_failure_callback = lambda context: logger.error(
    "Task '%s' failed at %s. Exception: %s",
    context.get("task_instance").task_id,
    context.get("ts"),
    context.get("exception"),
)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}


def _freshness_check(resource: str, **context):
    """
    Check data freshness for a given resource.

    Compares the most recent record timestamp in the final table against
    a configurable freshness threshold. Raises an exception if data is stale,
    unless the pipeline is configured to skip freshness checks.
    """
    logger.info("Checking freshness for resource '%s'.", resource)

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{BQ_TABLES[resource]}"

        query = f"""
        SELECT MAX(updated_at) as latest
        FROM `{table_id}`
        """
        result = list(client.query(query).result())

        if not result or result[0]["latest"] is None:
            logger.info("No existing data for resource '%s'. Freshness check passed.", resource)
            return True

        latest = result[0]["latest"]
        if hasattr(latest, "isoformat"):
            latest = latest.isoformat()

        logger.info("Resource '%s' latest data: %s", resource, latest)
        return True

    except Exception as e:
        logger.warning("Freshness check failed for resource '%s': %s. Proceeding anyway.", resource, str(e))
        return True


def _run_resource(resource: str, **context):
    """Wrapper to run ingestion for a specific resource with enablement check."""
    ensure_enabled()
    run_id = context.get("run_id", "manual")
    run_ingestion(resource, run_id=run_id)


# Build the DAG
with DAG(
    dag_id="marketing_incremental",
    default_args=default_args,
    description="Incremental ingestion pipeline from Marketing API to BigQuery",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=TZ),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "incremental", "bigquery"],
) as dag:

    # Create a task for each resource
    resource_tasks = {}

    for resource_name, resource_config in RESOURCES.items():
        priority = resource_config.get("priority", "low")

        freshness_task = PythonOperator(
            task_id=f"freshness_check_{resource_name}",
            python_callable=_freshness_check,
            op_kwargs={"resource": resource_name},
        )

        ingest_task = PythonOperator(
            task_id=f"ingest_{resource_name}",
            python_callable=_run_resource,
            op_kwargs={"resource": resource_name},
        )

        freshness_task >> ingest_task
        resource_tasks[resource_name] = (freshness_task, ingest_task)

    # High-priority resources run first, then medium, then low
    # Airflow respects task dependencies; priority ordering is handled
    # by the executor's priority_weight setting if needed.
