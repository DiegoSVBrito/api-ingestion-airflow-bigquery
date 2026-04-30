"""
Watermark Management
====================
Handles reading and writing watermark timestamps for incremental extraction.
Watermarks track the last successful sync point per resource in BigQuery.
"""

import logging
from datetime import datetime
from typing import Optional

from ingestion.marketing.config import (
    BQ_DATASET,
    BQ_TABLES,
    PROJECT_ID,
    TZ,
    WATERMARK_FIELDS,
)

logger = logging.getLogger(__name__)

CREATE_WATERMARKS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BQ_DATASET}.watermarks` (
    resource STRING NOT NULL,
    watermark_field STRING NOT NULL,
    watermark_value TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    run_id STRING
)
PARTITION BY DATE(updated_at)
CLUSTER BY resource
"""


def _get_client():
    """Lazy-load the BigQuery client."""
    try:
        from google.cloud import bigquery
        return bigquery.Client(project=PROJECT_ID)
    except ImportError:
        raise ImportError("google-cloud-bigquery is required.")


def ensure_watermarks_table():
    """Create the watermarks tracking table if it does not exist."""
    client = _get_client()
    table_id = f"{PROJECT_ID}.{BQ_TABLES['watermarks']}"
    try:
        client.get_table(table_id)
        logger.info("Watermarks table '%s' already exists.", table_id)
    except Exception:
        client.query(CREATE_WATERMARKS_TABLE_SQL).result()
        logger.info("Watermarks table '%s' created.", table_id)


def read_watermark(resource: str, run_id: Optional[str] = None) -> Optional[str]:
    """
    Read the last watermark timestamp for the given resource.

    Returns the watermark value as an ISO 8601 string, or None if no watermark exists.
    """
    client = _get_client()
    table_id = f"{PROJECT_ID}.{BQ_TABLES['watermarks']}"

    query = f"""
    SELECT watermark_value
    FROM `{table_id}`
    WHERE resource = @resource
    ORDER BY updated_at DESC
    LIMIT 1
    """

    from google.cloud import bigquery
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resource", "STRING", resource),
        ]
    )

    result = client.query(query, job_config=job_config).result()
    rows = list(result)

    if not rows:
        logger.info("No watermark found for resource '%s'. Will perform full extraction.", resource)
        return None

    watermark_value = rows[0]["watermark_value"]
    if hasattr(watermark_value, "isoformat"):
        watermark_value = watermark_value.isoformat()

    logger.info("Watermark for resource '%s': %s", resource, watermark_value)
    return watermark_value


def write_watermark(resource: str, watermark_value: str, run_id: Optional[str] = None):
    """
    Write or update the watermark timestamp for the given resource.

    Uses DELETE + INSERT to ensure only one watermark per resource.
    """
    client = _get_client()
    table_id = f"{PROJECT_ID}.{BQ_TABLES['watermarks']}"
    watermark_field = WATERMARK_FIELDS.get(resource, "updated")

    now = datetime.now(TZ).isoformat()

    delete_sql = f"""
    DELETE FROM `{table_id}`
    WHERE resource = @resource
    """

    insert_sql = f"""
    INSERT INTO `{table_id}` (resource, watermark_field, watermark_value, updated_at, run_id)
    VALUES (@resource, @watermark_field, @watermark_value, @updated_at, @run_id)
    """

    from google.cloud import bigquery
    params = [
        bigquery.ScalarQueryParameter("resource", "STRING", resource),
        bigquery.ScalarQueryParameter("watermark_field", "STRING", watermark_field),
        bigquery.ScalarQueryParameter("watermark_value", "TIMESTAMP", watermark_value),
        bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", now),
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id or ""),
    ]

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    # Execute delete then insert in sequence
    client.query(delete_sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("resource", "STRING", resource)]
    )).result()

    client.query(insert_sql, job_config=job_config).result()

    logger.info(
        "Watermark updated for resource '%s': %s (run_id=%s)",
        resource,
        watermark_value,
        run_id,
    )


def get_all_watermarks() -> dict:
    """Return a dict mapping resource -> watermark_value for all tracked resources."""
    client = _get_client()
    table_id = f"{PROJECT_ID}.{BQ_TABLES['watermarks']}"

    query = f"""
    SELECT resource, watermark_value
    FROM `{table_id}`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY resource ORDER BY updated_at DESC) = 1
    """

    result = client.query(query).result()
    watermarks = {}
    for row in result:
        val = row["watermark_value"]
        if hasattr(val, "isoformat"):
            val = val.isoformat()
        watermarks[row["resource"]] = val

    return watermarks
