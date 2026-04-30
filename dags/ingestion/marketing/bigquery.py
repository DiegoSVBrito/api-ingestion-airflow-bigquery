"""
BigQuery Operations
===================
Handles loading data into staging tables and MERGE operations into final tables.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

from ingestion.marketing.config import (
    BQ_DATASET,
    BQ_LOAD_JOB_TIMEOUT,
    BQ_MERGE_TIMEOUT,
    BQ_STAGING,
    BQ_TABLES,
    PROJECT_ID,
)

logger = logging.getLogger(__name__)

# Schema definitions for each resource
SCHEMAS = {
    "profiles": [
        {"name": "profile_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "organization", "type": "STRING", "mode": "NULLABLE"},
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "last_event_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "location", "type": "STRING", "mode": "NULLABLE"},
        {"name": "properties", "type": "STRING", "mode": "NULLABLE"},
        {"name": "subscriptions", "type": "STRING", "mode": "NULLABLE"},
        {"name": "self_link", "type": "STRING", "mode": "NULLABLE"},
    ],
    "events": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "profile_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_properties", "type": "STRING", "mode": "NULLABLE"},
        {"name": "metric_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "metric_name", "type": "STRING", "mode": "NULLABLE"},
    ],
    "campaigns": [
        {"name": "campaign_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "channel", "type": "STRING", "mode": "NULLABLE"},
        {"name": "subject", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "send_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "archived", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "tags", "type": "STRING", "mode": "NULLABLE"},
    ],
    "flows": [
        {"name": "flow_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "archived", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "trigger_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "tags", "type": "STRING", "mode": "NULLABLE"},
    ],
    "lists": [
        {"name": "list_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "member_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "opt_in_process", "type": "STRING", "mode": "NULLABLE"},
    ],
    "segments": [
        {"name": "segment_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "member_count", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    "metrics": [
        {"name": "metric_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "integration", "type": "STRING", "mode": "NULLABLE"},
    ],
}

# Primary key columns for MERGE operations
PRIMARY_KEYS = {
    "profiles": "profile_id",
    "events": "event_id",
    "campaigns": "campaign_id",
    "flows": "flow_id",
    "lists": "list_id",
    "segments": "segment_id",
    "metrics": "metric_id",
}


def _get_client():
    """Lazy-load the BigQuery client to avoid import errors outside GCP."""
    try:
        from google.cloud import bigquery
        return bigquery.Client(project=PROJECT_ID)
    except ImportError:
        raise ImportError("google-cloud-bigquery is required for BigQuery operations.")


def ensure_dataset_exists():
    """Create the target dataset if it does not already exist."""
    from google.cloud import bigquery

    client = _get_client()
    dataset_ref = client.dataset(BQ_DATASET)
    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset '%s' already exists.", BQ_DATASET)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.description = "Staging area for marketing API ingestion pipeline."
        client.create_dataset(dataset, exists_ok=True)
        logger.info("Dataset '%s' created.", BQ_DATASET)


def ensure_table_exists(resource: str):
    """Create the target table with the correct schema if it does not exist."""
    from google.cloud import bigquery

    client = _get_client()
    table_id = f"{PROJECT_ID}.{BQ_TABLES[resource]}"
    schema = SCHEMAS.get(resource, [])

    try:
        client.get_table(table_id)
        logger.info("Table '%s' already exists.", table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=[bigquery.SchemaField(**f) for f in schema])
        client.create_table(table, exists_ok=True)
        logger.info("Table '%s' created with %d columns.", table_id, len(schema))


def load_to_staging(resource: str, records: List[Dict[str, Any]]) -> int:
    """
    Load a list of transformed records into the staging table for the given resource.

    Truncates the staging table before loading to ensure a clean state.
    Returns the number of rows loaded.
    """
    from google.cloud import bigquery

    if not records:
        logger.info("No records to load for resource '%s'. Skipping.", resource)
        return 0

    client = _get_client()
    staging_table = f"{PROJECT_ID}.{BQ_STAGING[resource]}"

    # Ensure staging table exists with correct schema
    schema = SCHEMAS.get(resource, [])
    try:
        existing = client.get_table(staging_table)
        # Truncate existing staging data
        client.query(f"TRUNCATE TABLE `{staging_table}`").result()
        logger.info("Truncated staging table '%s'.", staging_table)
    except Exception:
        bq_schema = [bigquery.SchemaField(**f) for f in schema]
        table = bigquery.Table(staging_table, schema=bq_schema)
        client.create_table(table, exists_ok=True)
        logger.info("Created staging table '%s'.", staging_table)

    # Load records
    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(**f) for f in schema],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # Serialize records to NDJSON
    ndjson_lines = "\n".join(json.dumps(record, default=str) for record in records)

    logger.info(
        "Loading %d records into staging table '%s'...",
        len(records),
        staging_table,
    )

    load_job = client.load_table_from_json(
        records,
        staging_table,
        job_config=job_config,
    )
    load_job.result(timeout=BQ_LOAD_JOB_TIMEOUT)

    logger.info(
        "Loaded %d rows into staging table '%s'.",
        load_job.output_rows,
        staging_table,
    )
    return load_job.output_rows


def merge_staging_to_final(resource: str) -> int:
    """
    MERGE data from the staging table into the final table.

    Uses DELETE + INSERT on the primary key to guarantee idempotent upserts.
    Returns the number of rows affected.
    """
    client = _get_client()
    staging_table = f"{PROJECT_ID}.{BQ_STAGING[resource]}"
    final_table = f"{PROJECT_ID}.{BQ_TABLES[resource]}"
    pk = PRIMARY_KEYS[resource]

    merge_sql = f"""
    MERGE `{final_table}` AS target
    USING `{staging_table}` AS source
    ON target.{pk} = source.{pk}
    WHEN MATCHED THEN
        UPDATE SET
            {', '.join(f'target.{f["name"]} = source.{f["name"]}' for f in SCHEMAS[resource] if f["name"] != pk)}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(f["name"] for f in SCHEMAS[resource])})
        VALUES ({', '.join(f'source.{f["name"]}' for f in SCHEMAS[resource])})
    """

    logger.info("Executing MERGE from '%s' into '%s'...", staging_table, final_table)

    query_job = client.query(merge_sql, timeout=BQ_MERGE_TIMEOUT)
    query_job.result()

    num_dml = query_job.num_dml_affected_rows if hasattr(query_job, "num_dml_affected_rows") else -1
    logger.info("MERGE complete. Rows affected: %s", num_dml)

    # Truncate staging after successful merge
    client.query(f"TRUNCATE TABLE `{staging_table}`").result()
    logger.info("Truncated staging table '%s' after successful MERGE.", staging_table)

    return num_dml


def intermediate_merge(resource: str) -> int:
    """
    Perform an intermediate MERGE for large extractions.

    Called periodically during extraction to flush staged data and free memory.
    """
    logger.info("Starting intermediate MERGE for resource '%s'.", resource)
    return merge_staging_to_final(resource)


def get_row_count(resource: str, table_type: str = "final") -> int:
    """Return the row count for the given resource table (final or staging)."""
    client = _get_client()
    if table_type == "staging":
        table_id = f"{PROJECT_ID}.{BQ_STAGING[resource]}"
    else:
        table_id = f"{PROJECT_ID}.{BQ_TABLES[resource]}"

    try:
        table = client.get_table(table_id)
        return table.num_rows
    except Exception:
        return 0
