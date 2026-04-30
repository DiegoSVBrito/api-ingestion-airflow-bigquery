"""
Pipeline Orchestration
=====================
High-level orchestration functions that coordinate the full ingestion flow
for a single resource: extract -> stage -> merge -> watermark update.
"""

import logging
from datetime import datetime
from typing import Optional

from ingestion.marketing.api import APIClient
from ingestion.marketing.bigquery import (
    ensure_dataset_exists,
    ensure_table_exists,
    intermediate_merge,
    load_to_staging,
    merge_staging_to_final,
)
from ingestion.marketing.config import (
    INTERMEDIATE_MERGE_EVERY,
    MAX_INCREMENTAL_RECORDS,
    RESOURCES,
    TZ,
    WATERMARK_FIELDS,
    get_batch_size,
)
from ingestion.marketing.extractors import get_extractor
from ingestion.marketing.watermark import (
    ensure_watermarks_table,
    read_watermark,
    write_watermark,
)

logger = logging.getLogger(__name__)


def run_ingestion(
    resource: str,
    run_id: Optional[str] = None,
    force_full: bool = False,
):
    """
    Execute the full ingestion pipeline for a single resource.

    Steps:
        1. Read watermark (unless force_full)
        2. Build incremental filter params
        3. Extract and transform records via the resource extractor
        4. Load into staging table
        5. MERGE staging into final table
        6. Update watermark

    For large extractions, intermediate merges are performed periodically
    to keep memory usage bounded.
    """
    logger.info("Starting ingestion for resource '%s' (run_id=%s).", resource, run_id)

    # Validate resource
    if resource not in RESOURCES:
        raise ValueError(f"Unknown resource: '{resource}'. Valid: {list(RESOURCES.keys())}")

    # Ensure infrastructure exists
    ensure_dataset_exists()
    ensure_table_exists(resource)
    ensure_watermarks_table()

    # Initialize client and extractor
    client = APIClient()
    extractor = get_extractor(resource, client)

    # Determine watermark / incremental params
    watermark = None if force_full else read_watermark(resource, run_id=run_id)
    extra_params = _build_incremental_params(resource, watermark)

    if watermark:
        logger.info(
            "Running incremental extraction for '%s' since watermark '%s'.",
            resource,
            watermark,
        )
    else:
        logger.info("Running full extraction for '%s' (no watermark or force_full=True).", resource)

    # Extract in batches with optional intermediate merges
    batch_size = get_batch_size(resource)
    all_records = []
    total_extracted = 0

    for page in client.paginate(resource, extra_params=extra_params):
        # Transform records in this page
        for raw_record in page:
            transformed = extractor.transform(raw_record)
            if transformed is not None:
                all_records.append(transformed)
                total_extracted += 1

        # Check if we need an intermediate merge
        if len(all_records) >= INTERMEDIATE_MERGE_EVERY:
            logger.info(
                "Reached intermediate merge threshold (%d records) for '%s'. "
                "Loading to staging and merging.",
                len(all_records),
                resource,
            )
            load_to_staging(resource, all_records)
            intermediate_merge(resource)
            all_records = []

        # Check max incremental cap
        if watermark and total_extracted >= MAX_INCREMENTAL_RECORDS:
            logger.warning(
                "Reached max incremental records cap (%d) for '%s'. Stopping extraction.",
                MAX_INCREMENTAL_RECORDS,
                resource,
            )
            break

    # Load remaining records
    if all_records:
        load_to_staging(resource, all_records)

    # Final merge
    merge_staging_to_final(resource)

    # Determine new watermark value
    new_watermark = _compute_new_watermark(resource, watermark)
    if new_watermark:
        write_watermark(resource, new_watermark, run_id=run_id)

    logger.info(
        "Ingestion complete for resource '%s'. Total extracted: %d.",
        resource,
        total_extracted,
    )
    return {
        "resource": resource,
        "records_extracted": total_extracted,
        "watermark": new_watermark,
        "run_id": run_id,
    }


def _build_incremental_params(resource: str, watermark: Optional[str]) -> Optional[dict]:
    """
    Build API filter parameters for incremental extraction based on watermark.

    Returns None for full extractions, or a dict with filter params for incremental.
    """
    if not watermark:
        return None

    resource_config = RESOURCES[resource]
    watermark_field = resource_config.get("watermark_field", "updated")
    existing_params = resource_config.get("extra_params") or {}

    # Build the greater-than filter for the watermark field
    filter_str = f"greater-than({watermark_field},'{watermark}')"

    if "filter" in existing_params:
        # Append to existing filter
        filter_str = f"and({existing_params['filter']},{filter_str})"

    params = dict(existing_params) if existing_params else {}
    params["filter"] = filter_str

    return params


def _compute_new_watermark(resource: str, old_watermark: Optional[str]) -> str:
    """
    Determine the new watermark value after a successful extraction.

    Uses the current timestamp as the new watermark to ensure the next
    incremental run picks up any records created during this run.
    """
    now = datetime.now(TZ)
    return now.strftime("%Y-%m-%dT%H:%M:%S%z")
