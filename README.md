# API Ingestion Pipeline - Airflow to BigQuery

Author: Diego Brito

## Architecture

```
                     +-----------------------+
                     |    API Source         |
                     |  (Rate-Limited)       |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |  Rate-Limited Client  |
                     |  - Retry with backoff |
                     |  - Cursor pagination  |
                     |  - Burst protection   |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |  Extractors           |
                     |  - Transform per type |
                     |  - Flatten nested JSON|
                     |  - Checkpoint rows    |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |  Staging Table        |
                     |  (BigQuery load job)  |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |  MERGE into Final     |
                     |  - Upsert by PK       |
                     |  - Intermediate merge |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |  Watermark Update     |
                     |  (track last sync)    |
                     +-----------------------+
```

## Features

- **Watermark-based incremental extraction**: Tracks the last synced timestamp per resource, only fetches new/updated records.
- **Rate limiting with safety margin**: Respects API burst and steady-state limits with configurable safety margin.
- **Staging + MERGE pattern**: Loads raw data into a staging table first, then merges into the final table using upsert logic.
- **Intermediate merges**: For large extractions, periodically merges into the final table to avoid staging table overflow.
- **Freshness checks**: DAG-level sensors verify data freshness before pipeline execution.
- **Modular extractors**: Each resource type has its own extractor class with type-specific transform logic.
- **Checkpointing**: Periodically saves progress so the pipeline can resume after failures.

## How It Works

1. **Config loading**: The DAG reads API keys, project IDs, and resource definitions from Airflow Variables (or environment variables as fallback).
2. **Freshness check**: Before extraction, verifies that the data is not already fresh (within a configurable window).
3. **Watermark read**: Reads the last synced watermark timestamp for the target resource from BigQuery.
4. **Extraction loop**: The API client paginates through the API, applying rate limiting and retries. Each page is transformed by the resource-specific extractor.
5. **Staging load**: Transformed rows are loaded into a staging table in BigQuery.
6. **MERGE**: The staging data is merged into the final table using a DELETE + INSERT pattern keyed on the resource's primary key.
7. **Watermark update**: The watermark timestamp is updated to reflect the latest synced record.
8. **Cleanup**: Staging tables are truncated after a successful merge.

## Technical Decisions

### Why Staging + MERGE over Direct Insert

Direct streaming inserts to BigQuery are prone to duplicate records when the pipeline retries or re-runs. The staging pattern ensures idempotency: the staging table is always truncated before a new load, and the MERGE operation guarantees exactly-once semantics by matching on primary keys.

### Why Watermark over Timestamp Comparison

The watermark approach stores a single "last successful sync" timestamp per resource. This is simpler and more reliable than comparing timestamps across tables, which can drift due to timezone differences, clock skew, or partial loads.

### Why Modular Extractors

Each API resource has a different response shape and transform logic. Modular extractors keep the code testable and maintainable. Adding a new resource only requires a new extractor class and a config entry, with no changes to the core pipeline.

### Why Intermediate Merge

For resources that produce hundreds of thousands of records (e.g., profiles, events), loading everything into a single staging table can exceed BigQuery load limits or memory constraints. Intermediate merges periodically flush the staging table into the final table, keeping resource usage bounded.

## Structure

```
dags/ingestion/marketing/
    config.py                      # Centralized configuration
    api.py                         # Rate-limited API client
    extractors.py                  # Per-resource extract/transform logic
    bigquery.py                    # BigQuery load and MERGE operations
    watermark.py                   # Watermark read/write operations
    orchestration.py               # High-level pipeline orchestration
    marketing_incremental_dag.py   # Airflow DAG definition
```

## Usage

### Environment Variables / Airflow Variables

| Variable              | Description                        |
|-----------------------|------------------------------------|
| `GCP_PROJECT_PLATFORM`| GCP project ID                     |
| `GCS_STAGING_BUCKET`  | GCS bucket for staging data        |
| `MARKETING_API_KEY`   | API key for the marketing platform |
| `MARKETING_ENABLED`   | Set to `true` to enable the pipeline |

### Running the DAG

The DAG runs on a scheduled interval defined in the DAG configuration. It can also be triggered manually via the Airflow UI or CLI.

### Disabling the Pipeline

Set the `MARKETING_ENABLED` Airflow Variable to `false`. The DAG will skip all tasks with an `AirflowSkipException`.
