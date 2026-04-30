# Architecture Decision Records

Author: Diego Brito

## ADR-001: Staging + MERGE Pattern

**Status**: Accepted

**Context**: The pipeline needs to load data from an external API into BigQuery. Direct streaming inserts are prone to duplicates when the pipeline retries, re-runs, or encounters partial failures.

**Decision**: Use a two-step pattern: load raw data into a staging table, then MERGE into the final table using DELETE + INSERT on primary keys.

**Consequences**:
- Positive: Idempotent pipeline runs. Re-running the DAG produces the same result without duplicates.
- Positive: Staging table acts as a quarantine zone where data quality checks can be applied before merging.
- Negative: Requires additional storage for staging tables and an extra MERGE query per run.
- Negative: Slightly higher latency compared to direct streaming inserts.

## ADR-002: Watermark-Based Incremental Extraction

**Status**: Accepted

**Context**: The API returns large datasets (hundreds of thousands of records). Full extraction on every run is slow and expensive. We need a way to only fetch new or updated records.

**Decision**: Store a "watermark" timestamp per resource in a dedicated BigQuery table. Each run reads the watermark, constructs an API filter for records newer than the watermark, and updates the watermark after a successful run.

**Consequences**:
- Positive: Dramatically reduces API calls and processing time for incremental runs.
- Positive: Simple to reason about -- one timestamp per resource.
- Negative: If the API returns records out of order, some records may be missed. We mitigate this by setting the new watermark to the current time rather than the last record's timestamp.
- Negative: No built-in deduplication at the API level; relies on the MERGE pattern to handle duplicates.

## ADR-003: Modular Extractor Design

**Status**: Accepted

**Context**: The pipeline ingests 7 different resource types, each with a different response shape, transform logic, and pagination behavior. A monolithic extractor would be difficult to test and maintain.

**Decision**: Define a BaseExtractor abstract class with a common `extract()` method. Each resource type has its own subclass that implements `transform()`. A registry maps resource names to extractor classes.

**Consequences**:
- Positive: Each extractor is independently testable.
- Positive: Adding a new resource only requires a new extractor class and a config entry.
- Positive: Clear separation of concerns between extraction orchestration and per-record transformation.
- Negative: More files and classes to maintain.
- Negative: Slightly more complex import structure.

## ADR-004: Intermediate Merge Strategy

**Status**: Accepted

**Context**: Some resources (profiles, events) produce hundreds of thousands of records per extraction. Loading all records into a single staging table can exceed BigQuery load limits or cause memory pressure.

**Decision**: After every N records (configurable via `INTERMEDIATE_MERGE_EVERY`), load the accumulated batch into staging and perform a MERGE into the final table. Continue extraction after the merge.

**Consequences**:
- Positive: Bounded memory usage regardless of dataset size.
- Positive: If the pipeline fails midway, records already merged are preserved and do not need to be re-extracted.
- Negative: Multiple MERGE queries per run increase total BigQuery costs.
- Negative: Requires careful watermark management to avoid double-counting or missing records on retry.
