"""
Marketing API Configuration
===========================
Centralized settings for the Marketing API -> BigQuery ingestion pipeline.
"""

import os
from zoneinfo import ZoneInfo

try:
    from airflow.exceptions import AirflowSkipException
    from airflow.models import Variable
except ImportError:
    class Variable:
        @staticmethod
        def get(key, default_var=None):
            return os.getenv(key, default_var)
    class AirflowSkipException(Exception):
        pass


def ensure_enabled():
    """Check if the pipeline is enabled and the API key is configured."""
    enabled = str(Variable.get("MARKETING_ENABLED", "false")).strip().lower() == "true"
    if not enabled:
        raise AirflowSkipException("Marketing pipeline is DISABLED.")
    api_key = str(Variable.get("MARKETING_API_KEY", "")).strip()
    if not api_key:
        raise ValueError("MARKETING_API_KEY is not configured.")


PROJECT_ID = Variable.get("GCP_PROJECT_PLATFORM", "my-gcp-project")
GCS_BUCKET = Variable.get("GCS_STAGING_BUCKET", "my-staging-bucket")
BQ_DATASET = "stg_marketing"

BQ_TABLES = {
    "profiles": f"{BQ_DATASET}.raw_profiles",
    "events": f"{BQ_DATASET}.raw_events",
    "campaigns": f"{BQ_DATASET}.raw_campaigns",
    "flows": f"{BQ_DATASET}.raw_flows",
    "lists": f"{BQ_DATASET}.raw_lists",
    "segments": f"{BQ_DATASET}.raw_segments",
    "metrics": f"{BQ_DATASET}.raw_metrics",
    "watermarks": f"{BQ_DATASET}.watermarks",
}

BQ_STAGING = {
    resource: f"{BQ_DATASET}.staging_{resource}"
    for resource in ["profiles", "events", "campaigns", "flows", "lists", "segments", "metrics"]
}

API_KEY = Variable.get("MARKETING_API_KEY", "")
API_BASE_URL = "https://api.example.com/api"
API_REVISION = "2024-10-15"

HEADERS = (
        {
            "Authorization": f"Api-Key {API_KEY}",
            "revision": API_REVISION,
            "accept": "application/json",
            "content-type": "application/json",
        }
        if API_KEY
        else {}
)

RATE_LIMIT = {
    "burst": 350,
    "steady": 3500,
    "safety_margin": 0.8,
    "min_remaining": 10,
}

WATERMARK_FIELDS = {
    "profiles": "updated",
    "events": "datetime",
    "campaigns": "updated_at",
    "flows": "updated",
    "lists": "updated",
    "segments": "updated",
    "metrics": "updated",
}

RETRIES = 5
TIMEOUT = 30
REQUEST_DELAY = 0.05
CHECKPOINT_EVERY = 1000

BATCH_SIZE = 100_000
BATCH_SIZES = {
    "profiles": 100_000,
    "events": 100_000,
    "campaigns": 50_000,
    "flows": 50_000,
    "lists": 50_000,
    "segments": 50_000,
    "metrics": 50_000,
}

INTERMEDIATE_MERGE_EVERY = 100_000
MAX_INCREMENTAL_RECORDS = 500_000

BQ_LOAD_JOB_TIMEOUT = 300
BQ_MERGE_TIMEOUT = 600

TZ = ZoneInfo("America/New_York")


def get_batch_size(resource: str) -> int:
    """Return the configured batch size for a given resource type."""
    return BATCH_SIZES.get(resource, BATCH_SIZE)


RESOURCES = {
    "profiles": {
        "endpoint": "/profiles",
        "page_size": 100,
        "supports_pagination": True,
        "supports_sort": True,
        "watermark_field": "updated",
        "sort": "updated",
        "extra_params": None,
        "priority": "high",
    },
    "events": {
        "endpoint": "/events",
        "page_size": 200,
        "supports_pagination": True,
        "supports_sort": True,
        "watermark_field": "datetime",
        "sort": "datetime",
        "extra_params": None,
        "priority": "high",
    },
    "campaigns": {
        "endpoint": "/campaigns",
        "page_size": 50,
        "supports_pagination": False,
        "supports_sort": False,
        "watermark_field": "updated_at",
        "sort": None,
        "extra_params": {"filter": "equals(messages.channel,'email')"},
        "priority": "medium",
    },
    "flows": {
        "endpoint": "/flows",
        "page_size": 50,
        "supports_pagination": False,
        "supports_sort": False,
        "watermark_field": "updated",
        "sort": None,
        "extra_params": None,
        "priority": "low",
    },
    "lists": {
        "endpoint": "/lists",
        "page_size": 50,
        "supports_pagination": False,
        "supports_sort": False,
        "watermark_field": "updated",
        "sort": None,
        "extra_params": None,
        "priority": "low",
    },
    "segments": {
        "endpoint": "/segments",
        "page_size": 50,
        "supports_pagination": False,
        "supports_sort": False,
        "watermark_field": "updated",
        "sort": None,
        "extra_params": None,
        "priority": "low",
    },
    "metrics": {
        "endpoint": "/metrics",
        "page_size": 50,
        "supports_pagination": False,
        "supports_sort": False,
        "watermark_field": "updated",
        "sort": None,
        "extra_params": None,
        "priority": "low",
    },
}
