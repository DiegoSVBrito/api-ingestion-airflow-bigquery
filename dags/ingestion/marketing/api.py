"""
Rate-Limited API Client
========================
HTTP client with rate limiting, retry logic, and cursor-based pagination
for the Marketing API.
"""

import json
import time
import logging
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests

from ingestion.marketing.config import (
    API_BASE_URL,
    HEADERS,
    RATE_LIMIT,
    REQUEST_DELAY,
    RETRIES,
    TIMEOUT,
    RESOURCES,
)

logger = logging.getLogger(__name__)


class RateLimitTracker:
    """Tracks API rate limit headers and enforces backoff when limits are low."""

    def __init__(self, burst_limit: int = None, steady_limit: int = None, safety_margin: float = None):
        config = RATE_LIMIT
        self.burst_limit = burst_limit or config["burst"]
        self.steady_limit = steady_limit or config["steady"]
        self.safety_margin = safety_margin or config["safety_margin"]
        self.min_remaining = config["min_remaining"]
        self.last_request_time = 0.0
        self.remaining = self.burst_limit
        self.reset_at = 0.0

    def update_from_headers(self, headers: Dict[str, str]):
        """Update tracker state from API response headers."""
        for key, value in headers.items():
            key_lower = key.lower()
            if "rate-limit-remaining" in key_lower:
                try:
                    self.remaining = int(value)
                except (ValueError, TypeError):
                    pass
            elif "rate-limit-reset" in key_lower:
                try:
                    self.reset_at = time.time() + float(value)
                except (ValueError, TypeError):
                    pass

    def wait_if_needed(self):
        """Block until it is safe to make the next request."""
        if self.remaining <= self.min_remaining:
            wait_time = max(self.reset_at - time.time(), 1.0)
            logger.warning(
                "Rate limit nearly exhausted (remaining=%d). Waiting %.1fs until reset.",
                self.remaining,
                wait_time,
            )
            time.sleep(wait_time)

        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)

        self.last_request_time = time.time()


class APIClient:
    """
    Generic rate-limited API client with pagination support.

    Handles authentication, retries with exponential backoff, cursor-based
    pagination, and automatic rate limit enforcement.
    """

    def __init__(self, rate_tracker: RateLimitTracker = None):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.tracker = rate_tracker or RateLimitTracker()
        self._total_requests = 0
        self._total_retries = 0

    @property
    def total_requests(self) -> int:
        return self._total_requests

    @property
    def total_retries(self) -> int:
        return self._total_retries

    def _request(
        self,
        method: str,
        url: str,
        params: Optional[Dict] = None,
        json_body: Optional[Dict] = None,
    ) -> requests.Response:
        """Execute an HTTP request with retry logic and rate limiting."""
        self.tracker.wait_if_needed()

        last_exception = None
        for attempt in range(1, RETRIES + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    timeout=TIMEOUT,
                )
                self._total_requests += 1
                self.tracker.update_from_headers(dict(response.headers))

                if response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 60))
                    logger.warning("Rate limited (429). Waiting %ss before retry #%d.", retry_after, attempt)
                    time.sleep(retry_after)
                    continue

                if response.status_code >= 500:
                    backoff = 2 ** attempt
                    logger.warning(
                        "Server error %d on attempt #%d/%d. Backing off %ds.",
                        response.status_code,
                        attempt,
                        RETRIES,
                        backoff,
                    )
                    time.sleep(backoff)
                    continue

                response.raise_for_status()
                return response

            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exception = exc
                backoff = 2 ** attempt
                logger.warning(
                    "Connection error on attempt #%d/%d: %s. Backing off %ds.",
                    attempt,
                    RETRIES,
                    str(exc),
                    backoff,
                )
                time.sleep(backoff)
                self._total_retries += 1

        raise ConnectionError(
            f"Failed after {RETRIES} retries. Last error: {last_exception}"
        )

    def _get(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute a GET request and return parsed JSON."""
        response = self._request("GET", url, params=params)
        return response.json()

    def paginate(
        self,
        resource: str,
        extra_params: Optional[Dict] = None,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Yield pages of records for a given resource using cursor-based pagination.

        Falls back to a single page fetch for resources that do not support pagination.
        """
        resource_config = RESOURCES[resource]
        endpoint = resource_config["endpoint"]
        url = f"{API_BASE_URL}{endpoint}"
        page_size = resource_config["page_size"]
        supports_pagination = resource_config["supports_pagination"]
        supports_sort = resource_config["supports_sort"]
        sort_field = resource_config.get("sort")

        params = {"page[size]": page_size}
        if supports_sort and sort_field:
            params["sort"] = sort_field
        if extra_params:
            params.update(extra_params)

        page_number = 0
        cursor = None

        while True:
            if cursor:
                params["page[cursor]"] = cursor

            data = self._get(url, params=params)
            page_number += 1

            records = data.get("data", [])
            if not records:
                logger.info(
                    "Resource '%s': page %d returned 0 records. Pagination complete.",
                    resource,
                    page_number,
                )
                break

            logger.info(
                "Resource '%s': fetched page %d with %d records.",
                resource,
                page_number,
                len(records),
            )
            yield records

            if not supports_pagination:
                logger.info("Resource '%s': pagination not supported. Single page fetch complete.", resource)
                break

            links = data.get("links", {})
            next_cursor = links.get("next")
            if not next_cursor:
                logger.info("Resource '%s': no next cursor. Pagination complete at page %d.", resource, page_number)
                break
            cursor = next_cursor

        logger.info(
            "Resource '%s': pagination finished. Total pages: %d, Total requests: %d.",
            resource,
            page_number,
            self._total_requests,
        )

    def get_all_records(
        self,
        resource: str,
        extra_params: Optional[Dict] = None,
        max_records: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all records for a resource, optionally capped at max_records."""
        all_records = []
        for page in self.paginate(resource, extra_params=extra_params):
            all_records.extend(page)
            if max_records and len(all_records) >= max_records:
                logger.info(
                    "Reached max_records cap (%d) for resource '%s'. Truncating.",
                    max_records,
                    resource,
                )
                return all_records[:max_records]
        return all_records
