"""
Resource Extractors
===================
Per-resource extractor classes that transform raw API responses into
flat records suitable for BigQuery ingestion.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ingestion.marketing.api import APIClient
from ingestion.marketing.config import (
    CHECKPOINT_EVERY,
    RESOURCES,
    get_batch_size,
)

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class for resource extractors.

    Each subclass implements transform logic specific to its resource type.
    The base class handles pagination, checkpointing, and batch assembly.
    """

    resource: str = ""

    def __init__(self, client: APIClient):
        self.client = client
        self.config = RESOURCES[self.resource]
        self._record_count = 0

    @abstractmethod
    def transform(self, raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform a single raw API record into a flat dict. Return None to skip."""
        ...

    def extract(
        self,
        extra_params: Optional[Dict] = None,
        max_records: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract all records for this resource, applying transforms and checkpointing.

        Yields batches of transformed records, each batch sized according to config.
        """
        batch_size = get_batch_size(self.resource)
        all_records = []
        batch = []

        for page in self.client.paginate(self.resource, extra_params=extra_params):
            for raw in page:
                transformed = self.transform(raw)
                if transformed is None:
                    continue

                batch.append(transformed)
                self._record_count += 1

                if len(batch) >= batch_size:
                    all_records.extend(batch)
                    logger.info(
                        "Resource '%s': checkpoint at %d records.",
                        self.resource,
                        self._record_count,
                    )
                    batch = []

                if max_records and self._record_count >= max_records:
                    all_records.extend(batch)
                    logger.info(
                        "Resource '%s': reached max_records cap (%d).",
                        self.resource,
                        max_records,
                    )
                    return all_records

            if self._record_count > 0 and self._record_count % CHECKPOINT_EVERY == 0:
                logger.info(
                    "Resource '%s': checkpoint at %d total records.",
                    self.resource,
                    self._record_count,
                )

        # Flush remaining records in the batch
        if batch:
            all_records.extend(batch)

        logger.info(
            "Resource '%s': extraction complete. Total transformed records: %d.",
            self.resource,
            self._record_count,
        )
        return all_records

    @property
    def record_count(self) -> int:
        return self._record_count


class ProfileExtractor(BaseExtractor):
    """Extractor for profile records."""

    resource = "profiles"

    def transform(self, raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        attributes = raw_record.get("attributes", {})
        if not attributes:
            return None

        profile_id = raw_record.get("id", "")
        links = raw_record.get("links", {})
        self_key = links.get("self", "")

        return {
            "profile_id": profile_id,
            "email": attributes.get("email", ""),
            "phone_number": attributes.get("phone_number", ""),
            "first_name": attributes.get("first_name", ""),
            "last_name": attributes.get("last_name", ""),
            "organization": attributes.get("organization", ""),
            "title": attributes.get("title", ""),
            "created": attributes.get("created", ""),
            "updated": attributes.get("updated", ""),
            "last_event_date": attributes.get("last_event_date", ""),
            "location": self._flatten_location(attributes.get("location", {})),
            "properties": self._serialize_json(attributes.get("properties", {})),
            "subscriptions": self._serialize_json(attributes.get("subscriptions", {})),
            "self_link": self_key,
        }

    @staticmethod
    def _flatten_location(location: Dict) -> str:
        if not location:
            return ""
        parts = [
            location.get("address1", ""),
            location.get("address2", ""),
            location.get("city", ""),
            location.get("region", ""),
            location.get("country", ""),
            location.get("zip", ""),
        ]
        return ", ".join(p for p in parts if p)

    @staticmethod
    def _serialize_json(obj: Any) -> str:
        if not obj:
            return "{}"
        import json
        return json.dumps(obj, default=str)


class EventExtractor(BaseExtractor):
    """Extractor for event records."""

    resource = "events"

    def transform(self, raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        attributes = raw_record.get("attributes", {})
        if not attributes:
            return None

        event_id = raw_record.get("id", "")
        event_properties = attributes.get("event_properties", {})
        person = attributes.get("profile", {})
        person_data = person.get("data", {}) if isinstance(person, dict) else {}
        person_attributes = person_data.get("attributes", {}) if isinstance(person_data, dict) else {}

        return {
            "event_id": event_id,
            "event_name": attributes.get("name", ""),
            "datetime": attributes.get("datetime", ""),
            "profile_id": person_data.get("id", ""),
            "email": person_attributes.get("email", ""),
            "event_properties": self._serialize_json(event_properties),
            "metric_id": attributes.get("metric", {}).get("data", {}).get("id", "") if isinstance(attributes.get("metric"), dict) else "",
            "metric_name": attributes.get("metric", {}).get("data", {}).get("attributes", {}).get("name", "") if isinstance(attributes.get("metric"), dict) else "",
        }

    @staticmethod
    def _serialize_json(obj: Any) -> str:
        if not obj:
            return "{}"
        import json
        return json.dumps(obj, default=str)


class CampaignExtractor(BaseExtractor):
    """Extractor for campaign records."""

    resource = "campaigns"

    def transform(self, raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        attributes = raw_record.get("attributes", {})
        if not attributes:
            return None

        campaign_id = raw_record.get("id", "")
        messages = attributes.get("messages", {})

        return {
            "campaign_id": campaign_id,
            "name": attributes.get("name", ""),
            "status": attributes.get("status", ""),
            "channel": messages.get("channel", "") if isinstance(messages, dict) else "",
            "subject": messages.get("subject", "") if isinstance(messages, dict) else "",
            "created_at": attributes.get("created_at", ""),
            "updated_at": attributes.get("updated_at", ""),
            "send_time": attributes.get("send_time", ""),
            "archived": attributes.get("archived", False),
            "tags": self._serialize_json(attributes.get("tags", [])),
        }

    @staticmethod
    def _serialize_json(obj: Any) -> str:
        if not obj:
            return "[]"
        import json
        return json.dumps(obj, default=str)


class FlowExtractor(BaseExtractor):
    """Extractor for flow records."""

    resource = "flows"

    def transform(self, raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        attributes = raw_record.get("attributes", {})
        if not attributes:
            return None

        flow_id = raw_record.get("id", "")

        return {
            "flow_id": flow_id,
            "name": attributes.get("name", ""),
            "status": attributes.get("status", ""),
            "created": attributes.get("created", ""),
            "updated": attributes.get("updated", ""),
            "archived": attributes.get("archived", False),
            "trigger_type": attributes.get("trigger", {}).get("trigger_type", "") if isinstance(attributes.get("trigger"), dict) else "",
            "tags": self._serialize_json(attributes.get("tags", [])),
        }

    @staticmethod
    def _serialize_json(obj: Any) -> str:
        if not obj:
            return "[]"
        import json
        return json.dumps(obj, default=str)


class ListExtractor(BaseExtractor):
    """Extractor for list records."""

    resource = "lists"

    def transform(self, raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        attributes = raw_record.get("attributes", {})
        if not attributes:
            return None

        list_id = raw_record.get("id", "")

        return {
            "list_id": list_id,
            "name": attributes.get("name", ""),
            "created": attributes.get("created", ""),
            "updated": attributes.get("updated", ""),
            "member_count": attributes.get("member_count", 0),
            "opt_in_process": attributes.get("opt_in_process", ""),
        }

    def extract(self, extra_params=None, max_records=None):
        """Lists do not support incremental filtering; always full extract."""
        return super().extract(extra_params=extra_params, max_records=max_records)


class SegmentExtractor(BaseExtractor):
    """Extractor for segment records."""

    resource = "segments"

    def transform(self, raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        attributes = raw_record.get("attributes", {})
        if not attributes:
            return None

        segment_id = raw_record.get("id", "")

        return {
            "segment_id": segment_id,
            "name": attributes.get("name", ""),
            "created": attributes.get("created", ""),
            "updated": attributes.get("updated", ""),
            "member_count": attributes.get("member_count", 0),
        }


class MetricExtractor(BaseExtractor):
    """Extractor for metric records."""

    resource = "metrics"

    def transform(self, raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        attributes = raw_record.get("attributes", {})
        if not attributes:
            return None

        metric_id = raw_record.get("id", "")

        return {
            "metric_id": metric_id,
            "name": attributes.get("name", ""),
            "created": attributes.get("created", ""),
            "updated": attributes.get("updated", ""),
            "integration": self._serialize_json(attributes.get("integration", {})),
        }

    @staticmethod
    def _serialize_json(obj: Any) -> str:
        if not obj:
            return "{}"
        import json
        return json.dumps(obj, default=str)


# Registry mapping resource names to extractor classes
EXTRACTOR_REGISTRY = {
    "profiles": ProfileExtractor,
    "events": EventExtractor,
    "campaigns": CampaignExtractor,
    "flows": FlowExtractor,
    "lists": ListExtractor,
    "segments": SegmentExtractor,
    "metrics": MetricExtractor,
}


def get_extractor(resource: str, client: APIClient) -> BaseExtractor:
    """Return the appropriate extractor instance for a given resource."""
    extractor_class = EXTRACTOR_REGISTRY.get(resource)
    if not extractor_class:
        raise ValueError(f"No extractor registered for resource '{resource}'.")
    return extractor_class(client)
