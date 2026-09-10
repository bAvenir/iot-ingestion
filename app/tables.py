"""M2/M4 — Iceberg table definitions and create-if-missing.

bronze_raw: job_id, trace_id, received_at, event_time, tenant_id, device_id,
source, pipeline_hint, schema_fp, payload (string). Partition day(received_at).
dead_letter: job_id, received_at, reason, schema_fp, s3_key, payload.
silver_*: schema DERIVED from a mapper's columns, never hardcoded.

Set at creation on every table (defaults are wrong for streaming writes):
  write.metadata.delete-after-commit.enabled = true
  write.metadata.previous-versions-max = 100
"""

import logging

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import NestedField, StringType, TimestamptzType

logger = logging.getLogger(__name__)

BRONZE_NAMESPACE = "bronze"
BRONZE_RAW = "bronze.raw"


METADATA_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


BRONZE_RAW_SCHEMA = Schema(
    NestedField(1, "job_id", StringType(), required=True),
    NestedField(2, "trace_id", StringType(), required=True),
    NestedField(3, "received_at", TimestamptzType(), required=True),
    # Optional: a producer may not send an event time. Bronze falls back to
    # received_at, but the column stays nullable.
    NestedField(4, "event_time", TimestamptzType(), required=False),
    NestedField(5, "tenant_id", StringType(), required=True),
    NestedField(6, "device_id", StringType(), required=True),
    NestedField(7, "source", StringType(), required=True),
    # Optional: absent whenever a producer doesn't declare itself — the normal case.
    NestedField(8, "pipeline_hint", StringType(), required=False),
    NestedField(9, "schema_fp", StringType(), required=True),
    NestedField(10, "payload", StringType(), required=True),
)


BRONZE_RAW_SPEC = PartitionSpec(
    PartitionField(
        source_id=3,
        field_id=1000,
        transform=DayTransform(),
        name="received_at_day",
    )
)


def ensure_bronze_raw(catalog: Catalog) -> Table:
    """Create the bronze.raw table if it doesn't exist. Idempotent."""
    catalog.create_namespace_if_not_exists(BRONZE_NAMESPACE)
    table = catalog.create_table_if_not_exists(
        identifier=BRONZE_RAW,
        schema=BRONZE_RAW_SCHEMA,
        partition_spec=BRONZE_RAW_SPEC,
        properties=METADATA_PROPERTIES,
    )
    logger.info("bronze table ready: %s", BRONZE_RAW)
    return table
