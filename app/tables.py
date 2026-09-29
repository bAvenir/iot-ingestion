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
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from app.exceptions import MapperConfigError
from app.mappers.schema import Mapper

logger = logging.getLogger(__name__)

BRONZE_NAMESPACE = "bronze"
BRONZE_RAW = "bronze.raw"
DEAD_LETTER = "bronze.dead_letter"


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


DEAD_LETTER_SCHEMA = Schema(
    NestedField(1, "job_id", StringType(), required=True),
    NestedField(2, "trace_id", StringType(), required=True),
    NestedField(3, "stage", StringType(), required=True),
    # object_missing / sha256_mismatch / fetch_failed / append_failed
    NestedField(4, "reason", StringType(), required=True),
    NestedField(5, "detail", StringType(), required=False),
    NestedField(6, "failed_at", TimestamptzType(), required=True),
    NestedField(7, "received_at", TimestamptzType(), required=True),
    NestedField(8, "tenant_id", StringType(), required=True),
    NestedField(9, "device_id", StringType(), required=True),
    NestedField(10, "attempt", IntegerType(), required=True),
    NestedField(11, "s3_key", StringType(), required=False),
    NestedField(12, "schema_fp", StringType(), required=False),
    NestedField(13, "payload", StringType(), required=False),
)

DEAD_LETTER_SPEC = PartitionSpec(
    PartitionField(
        source_id=6,  # failed_at
        field_id=1000,
        transform=DayTransform(),
        name="failed_at_day",
    )
)


def ensure_dead_letter(catalog: Catalog) -> Table:
    """Create the bronze.dead_letter table if it doesn't exist. Idempotent."""
    catalog.create_namespace_if_not_exists(BRONZE_NAMESPACE)
    table = catalog.create_table_if_not_exists(
        identifier=DEAD_LETTER,
        schema=DEAD_LETTER_SCHEMA,
        partition_spec=DEAD_LETTER_SPEC,
        properties=METADATA_PROPERTIES,
    )
    logger.info("dead letter table ready: %s", DEAD_LETTER)
    return table


SILVER_NAMESPACE = "silver"

MAPPER_TYPES: dict[str, IcebergType] = {
    "double": DoubleType(),
    "int": LongType(),
    "string": StringType(),
    "bool": BooleanType(),
    "timestamp": TimestamptzType(),
}

# Same on every silver table, whatever the vendor. (device_id, event_time) is
# the dedup key, so both are required.
SILVER_ENVELOPE = [
    ("job_id", StringType(), True),
    ("trace_id", StringType(), True),
    ("tenant_id", StringType(), True),
    ("device_id", StringType(), True),
    ("event_time", TimestamptzType(), True),
    ("received_at", TimestamptzType(), True),
    ("mapper_id", StringType(), True),
    ("mapper_version", IntegerType(), True),
]


def silver_identifier(target_table: str) -> str:
    """silver_air_quality -> silver.air_quality"""
    return f"{SILVER_NAMESPACE}.{target_table.removeprefix('silver_')}"


def silver_schema(target_table: str, mappers: list[Mapper]) -> Schema:
    """Envelope columns plus every measurement column the mappers declare.

    Mappers sharing a table must agree on each column's type and unit, or two
    vendors would silently land incompatible values in one column.
    """
    measurements: dict[str, tuple[str, str, str]] = {}  # name -> (type, unit, mapper id)
    for mapper in mappers:
        if mapper.target_table != target_table:
            continue
        for col in mapper.columns:
            seen = measurements.get(col.name)
            if seen and seen[:2] != (col.type, col.unit):
                raise MapperConfigError(
                    f"{target_table}.{col.name}: {mapper.id} declares "
                    f"{col.type} {col.unit}, {seen[2]} declares {seen[0]} {seen[1]}"
                )
            measurements.setdefault(col.name, (col.type, col.unit, mapper.id))

    if not measurements:
        raise MapperConfigError(f"no mapper targets {target_table}")

    fields = [
        NestedField(i, name, typ, required=req)
        for i, (name, typ, req) in enumerate(SILVER_ENVELOPE, start=1)
    ]
    # Measurements are always nullable: a missing sensor is NULL, not a
    # rejected row. Per-column `required` is enforced by validation, not here.
    for name in sorted(measurements):
        fields.append(
            NestedField(len(fields) + 1, name, MAPPER_TYPES[measurements[name][0]], required=False)
        )
    return Schema(*fields)


def ensure_silver_tables(catalog: Catalog, mappers: list[Mapper]) -> dict[str, Table]:
    """Create or evolve one silver table per target_table. Idempotent.

    An existing table only ever gains columns (union_by_name), so field ids of
    data already written never change.
    """
    catalog.create_namespace_if_not_exists(SILVER_NAMESPACE)
    tables: dict[str, Table] = {}
    for target in sorted({m.target_table for m in mappers}):
        schema = silver_schema(target, mappers)
        identifier = silver_identifier(target)
        table = catalog.create_table_if_not_exists(
            identifier=identifier,
            schema=schema,
            partition_spec=PartitionSpec(
                PartitionField(
                    source_id=schema.find_field("event_time").field_id,
                    field_id=1000,
                    transform=DayTransform(),
                    name="event_time_day",
                )
            ),
            properties=METADATA_PROPERTIES,
        )
        with table.update_schema() as update:
            update.union_by_name(schema)
        logger.info("silver table ready: %s", identifier)
        tables[target] = table
    return tables
