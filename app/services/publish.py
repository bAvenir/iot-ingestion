"""Export a slice of one silver table as JSON into the published bucket.

No FastAPI in here: plain arguments in, a dict out, and errors from
app.exceptions. The endpoint in app/api/publish.py turns those into status codes.
"""

import json
import logging
import re
from datetime import datetime
from functools import lru_cache

from botocore.exceptions import BotoCoreError, ClientError
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
    GreaterThanOrEqual,
    LessThan,
)
from ulid import ULID

from app.catalog import get_catalog
from app.config import get_settings
from app.exceptions import InvalidRequest, StorageUnavailable, UnknownTable
from app.storage import S3Storage
from app.tables import silver_identifier

logger = logging.getLogger(__name__)


@lru_cache
def _storage() -> S3Storage:
    return S3Storage()


def tenant_safety_check(tenant_id: str) -> bool:
    tmp = re.fullmatch(r"[A-Za-z0-9_-]+", tenant_id)

    if tmp is None:
        return False
    return True


def build_filter(
    tenant_id: str,
    device_id: str | None,
    start: datetime | None,
    end: datetime | None,
) -> BooleanExpression:
    """Tenant always; device and each end of the time range only when given."""
    row_filter = EqualTo("tenant_id", tenant_id)
    if device_id:
        row_filter = And(row_filter, EqualTo("device_id", device_id))
    if start:
        row_filter = And(row_filter, GreaterThanOrEqual("event_time", start.isoformat()))
    if end:
        row_filter = And(row_filter, LessThan("event_time", end.isoformat()))
    return row_filter


def export_key(tenant_id: str, table: str, export_id: str) -> str:
    return f"{tenant_id}/{table}/{export_id}.json"


def to_json(rows: list[dict]) -> bytes:
    return json.dumps(rows, default=str).encode()


def publish_slice(
    table: str,
    tenant_id: str,
    device_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 10,
) -> dict:
    """Read the matching rows, write them to published/, say where they went.

    Blocking (catalog, S3), so async callers run it in a thread.
    """
    if not tenant_safety_check(tenant_id):
        raise InvalidRequest("X-Tenant-Id")

    try:
        silver_table = get_catalog().load_table(silver_identifier(table))
    except NoSuchTableError:
        raise UnknownTable(f"unknown table: {table}") from None

    if start and end and start >= end:
        raise InvalidRequest("Start date must be before end")

    row_filter = build_filter(tenant_id, device_id, start, end)
    rows = silver_table.scan(row_filter=row_filter, limit=limit).to_arrow().to_pylist()

    key = export_key(tenant_id, table, str(ULID()))
    try:
        _storage().upload_file(get_settings().S3_BUCKET_PUBLISHED, key, to_json(rows))
    except (ClientError, BotoCoreError):
        logger.exception("export upload failed: %s", key)
        raise StorageUnavailable("storage unavailable") from None

    return {
        "table": table,
        "tenant_id": tenant_id,
        "device_id": device_id,
        "from": start,
        "to": end,
        "limit": limit,
        "count": len(rows),
        "key": key,
        "rows": rows,
    }
