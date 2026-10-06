"""M4 — Apply a mapper to one bronze row, producing one silver row.

The mapper is only data; this is the code that interprets it. Per column:
read the value at `from`, cast to `type`, `convert`, multiply by `scale`,
then check `min` / `max` / `required`. Any failure raises RowRejected with a
reason code the caller writes to dead_letter.
"""

import json
from datetime import UTC, datetime

from app.exceptions import RowRejected
from app.mappers.paths import MISSING, get_path
from app.mappers.schema import Column, Mapper

REASON_BAD_PAYLOAD = "bad_payload"
REASON_BAD_VALUE = "bad_value"
REASON_MISSING_VALUE = "missing_value"
REASON_OUT_OF_RANGE = "out_of_range"

CONVERSIONS = {
    "fahrenheit_to_celsius": lambda f: (f - 32) * 5 / 9,
}


def _to_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


CASTS = {
    "double": float,
    "int": int,
    "string": str,
    "bool": bool,
    "timestamp": _to_datetime,
}


def _column_value(payload: dict, col: Column) -> object:
    raw = get_path(payload, col.from_)

    if raw is MISSING or raw is None:
        if col.required:
            raise RowRejected(REASON_MISSING_VALUE, f"{col.name} <- {col.from_}")
        return None

    try:
        value = CASTS[col.type](raw)
    except (TypeError, ValueError) as e:
        raise RowRejected(
            REASON_BAD_VALUE, f"{col.name} <- {col.from_}={raw!r}: {e}"
        ) from e

    # convert before scale, as documented in mappers/README.md
    if col.convert:
        value = CONVERSIONS[col.convert](value)
    if col.scale != 1:
        value = value * col.scale

    if col.min is not None and value < col.min:
        raise RowRejected(REASON_OUT_OF_RANGE, f"{col.name}={value} < min {col.min}")
    if col.max is not None and value > col.max:
        raise RowRejected(REASON_OUT_OF_RANGE, f"{col.name}={value} > max {col.max}")
    return value


def apply_mapper(mapper: Mapper, row: dict) -> dict:
    """One bronze.raw row (as a dict) -> one silver row (as a dict)."""
    try:
        payload = json.loads(row["payload"])
    except json.JSONDecodeError as e:
        raise RowRejected(REASON_BAD_PAYLOAD, str(e)) from e
    if not isinstance(payload, dict):
        raise RowRejected(
            REASON_BAD_PAYLOAD, f"expected an object, got {type(payload).__name__}"
        )

    event_time = row["event_time"]
    if mapper.event_time_from:
        declared = get_path(payload, mapper.event_time_from)
        if declared is not MISSING and declared is not None:
            try:
                event_time = _to_datetime(declared)
            except TypeError, ValueError:
                pass  # keep bronze's value rather than reject a reading over it

    device_id = row["device_id"]
    if mapper.device_id_from:
        declared = get_path(payload, mapper.device_id_from)
        if declared is not MISSING and declared is not None:
            device_id = str(declared)

    silver_row = {
        "job_id": row["job_id"],
        "trace_id": row["trace_id"],
        "tenant_id": row["tenant_id"],
        "device_id": device_id,
        "event_time": event_time,
        "received_at": row["received_at"],
        "mapper_id": mapper.id,
        "mapper_version": mapper.version,
    }
    for col in mapper.columns:
        silver_row[col.name] = _column_value(payload, col)
    return silver_row
