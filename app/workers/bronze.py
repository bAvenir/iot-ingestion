"""M2 — Bronze worker.

XREADGROUP loop, batch to N messages or T seconds. Branch once on payload.mode
(inline = in hand, reference = fetch from landing/); identical after that.
Normalize the envelope (ts/timestamp/time -> event_time, fall back to
received_at), compute schema_fp, leave payload an unparsed JSON string.
One table.append() per batch, then XACK. Retry with attempt++, dead-letter past 3.
"""

import asyncio
import hashlib
import json
import logging
import os
import socket
from datetime import UTC, datetime
from time import monotonic

import pyarrow as pa
from botocore.exceptions import ClientError
from pydantic import ValidationError
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table
from valkey import ValkeyError
from valkey.asyncio import Valkey

from app.catalog import get_catalog
from app.config import get_settings
from app.exceptions import ObjectNotFound
from app.fingerprint import fingerprint
from app.models import JobDescriptor, SnapshotJob
from app.queue import (
    GROUP_BRONZE,
    STREAM_BRONZE,
    STREAM_SILVER,
    create_client,
    create_group,
    enqueue,
)
from app.storage import S3Storage
from app.tables import BRONZE_RAW, ensure_dead_letter

STREAM_MAXLEN = 100_000

logger = logging.getLogger(__name__)

MAX_BLOCK_MS = 4000


SOURCE_HTTP = "http"


EVENT_TIME_KEYS = ("ts", "timestamp", "time")

MAX_ATTEMPTS = 3

REASON_VALIDATION_ERROR = "unparseable_descriptor"
REASON_OBJECT_MISSING = "object_missing"
REASON_FETCH_FAILED = "fetch_failed"
REASON_SHA256_MISMATCH = "sha256_mismatch"
REASON_APPEND_FAILED = "append_failed"

CLAIM_MIN_IDLE_MS = 60_000


def extract_event_time(payload: dict, fallback: datetime) -> datetime:
    """Pull the producer's event time out of a parsed payload.

    Returns `fallback` (the API's received_at) when no known key is present or
    the value doesn't parse — bronze never rejects a reading over a timestamp.
    """
    for key in EVENT_TIME_KEYS:
        value = payload.get(key)
        if value is None:
            continue
        try:
            parsed = datetime.fromisoformat(str(value))
        except TypeError, ValueError:
            logger.warning("unparseable %s=%r, using received_at", key, value)
            continue
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return fallback


def to_bronze_row(descriptor: JobDescriptor, payload_text: str) -> dict:
    """One bronze.raw row. Identical for inline and reference payloads.

    `payload_text` is stored verbatim: it is parsed here only to read the event
    time, never re-serialised, so the stored bytes still match the descriptor's
    sha256.
    """
    try:
        parsed = json.loads(payload_text)
    except json.JSONDecodeError:
        parsed = {}

    if not isinstance(parsed, dict):
        parsed = {}
    return {
        "job_id": descriptor.job_id,
        "trace_id": descriptor.trace_id,
        "received_at": descriptor.received_at,
        "event_time": extract_event_time(parsed, descriptor.received_at),
        "tenant_id": descriptor.tenant_id,
        "device_id": descriptor.device_id,
        "source": SOURCE_HTTP,
        "pipeline_hint": descriptor.pipeline_hint,
        "schema_fp": fingerprint(parsed),
        "payload": payload_text,
    }


def to_dead_letter_row(
    descriptor: JobDescriptor,
    reason: str,
    detail: str | None = None,
    payload_text: str | None = None,
) -> dict:
    """One bronze.dead_letter row. payload_text is None when the payload is what
    failed; schema_fp follows it, since hashing needs something to hash."""
    return {
        "job_id": descriptor.job_id,
        "trace_id": descriptor.trace_id,
        "stage": descriptor.stage,
        "reason": reason,
        "detail": detail,
        "failed_at": datetime.now(UTC),
        "received_at": descriptor.received_at,
        "tenant_id": descriptor.tenant_id,
        "device_id": descriptor.device_id,
        "attempt": descriptor.attempt,
        "s3_key": getattr(descriptor.payload, "s3_key", None),
        "schema_fp": fingerprint(json.loads(payload_text)) if payload_text else None,
        "payload": payload_text,
    }


def append_rows(table: Table, rows: list[dict]) -> None:
    """One Iceberg commit for the whole batch.

    Synchronous and slow (writes Parquet, rewrites metadata, compare-and-swaps
    the catalog pointer), so callers run it off the event loop. The table is
    refreshed first: another writer may have moved the pointer since the last
    batch.
    """
    table.refresh()
    arrow = pa.Table.from_pylist(rows, schema=schema_to_pyarrow(table.schema()))
    table.append(arrow)


async def dead_letter(
    table: Table,
    descriptor: JobDescriptor,
    reason: str,
    detail: str | None = None,
    payload_text: str | None = None,
) -> None:
    """Record one failure. Per failure rather than batched — they should be rare,
    and a row you delay is a row you can lose."""
    row = to_dead_letter_row(descriptor, reason, detail, payload_text)
    await asyncio.to_thread(append_rows, table, [row])
    logger.warning(
        "dead-lettered %s: reason=%s attempt=%d",
        descriptor.job_id,
        reason,
        row["attempt"],
    )


async def parse_entries(
    client: Valkey,
    dl_table: Table,
    entries: list[tuple[str, dict]],
) -> list[tuple[str, JobDescriptor]]:

    batch: list[tuple[str, JobDescriptor]] = []

    for entry_id, fields in entries:
        try:
            descriptor = JobDescriptor.model_validate_json(fields.get("job", ""))
        except ValidationError as e:
            logger.warning("unparseable entry %s", entry_id)

            dl_row = {
                "job_id": entry_id,
                "trace_id": "",
                "stage": "bronze",
                "reason": REASON_VALIDATION_ERROR,
                "detail": f"descriptor failed validation: {e.error_count()} error(s), first: {e.errors()[0]['loc']} {e.errors()[0]['msg']}",
                "failed_at": datetime.now(UTC),
                "received_at": datetime.now(UTC),
                "tenant_id": "",
                "device_id": "",
                "attempt": 1,
                "s3_key": None,
                "schema_fp": None,
                "payload": fields.get("job"),
            }

            try:
                await asyncio.to_thread(append_rows, dl_table, [dl_row])
            except Exception:
                logger.exception("dead letter append failed for %d rows", 1)

            else:
                await client.xack(STREAM_BRONZE, GROUP_BRONZE, entry_id)

            continue
        batch.append((entry_id, descriptor))

    return batch


async def run(stop: asyncio.Event) -> None:
    settings = get_settings()
    client = create_client()
    s3_client = S3Storage()
    catalog = get_catalog()
    table = catalog.load_table(BRONZE_RAW)
    dl_table = ensure_dead_letter(catalog)

    await create_group(client, STREAM_BRONZE, GROUP_BRONZE)

    consumer = f"bronze-{socket.gethostname()}-{os.getpid()}"
    logger.info("bronze worker started as %s", consumer)

    while not stop.is_set():
        batch: list[tuple[str, JobDescriptor]] = []
        deadline = monotonic() + settings.BRONZE_BATCH_WINDOW_SECONDS

        _next, claimed, _deleted = await client.xautoclaim(
            STREAM_BRONZE,
            GROUP_BRONZE,
            consumer,
            min_idle_time=CLAIM_MIN_IDLE_MS,
            start_id="0-0",
            count=settings.BRONZE_BATCH_SIZE,
        )

        batch.extend(await parse_entries(client, dl_table, claimed))

        while len(batch) < settings.BRONZE_BATCH_SIZE and monotonic() < deadline:
            response = await client.xreadgroup(
                GROUP_BRONZE,
                consumer,
                {STREAM_BRONZE: ">"},
                count=settings.BRONZE_BATCH_SIZE - len(batch),
                block=max(1, min(MAX_BLOCK_MS, int((deadline - monotonic()) * 1000))),
            )

            if not response:
                continue

            for _stream, entries in response:
                batch.extend(await parse_entries(client, dl_table, entries))

        rows: list[tuple[str, dict]] = []
        descriptors = dict(batch)
        for entry_id, descriptor in batch:
            try:
                if descriptor.payload.mode == "reference":
                    raw = await asyncio.to_thread(
                        s3_client.download_file,
                        settings.S3_BUCKET_LANDING,
                        descriptor.payload.s3_key,
                    )
                    payload = raw.decode("utf-8")
                else:
                    payload = descriptor.payload.data
                    raw = descriptor.payload.data.encode()
            except ObjectNotFound as e:
                # nothing will put the object back
                await dead_letter(
                    dl_table, descriptor, REASON_OBJECT_MISSING, detail=str(e)
                )
                await client.xack(STREAM_BRONZE, GROUP_BRONZE, entry_id)
                continue
            except ClientError as e:
                logger.exception("fetch failed for %s", descriptor.job_id)
                retried = descriptor.model_copy(
                    update={"attempt": descriptor.attempt + 1}
                )
                if retried.attempt <= MAX_ATTEMPTS:
                    await enqueue(client, STREAM_BRONZE, retried)
                else:
                    await dead_letter(
                        dl_table, descriptor, REASON_FETCH_FAILED, detail=str(e)
                    )
                await client.xack(STREAM_BRONZE, GROUP_BRONZE, entry_id)
                continue

            sha256 = hashlib.sha256(raw).hexdigest()
            if sha256 != descriptor.payload.sha256:
                # stored bytes are not what was received; keep them as evidence
                await dead_letter(
                    dl_table,
                    descriptor,
                    REASON_SHA256_MISMATCH,
                    detail=f"got {sha256}, expected {descriptor.payload.sha256}",
                    payload_text=payload,
                )
                await client.xack(STREAM_BRONZE, GROUP_BRONZE, entry_id)
                continue

            rows.append((entry_id, to_bronze_row(descriptor, payload)))

        if rows:
            try:
                await asyncio.to_thread(append_rows, table, [r for _, r in rows])
            except Exception as e:
                logger.exception("bronze append failed for %d rows", len(rows))
                for entry_id, row in rows:
                    descriptor = descriptors[entry_id]
                    retried = descriptor.model_copy(
                        update={"attempt": descriptor.attempt + 1}
                    )
                    if retried.attempt <= MAX_ATTEMPTS:
                        await enqueue(client, STREAM_BRONZE, retried)
                    else:
                        await dead_letter(
                            dl_table,
                            descriptor,
                            REASON_APPEND_FAILED,
                            detail=str(e),
                            payload_text=row["payload"],
                        )
                    await client.xack(STREAM_BRONZE, GROUP_BRONZE, entry_id)
                continue

            snapshot_id = table.current_snapshot().snapshot_id
            job = SnapshotJob(snapshot_id=snapshot_id, table=BRONZE_RAW)

            try:
                await client.xadd(
                    STREAM_SILVER,
                    {"job": job.model_dump_json()},
                    maxlen=STREAM_MAXLEN,
                    approximate=True,
                )
            except ValkeyError:
                logger.exception("could not notify silver of snapshot %s", snapshot_id)
                continue

            await client.xack(STREAM_BRONZE, GROUP_BRONZE, *[e for e, _ in rows])

            logger.info(
                "committed %d rows (%d entries read): %s",
                len(rows),
                len(batch),
                [r["job_id"] for _, r in rows],
            )

    await client.aclose()
