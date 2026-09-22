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

from app.catalog import get_catalog
from app.config import get_settings
from app.exceptions import ObjectNotFound
from app.fingerprint import fingerprint
from app.models import JobDescriptor
from app.queue import GROUP_BRONZE, STREAM_BRONZE, create_client, create_group
from app.storage import S3Storage
from app.tables import BRONZE_RAW

logger = logging.getLogger(__name__)

MAX_BLOCK_MS = 4000

# Only adapter for now; mqtt / direct-to-valkey are backlog items.
SOURCE_HTTP = "http"

# Producers spell the event time differently. First match wins, so the order is
# the preference order.
EVENT_TIME_KEYS = ("ts", "timestamp", "time")


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
        except (TypeError, ValueError):
            logger.warning("unparseable %s=%r, using received_at", key, value)
            continue
        # A producer may omit the offset; assume UTC rather than dropping the value.
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
        # Bronze accepts shapes it cannot interpret — that is the layer's point.
        # Unparseable JSON still lands, with no event time and no fingerprint.
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


async def run(stop: asyncio.Event) -> None:
    settings = get_settings()
    client = create_client()
    s3_client = S3Storage()
    table = get_catalog().load_table(BRONZE_RAW)

    await create_group(client, STREAM_BRONZE, GROUP_BRONZE)

    consumer = f"bronze-{socket.gethostname()}-{os.getpid()}"
    logger.info("bronze worker started as %s", consumer)

    while not stop.is_set():
        batch: list[tuple[str, JobDescriptor]] = []
        deadline = monotonic() + settings.BRONZE_BATCH_WINDOW_SECONDS

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
                for entry_id, fields in entries:
                    try:
                        descriptor = JobDescriptor.model_validate_json(fields["job"])
                    except ValidationError:
                        logger.warning("unparseable entry %s, skipping", entry_id)
                        continue
                    batch.append((entry_id, descriptor))

        rows: list[tuple[str, dict]] = []
        for entry_id, descriptor in batch:
            try:
                if descriptor.payload.mode == "reference":
                    # boto3 is synchronous: to_thread takes the function and its
                    # args, so the blocking call runs off the event loop.
                    raw = await asyncio.to_thread(
                        s3_client.download_file,
                        settings.S3_BUCKET_LANDING,
                        descriptor.payload.s3_key,
                    )
                    payload = raw.decode("utf-8")
                else:
                    payload = descriptor.payload.data
                    raw = descriptor.payload.data.encode()
            except ObjectNotFound:
                # Permanent: retrying cannot conjure the object back. Left
                # unacked so the dead-letter task can claim it from the PEL.
                logger.error("payload object missing for %s", descriptor.job_id)
                continue
            except ClientError:
                # Transient (network, 5xx). Also left unacked, so it is
                # redelivered by XAUTOCLAIM rather than lost.
                logger.exception("fetch failed for %s", descriptor.job_id)
                continue

            sha256 = hashlib.sha256(raw).hexdigest()
            if sha256 != descriptor.payload.sha256:
                logger.warning(
                    "sha256 mismatch for %s: got %s, expected %s",
                    descriptor.job_id,
                    sha256,
                    descriptor.payload.sha256,
                )
                continue

            rows.append((entry_id, to_bronze_row(descriptor, payload)))

        if rows:
            try:
                await asyncio.to_thread(append_rows, table, [r for _, r in rows])
            except Exception:
                # Nothing is acked, so every entry stays claimable. Redelivery
                # may duplicate rows if the commit actually landed before the
                # failure — at-least-once, deduped in silver on (device, event).
                logger.exception("bronze append failed, %d entries left pending", len(rows))
                continue

            # XACK only after the commit: a crash in between means redelivery,
            # while acking first would lose the readings outright.
            await client.xack(STREAM_BRONZE, GROUP_BRONZE, *[e for e, _ in rows])
            logger.info(
                "committed %d rows (%d entries read): %s",
                len(rows),
                len(batch),
                [r["job_id"] for _, r in rows],
            )

    await client.aclose()
