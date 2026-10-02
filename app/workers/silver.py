"""M4 — Silver worker.

Each message on tasks:silver points at one bronze commit (a snapshot id). The
worker reads exactly the rows that commit added, resolves each to a mapper,
transforms and validates it, and appends the result to the matching silver table.

One message = one bronze commit = up to BRONZE_BATCH_SIZE rows, so there is no
N-or-T batching here: bronze already did it.
"""

import asyncio
import logging
import os
import socket
from collections import defaultdict
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table

from app.catalog import get_catalog
from app.exceptions import RowRejected
from app.mappers.registry import get_registry
from app.mappers.resolve import resolve
from app.mappers.transform import apply_mapper
from app.models import SnapshotJob
from app.queue import (
    GROUP_SILVER,
    STREAM_SILVER,
    create_client,
    create_group,
)
from app.tables import BRONZE_RAW, ensure_dead_letter, ensure_silver_tables

logger = logging.getLogger(__name__)

CLAIM_MIN_IDLE_MS = 60_000
# Must stay under Valkey.from_url's 5s socket timeout (same as bronze).
MAX_BLOCK_MS = 4000

# No hint, no cached fingerprint, no mapper whose required_keys all match.
REASON_UNRESOLVED = "unresolved"


def existing_keys(table: Table, rows: list[dict]) -> set[tuple]:
    """(device_id, event_time) pairs already in the table for the batch's time range.

    A duplicate has the same event_time, so nothing outside [lo, hi] can match.
    """
    if not rows:
        return set()

    table.refresh()

    times = [row["event_time"] for row in rows]
    lo = min(times)
    hi = max(times)

    row_filter = And(
        GreaterThanOrEqual("event_time", lo.isoformat()),
        LessThanOrEqual("event_time", hi.isoformat()),
    )

    existing = (
        table.scan(row_filter=row_filter, selected_fields=("device_id", "event_time"))
        .to_arrow()
        .to_pylist()
    )
    return {(row["device_id"], row["event_time"]) for row in existing}


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


def to_dead_letter_row_silver(
    row: dict, reason: str, detail: str | None = None
) -> dict:
    """One bronze.dead_letter row from a bronze row that silver couldn't use.

    The bronze row already has the payload inline and its fingerprint computed,
    so unlike bronze's version there is nothing to fetch or hash here.
    """
    return {
        "job_id": row["job_id"],
        "trace_id": row["trace_id"],
        "stage": "silver",
        "reason": reason,
        "detail": detail,
        "failed_at": datetime.now(UTC),
        "received_at": row["received_at"],
        "tenant_id": row["tenant_id"],
        "device_id": row["device_id"],
        "attempt": 1,
        "s3_key": None,  # bronze doesn't keep it; the payload is already inline
        "schema_fp": row["schema_fp"],
        "payload": row["payload"],
    }


def rows_added_by(table: Table, snapshot_id: int) -> pa.Table | None:
    """The rows one snapshot appended, not the whole table.

    Returns None if the snapshot no longer exists (expired by maintenance).
    """
    snap = table.snapshot_by_id(snapshot_id)
    if snap is None:
        return None

    paths = [
        entry.data_file.file_path
        for manifest in snap.manifests(table.io)
        if manifest.added_snapshot_id == snapshot_id
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True)
        if entry.snapshot_id == snapshot_id
    ]
    if not paths:
        return pa.table({})

    return pa.concat_tables(pq.read_table(table.io.new_input(p).open()) for p in paths)


async def run(stop: asyncio.Event) -> None:
    client = create_client()
    catalog = get_catalog()
    bronze = catalog.load_table(BRONZE_RAW)
    dl_table = ensure_dead_letter(catalog)

    registry = get_registry()
    silver = ensure_silver_tables(catalog, list(registry.values()))

    await create_group(client, STREAM_SILVER, GROUP_SILVER)

    consumer = f"silver-{socket.gethostname()}-{os.getpid()}"
    logger.info("silver worker started as %s", consumer)

    while not stop.is_set():
        by_table: dict[str, list[dict]] = defaultdict(list)
        _next, claimed, _deleted = await client.xautoclaim(
            STREAM_SILVER,
            GROUP_SILVER,
            consumer,
            min_idle_time=CLAIM_MIN_IDLE_MS,
            start_id="0-0",
            count=1,
        )
        if claimed:
            entries = claimed
        else:
            response = await client.xreadgroup(
                GROUP_SILVER,
                consumer,
                {STREAM_SILVER: ">"},
                count=1,
                block=MAX_BLOCK_MS,
            )
            if not response:
                continue
            entries = response[0][1]

        entry_id, fields = entries[0]

        try:
            job = SnapshotJob.model_validate_json(fields.get("job", ""))
        except ValidationError:
            logger.error("unparseable silver job %s, dropping", entry_id)
            await client.xack(STREAM_SILVER, GROUP_SILVER, entry_id)
            continue

        bronze.refresh()
        rows = await asyncio.to_thread(rows_added_by, bronze, job.snapshot_id)
        if rows is None:
            logger.error("snapshot %s no longer exists, dropping", job.snapshot_id)
            await client.xack(STREAM_SILVER, GROUP_SILVER, entry_id)
            continue

        logger.info("snapshot %s: %d rows", job.snapshot_id, rows.num_rows)

        dl_rows: list[dict] = []
        for row in rows.to_pylist():
            mapper = resolve(row, registry)

            if mapper is None:
                dl_rows.append(
                    to_dead_letter_row_silver(
                        row,
                        REASON_UNRESOLVED,
                        f"no mapper matched: hint={row['pipeline_hint']!r}, "
                        f"schema_fp={row['schema_fp']}",
                    )
                )
                continue

            try:
                silver_row = apply_mapper(mapper, row)
            except RowRejected as e:
                dl_rows.append(to_dead_letter_row_silver(row, e.reason, e.detail))
                continue

            by_table[mapper.target_table].append(silver_row)

        # Check duplicates in current batch
        deduplicated: dict[str, list[dict]] = defaultdict(list)

        for target, target_rows in by_table.items():
            seen = await asyncio.to_thread(
                existing_keys, silver[target], target_rows
            )
            for row in target_rows:
                device_id = row["device_id"]
                event_time = row["event_time"]

                key = (device_id, event_time)

                if key in seen:
                    continue

                seen.add(key)
                deduplicated[target].append(row)

        try:
            if dl_rows:
                await asyncio.to_thread(append_rows, dl_table, dl_rows)

            for target, target_rows in deduplicated.items():
                await asyncio.to_thread(append_rows, silver[target], target_rows)

        except Exception:
            logger.exception(
                "silver commit failed for snapshot %s, left pending", job.snapshot_id
            )

            continue

        await client.xack(STREAM_SILVER, GROUP_SILVER, entry_id)
        logger.info(
            "snapshot %s: wrote %s, %d duplicates skipped, %d dead-lettered",
            job.snapshot_id,
            {target: len(r) for target, r in deduplicated.items()},
            sum(len(r) for r in by_table.values())
            - sum(len(r) for r in deduplicated.values()),
            len(dl_rows),
        )

    await client.aclose()
