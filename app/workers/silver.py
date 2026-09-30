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
from app.tables import BRONZE_RAW, ensure_dead_letter

logger = logging.getLogger(__name__)

CLAIM_MIN_IDLE_MS = 60_000
# Must stay under Valkey.from_url's 5s socket timeout (same as bronze).
MAX_BLOCK_MS = 4000

# No hint, no cached fingerprint, no mapper whose required_keys all match.
REASON_UNRESOLVED = "unresolved"


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
    dl_table = ensure_dead_letter(catalog)  # noqa: F841 — used once transforms land

    await create_group(client, STREAM_SILVER, GROUP_SILVER)

    consumer = f"silver-{socket.gethostname()}-{os.getpid()}"
    logger.info("silver worker started as %s", consumer)

    by_table: dict[str, list[dict]] = defaultdict(list)

    while not stop.is_set():
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
            registry = get_registry()
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

        # 4. Done with this message. Later this moves after the silver appends.
        await client.xack(STREAM_SILVER, GROUP_SILVER, entry_id)

    await client.aclose()
