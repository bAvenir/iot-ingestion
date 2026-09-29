"""M4 — Silver worker.

Scan only the bronze snapshot range newer than the last processed snapshot id,
hand Arrow to DuckDB, json_extract per mapper column, cast + scale/convert,
range/null validation (failures -> dead_letter with a reason), anti-join dedup on
(device_id, event_time) against the recent partition, append with mapper_id +
mapper_version for lineage.

Open: where the snapshot cursor lives (Postgres row vs small S3 object) — the one
piece of state not held by Iceberg.
"""

import asyncio
import logging
import os
import socket

from app.catalog import get_catalog
from app.config import get_settings
from app.queue import (
    GROUP_BRONZE,
    GROUP_SILVER,
    STREAM_SILVER,
    create_client,
)
from app.storage import S3Storage
from app.tables import ensure_dead_letter

logger = logging.getLogger(__name__)
CLAIM_MIN_IDLE_MS = 60_000
MAX_BLOCK_MS = 4000


async def run(stop: asyncio.Event) -> None:
    settings = get_settings()
    client = create_client()
    s3_client = S3Storage()
    catalog = get_catalog()

    dl_table = ensure_dead_letter(catalog)

    consumer = f"silver-{socket.gethostname()}-{os.getpid()}"
    logger.info("silver worker started as %s", consumer)

    while not stop.is_set():
        _next, claimed, _deleted = await client.xautoclaim(
            STREAM_SILVER,
            GROUP_BRONZE,
            consumer,
            min_idle_time=CLAIM_MIN_IDLE_MS,
            start_id="0-0",
            count=settings.BRONZE_BATCH_SIZE,
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
