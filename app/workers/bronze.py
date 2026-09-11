"""M2 — Bronze worker.

XREADGROUP loop, batch to N messages or T seconds. Branch once on payload.mode
(inline = in hand, reference = fetch from landing/); identical after that.
Normalise the envelope (ts/timestamp/time -> event_time, fall back to
received_at), compute schema_fp, leave payload an unparsed JSON string.
One table.append() per batch, then XACK. Retry with attempt++, dead-letter past 3.
"""

import asyncio
import logging
import os
import socket
from time import monotonic

from pydantic import ValidationError

from app.config import get_settings
from app.models import JobDescriptor
from app.queue import GROUP_BRONZE, STREAM_BRONZE, create_client

logger = logging.getLogger(__name__)

# Valkey.from_url defaults socket_timeout=5s, so a longer XREADGROUP block would
# trip the socket read timeout before the command returns. Cap each block below
# that and let the outer loop re-issue until the batch deadline is reached.
MAX_BLOCK_MS = 4000


async def run(stop: asyncio.Event) -> None:
    settings = get_settings()
    client = create_client()

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

        if batch:
            logger.info(
                "batch of %d: %s",
                len(batch),
                [d.job_id for _, d in batch],
            )

    await client.aclose()
