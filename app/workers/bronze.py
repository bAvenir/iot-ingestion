"""M2 — Bronze worker.

XREADGROUP loop, batch to N messages or T seconds. Branch once on payload.mode
(inline = in hand, reference = fetch from landing/); identical after that.
Normalize the envelope (ts/timestamp/time -> event_time, fall back to
received_at), compute schema_fp, leave payload an unparsed JSON string.
One table.append() per batch, then XACK. Retry with attempt++, dead-letter past 3.
"""

import asyncio
import hashlib
import logging
import os
import socket
from time import monotonic

from pydantic import ValidationError

from app.config import get_settings
from app.models import JobDescriptor
from app.queue import GROUP_BRONZE, STREAM_BRONZE, create_client, create_group
from app.storage import S3Storage

s3_client = S3Storage()

logger = logging.getLogger(__name__)

MAX_BLOCK_MS = 4000


async def run(stop: asyncio.Event) -> None:
    settings = get_settings()
    client = create_client()

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

        for entry_id, descriptor in batch:
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

            sha256 = hashlib.sha256(raw).hexdigest()

            if sha256 != descriptor.payload.sha256:
                logger.warning("Sha256 does not match, skipping: %s", sha256)
                continue

            row = {
                "job_id": descriptor.job_id,
                "trace_id": descriptor.trace_id,
                # These idk
                # "received_at": datetime(
                #     2026, 9, 11, 8, 14, 22, tzinfo=UTC
                # ),  # from the descriptor
                # "event_time": datetime(2026, 9, 10, 8, 14, 22, tzinfo=UTC),
                "tenant_id": descriptor.tenant_id,
                "device_id": descriptor.device_id,
                "source": "http",  # idk,
                "pipeline_hint": descriptor.pipeline_hint,
                "schema_fp": "_",
                "payload": payload,
            }

            logger.info(row)

        if batch:
            logger.info(
                "batch of %d: %s",
                len(batch),
                [d.job_id for _, d in batch],
            )

    await client.aclose()
