"""M1/M2 — Valkey Streams.

Streams per stage: tasks:bronze, tasks:silver, tasks:publish.
XADD with MAXLEN trimming, idempotent XGROUP CREATE ... $ MKSTREAM on startup,
XREADGROUP batching, XACK only after the Iceberg commit, XAUTOCLAIM to recover
messages from a crashed worker.
"""

import logging

from valkey import ResponseError
from valkey.asyncio import Valkey

from app.config import get_settings
from app.models import JobDescriptor

logger = logging.getLogger(__name__)

STREAM_BRONZE = "tasks:bronze"
STREAM_SILVER = "tasks:silver"
STREAM_PUBLISH = "tasks:publish"

# Consumer group on tasks:bronze. Created in the lifespan, joined by the worker.
GROUP_BRONZE = "bronze-workers"
STREAM_MAXLEN = 100_000


def create_client() -> Valkey:
    """One client per process; the pool is bound to the running event loop."""
    return Valkey.from_url(get_settings().VALKEY_URL, decode_responses=True)


async def enqueue(client: Valkey, stream: str, descriptor: JobDescriptor) -> str:
    """XADD one descriptor. Returns the stream entry id.

    XADD takes a flat str->str map, so the whole descriptor goes in as a single
    JSON field rather than being spread across stream fields — it stays
    versionable as one blob and XRANGE output stays readable.
    """
    entry_id = await client.xadd(
        stream,
        {"job": descriptor.model_dump_json()},
        maxlen=STREAM_MAXLEN,
        approximate=True,
    )
    logger.info(
        "enqueued stream=%s entry=%s job_id=%s", stream, entry_id, descriptor.job_id
    )
    return entry_id


async def create_group(valkey_client, STREAM: str, GROUP: str):

    try:
        await valkey_client.xgroup_create(STREAM, GROUP, id="0", mkstream=True)

    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
