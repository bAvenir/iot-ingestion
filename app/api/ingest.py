import hashlib
import logging
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Request, status
from ulid import ULID
from valkey import ValkeyError

from app.config import get_settings
from app.models import InlinePayload, JobDescriptor, ReferencePayload
from app.queue import STREAM_BRONZE, enqueue
from app.storage import S3Storage

s3_client = S3Storage()

ingest_router = APIRouter(tags=["ingest"])
logger = logging.getLogger(__name__)

MAX_REQUEST_BYTES = 1024 * 1024


@ingest_router.post(
    "/ingest",
    status_code=status.HTTP_202_ACCEPTED,
    # The body is read raw via request.body(), which FastAPI cannot see from the
    # signature — so declare it here or /docs renders no editor box.
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": {"type": "object"}}},
        }
    },
)
async def ingest_reading(
    request: Request,
    device_id: str = Header(alias="X-Device-Id"),
    tenant_id: str = Header(alias="X-Tenant-Id"),
    pipeline_hint: str | None = Header(default=None, alias="X-Pipeline-Hint"),
) -> dict[str, str]:
    settings = get_settings()

    if int(request.headers.get("content-length", 0)) > MAX_REQUEST_BYTES:
        raise HTTPException(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "request too large"
        )

    body = await request.body()
    if not body:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "empty body")

    job_id = str(ULID())
    size_bytes = len(body)
    digest = hashlib.sha256(body).hexdigest()

    try:
        data = body.decode()
    except UnicodeDecodeError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "body must be utf-8")

    received_at = datetime.now(UTC)
    if size_bytes <= settings.INLINE_PAYLOAD_MAX_BYTES:
        payload = InlinePayload(
            mode="inline", data=data, size_bytes=size_bytes, sha256=digest
        )

    else:
        key = f"{received_at:%Y/%m/%d}/{job_id}.json"
        s3_client.upload_file(settings.S3_BUCKET_LANDING, key, body)
        payload = ReferencePayload(
            mode="reference", s3_key=key, size_bytes=size_bytes, sha256=digest
        )

    descriptor = JobDescriptor(
        job_id=job_id,
        trace_id=uuid4().hex,
        stage="bronze",
        tenant_id=tenant_id,
        device_id=device_id,
        pipeline_hint=pipeline_hint,
        # domain hint
        received_at=received_at,
        payload=payload,
    )

    try:
        await enqueue(request.app.state.valkey, STREAM_BRONZE, descriptor)
    except ValkeyError:
        raise HTTPException(503, "queue unavailable") from None

    logger.info("accepted job_id=%s device=%s bytes=%d", job_id, device_id, size_bytes)
    return {"job_id": job_id}
