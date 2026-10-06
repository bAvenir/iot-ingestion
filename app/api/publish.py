"""M5 — POST /publish: a slice of one silver table, by tenant, device and time range."""

import asyncio
from datetime import datetime

from fastapi import APIRouter, Header, HTTPException, Query

from app.exceptions import InvalidRequest, StorageUnavailable, UnknownTable
from app.services.publish import publish_slice

publish_router = APIRouter(tags=["publish"])


@publish_router.post("/publish")
async def publish(
    table: str = Query(description="silver table, e.g. silver_air_quality"),
    tenant_id: str = Header(alias="X-Tenant-Id"),
    device_id: str | None = Query(default=None, description="one device; omit for all"),
    start: datetime | None = Query(default=None, alias="from"),
    end: datetime | None = Query(default=None, alias="to"),
    limit: int = Query(default=10, ge=1, le=1000),
) -> dict:
    try:
        return await asyncio.to_thread(
            publish_slice, table, tenant_id, device_id, start, end, limit
        )
    except InvalidRequest as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    except UnknownTable as e:
        raise HTTPException(status_code=404, detail=str(e)) from None
    except StorageUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e)) from None
