"""FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from valkey import ResponseError, ValkeyError

from app.api.ingest import ingest_router
from app.catalog import get_catalog
from app.config import get_settings
from app.queue import GROUP_BRONZE, STREAM_BRONZE, create_client
from app.tables import ensure_bronze_raw

settings = get_settings()


logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s [%(name)s] %(levelname)-8s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing the application...")
    app.state.valkey = create_client()
    try:
        await app.state.valkey.xgroup_create(
            STREAM_BRONZE, GROUP_BRONZE, id="$", mkstream=True
        )
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    ensure_bronze_raw(get_catalog())

    yield
    logger.info("Shutting down the application...")
    await app.state.valkey.aclose()


app = FastAPI(
    lifespan=lifespan,
    title="iot-ingestion",
    description="tbd",
    version="0.1.0",
)

app.include_router(ingest_router)


@app.get("/health")
async def health() -> dict[str, str | int]:

    try:
        stream_depth = await app.state.valkey.xlen(STREAM_BRONZE)
    except ValkeyError:
        raise HTTPException(503, "queue unavailable") from None

    return {"status": "ok", "stream_depth": stream_depth}
