"""FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.ingest import ingest_router
from app.config import get_settings
from app.queue import create_client

settings = get_settings()


logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s [%(name)s] %(levelname)-8s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing the application...")
    # One client for the process, reused by every request. Created here rather
    # than at import time so the pool binds to the running event loop.
    app.state.valkey = create_client()
    # TODO(M2): XGROUP CREATE tasks:bronze bronze-workers $ MKSTREAM (idempotent)
    yield
    logger.info("Shutting down the application...")
    await app.state.valkey.aclose()


app = FastAPI(
    lifespan=lifespan,
    title="iot-ingestion",
    description="tbd",
    version="0.1.0",
)
# app.add_exception_handler(HTTPException, http_exception_handler)
# app.add_exception_handler(RequestValidationError, validation_exception_handler)
# app.add_exception_handler(ValidationError, validation_exception_handler)
# app.add_exception_handler(Exception, unhandled_exception_handler)
# app.include_router(advisor_router)
# app.include_router(processes_router)
app.include_router(ingest_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
