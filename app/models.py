"""M1 — Job descriptor.

The wire contract for the Valkey stream: everything a worker needs to do its
job without asking anything else, and nothing more.

payload is a discriminated union on `mode`:
  inline    -> data (JSON string), size_bytes, sha256
  reference -> s3_key, size_bytes, sha256
Inline at or below settings.INLINE_PAYLOAD_MAX_BYTES, reference above. The
threshold lives in config, not here — the model only records which branch the
API chose.

Deliberately absent: schema_fp, event_time (bronze computes them), mapper_id and
mapper_version (silver). Those are stage outputs, not stage inputs.
"""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class InlinePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["inline"]
    data: str  # raw body, JSON as text — unparsed until silver
    size_bytes: int = Field(ge=0)
    sha256: str

    @model_validator(mode="after")
    def _size_matches(self) -> "InlinePayload":
        if len(self.data.encode()) != self.size_bytes:
            raise ValueError("size_bytes does not match len(data)")
        return self


class ReferencePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["reference"]
    s3_key: str
    size_bytes: int = Field(ge=0)
    sha256: str


Payload = Annotated[InlinePayload | ReferencePayload, Field(discriminator="mode")]


class JobDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    trace_id: str
    stage: Literal["bronze", "silver", "publish"]
    tenant_id: str
    device_id: str
    received_at: datetime
    payload: Payload

    pipeline_hint: str | None = None
    parent_job_id: str | None = None
    attempt: int = Field(default=1, ge=1)

    @field_validator("received_at")
    @classmethod
    def _require_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")
        return v.astimezone(UTC)
