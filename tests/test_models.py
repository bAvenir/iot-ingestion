"""M1 — descriptor round-trips JSON in BOTH payload modes (inline and reference)."""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.models import JobDescriptor

BASE = {
    "job_id": "01JB7XQ2M4KP8ZQZ2W9F0H3R7T",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "stage": "bronze",
    "tenant_id": "0c4f8e2a-1111-2222-3333-444455556666",
    "device_id": "sensor-lab-04",
    "received_at": datetime(2026, 9, 8, 10, 14, 22, tzinfo=UTC),
}
INLINE = {"mode": "inline", "data": '{"temp":21.4}', "size_bytes": 13, "sha256": "9f2c"}
REFERENCE = {
    "mode": "reference",
    "s3_key": "landing/2026/09/08/01JB7XQ2M4KP8ZQZ2W9F0H3R7T.json",
    "size_bytes": 184320,
    "sha256": "ab19",
}


@pytest.mark.parametrize("payload", [INLINE, REFERENCE], ids=["inline", "reference"])
def test_round_trips_json(payload):
    d = JobDescriptor(**BASE, payload=payload)
    assert JobDescriptor.model_validate_json(d.model_dump_json()) == d


def test_discriminator_selects_the_branch():
    assert JobDescriptor(**BASE, payload=INLINE).payload.data == '{"temp":21.4}'
    assert JobDescriptor(**BASE, payload=REFERENCE).payload.s3_key.startswith(
        "landing/"
    )


def test_unknown_mode_is_rejected():
    with pytest.raises(ValidationError, match="union_tag_invalid"):
        JobDescriptor(**BASE, payload={"mode": "carrier-pigeon", "sha256": "x"})


def test_naive_received_at_is_rejected():
    with pytest.raises(ValidationError, match="timezone-aware"):
        JobDescriptor(
            **{**BASE, "received_at": datetime(2026, 9, 8, 10, 14, 22)}, payload=INLINE
        )


def test_received_at_normalised_to_utc():
    cet = timezone(timedelta(hours=2))
    d = JobDescriptor(
        **{**BASE, "received_at": datetime(2026, 9, 8, 12, 14, 22, tzinfo=cet)},
        payload=INLINE,
    )
    assert d.received_at == BASE["received_at"]
    assert d.received_at.tzinfo is UTC


def test_size_must_match_inline_data():
    with pytest.raises(ValidationError, match="size_bytes does not match"):
        JobDescriptor(**BASE, payload={**INLINE, "size_bytes": 999})


def test_extra_field_is_rejected():
    with pytest.raises(ValidationError, match="extra_forbidden"):
        JobDescriptor(**BASE, payload=INLINE, sneaky="value")


def test_defaults():
    d = JobDescriptor(**BASE, payload=INLINE)
    assert (d.attempt, d.pipeline_hint, d.parent_job_id) == (1, None, None)
