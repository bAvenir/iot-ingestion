"""M2 — envelope normalisation.

ts / timestamp / time all map to event_time; missing falls back to received_at.
Inline and reference payloads must produce identical bronze rows.
"""

import hashlib
from datetime import UTC, datetime

import pytest

from app.models import InlinePayload, JobDescriptor, ReferencePayload
from app.workers.bronze import SOURCE_HTTP, extract_event_time, to_bronze_row

RECEIVED_AT = datetime(2026, 9, 22, 10, 0, 0, tzinfo=UTC)
EVENT_AT = datetime(2026, 9, 10, 8, 14, 22, tzinfo=UTC)
READING = '{"device_id": "sensor-lab-04", "ts": "2026-09-10T08:14:22Z", "temp": 21.4}'


def descriptor(payload) -> JobDescriptor:
    return JobDescriptor(
        job_id="01M342VYT1WCHX1CDZ24T7NM1X",
        trace_id="9f529d02f04c4c008817894d544e11f2",
        stage="bronze",
        tenant_id="0c4f8e2a-1111-2222-3333-444455556666",
        device_id="sensor-lab-04",
        pipeline_hint="indoor-air-quality",
        received_at=RECEIVED_AT,
        payload=payload,
    )


# --- event time ------------------------------------------------------------


@pytest.mark.parametrize("key", ["ts", "timestamp", "time"])
def test_three_spellings_all_map_to_event_time(key):
    assert extract_event_time({key: "2026-09-10T08:14:22Z"}, RECEIVED_AT) == EVENT_AT


def test_offset_is_converted_not_relabelled():
    # 10:14:22+02:00 is the same instant as 08:14:22Z.
    assert (
        extract_event_time({"ts": "2026-09-10T10:14:22+02:00"}, RECEIVED_AT) == EVENT_AT
    )


def test_naive_timestamp_is_assumed_utc():
    assert extract_event_time({"ts": "2026-09-10T08:14:22"}, RECEIVED_AT) == EVENT_AT


def test_missing_key_falls_back_to_received_at():
    assert extract_event_time({"temp": 21.4}, RECEIVED_AT) == RECEIVED_AT


def test_unparseable_timestamp_falls_back_rather_than_raising():
    assert extract_event_time({"ts": "not-a-date"}, RECEIVED_AT) == RECEIVED_AT


def test_key_order_is_preference_order():
    payload = {"ts": "2026-09-10T08:14:22Z", "timestamp": "1999-01-01T00:00:00Z"}
    assert extract_event_time(payload, RECEIVED_AT) == EVENT_AT


# --- row building ----------------------------------------------------------


def test_row_has_every_bronze_column():
    row = to_bronze_row(descriptor(inline(READING)), READING)
    assert set(row) == {
        "job_id",
        "trace_id",
        "received_at",
        "event_time",
        "tenant_id",
        "device_id",
        "source",
        "pipeline_hint",
        "schema_fp",
        "payload",
    }
    assert row["source"] == SOURCE_HTTP
    assert row["received_at"] == RECEIVED_AT
    assert row["event_time"] == EVENT_AT


def test_payload_is_stored_verbatim():
    # Whitespace and key order must survive, or the stored bytes no longer
    # match the descriptor's sha256.
    odd = '{  "ts":"2026-09-10T08:14:22Z" ,   "temp"  : 21.4 }'
    assert to_bronze_row(descriptor(inline(odd)), odd)["payload"] == odd


def test_unparseable_json_still_produces_a_row():
    junk = "this is not json"
    row = to_bronze_row(descriptor(inline(junk)), junk)
    assert row["payload"] == junk
    assert row["event_time"] == RECEIVED_AT


@pytest.mark.parametrize("text", ["[1, 2, 3]", "123", '"just text"', "true", "null"])
def test_json_that_is_not_an_object_still_produces_a_row(text):
    # valid JSON, but nothing to look a key up in
    row = to_bronze_row(descriptor(inline(text)), text)
    assert row["payload"] == text
    assert row["event_time"] == RECEIVED_AT


def test_inline_and_reference_produce_identical_rows():
    """The done-when: after the mode branch, nothing differs."""
    inline_row = to_bronze_row(descriptor(inline(READING)), READING)
    reference_row = to_bronze_row(descriptor(reference(READING)), READING)
    assert inline_row == reference_row


# --- helpers ---------------------------------------------------------------


def inline(text: str) -> InlinePayload:
    return InlinePayload(
        mode="inline",
        data=text,
        size_bytes=len(text.encode()),
        sha256=hashlib.sha256(text.encode()).hexdigest(),
    )


def reference(text: str) -> ReferencePayload:
    return ReferencePayload(
        mode="reference",
        s3_key="2026/09/22/01M342VYT1WCHX1CDZ24T7NM1X.json",
        size_bytes=len(text.encode()),
        sha256=hashlib.sha256(text.encode()).hexdigest(),
    )
