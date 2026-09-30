"""M4 — apply_mapper: one bronze row in, one silver row out."""

import json
from datetime import UTC, datetime

import pytest

from app.exceptions import RowRejected
from app.mappers.registry import get_registry
from app.mappers.transform import apply_mapper

RECEIVED = datetime(2026, 9, 30, 10, 0, tzinfo=UTC)


def bronze_row(payload: dict | str, **overrides) -> dict:
    row = {
        "job_id": "01TEST",
        "trace_id": "t" * 32,
        "tenant_id": "tenant-a",
        "device_id": "from-bronze",
        "event_time": RECEIVED,
        "received_at": RECEIVED,
        "payload": payload if isinstance(payload, str) else json.dumps(payload),
    }
    return row | overrides


@pytest.fixture(scope="module")
def mappers():
    return get_registry()


def test_task_done_when(mappers):
    """{"temp":21.4,"hum":0.47} becomes temperature=21.4, humidity=47.0."""
    out = apply_mapper(mappers["indoor-air-quality"], bronze_row({"temp": 21.4, "hum": 0.47}))
    assert out["temperature"] == 21.4
    assert out["humidity"] == pytest.approx(47.0)


def test_lineage_and_envelope(mappers):
    out = apply_mapper(mappers["indoor-air-quality"], bronze_row({"temp": 21.4, "hum": 0.47}))
    assert (out["mapper_id"], out["mapper_version"]) == ("indoor-air-quality", 1)
    assert out["job_id"] == "01TEST"
    assert out["received_at"] == RECEIVED


def test_fahrenheit_is_converted(mappers):
    payload = {"measurements": {"temperature_f": 70.5, "rh": 46.0}}
    out = apply_mapper(mappers["vendor-b-airquality"], bronze_row(payload))
    assert out["temperature"] == pytest.approx(21.39, abs=0.01)
    assert out["humidity"] == 46.0


def test_kw_scaled_to_w_and_device_id_from_payload(mappers):
    payload = {"meter": "meter-hall-02", "readings": {"active_power_kw": 1.43, "total_kwh": 88.2}}
    out = apply_mapper(mappers["vendor-c-energy"], bronze_row(payload))
    assert out["power"] == pytest.approx(1430.0)
    assert out["energy"] == pytest.approx(88200.0)
    assert out["device_id"] == "meter-hall-02"


def test_two_vendors_same_silver_columns(mappers):
    a = apply_mapper(mappers["indoor-air-quality"], bronze_row({"temp": 21.4, "hum": 0.47}))
    b = apply_mapper(
        mappers["vendor-b-airquality"],
        bronze_row({"measurements": {"temperature_f": 70.5, "rh": 46.0}}),
    )
    assert set(a) == set(b)


def test_event_time_from_mapper_wins(mappers):
    payload = {"temp": 21.4, "hum": 0.47, "ts": "2026-09-10T08:14:22Z"}
    out = apply_mapper(mappers["indoor-air-quality"], bronze_row(payload))
    assert out["event_time"] == datetime(2026, 9, 10, 8, 14, 22, tzinfo=UTC)


def test_missing_optional_value_is_null(mappers):
    out = apply_mapper(mappers["indoor-air-quality"], bronze_row({"temp": 21.4}))
    assert out["humidity"] is None


def test_out_of_range_is_rejected(mappers):
    with pytest.raises(RowRejected) as e:
        apply_mapper(mappers["indoor-air-quality"], bronze_row({"temp": 900, "hum": 0.4}))
    assert e.value.reason == "out_of_range"


def test_wrong_type_is_rejected(mappers):
    with pytest.raises(RowRejected) as e:
        apply_mapper(mappers["indoor-air-quality"], bronze_row({"temp": "hot", "hum": 0.4}))
    assert e.value.reason == "bad_value"


def test_non_json_payload_is_rejected(mappers):
    with pytest.raises(RowRejected) as e:
        apply_mapper(mappers["indoor-air-quality"], bronze_row("not json"))
    assert e.value.reason == "bad_payload"
