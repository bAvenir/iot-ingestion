"""M5 — the pieces of the publish service that need nothing running."""

import json
from datetime import UTC, datetime

import pytest
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

from app.exceptions import InvalidRequest
from app.services.publish import (
    build_filter,
    export_key,
    publish_slice,
    tenant_safety_check,
    to_json,
)

START = datetime(2026, 9, 10, tzinfo=UTC)
END = datetime(2026, 9, 11, tzinfo=UTC)
TENANT = EqualTo("tenant_id", "demo")


@pytest.mark.parametrize("tenant_id", ["demo", "tenant-6e338c", "Tenant_01"])
def test_ordinary_tenant_ids_are_accepted(tenant_id):
    assert tenant_safety_check(tenant_id)


@pytest.mark.parametrize("tenant_id", ["../x", "a/b", "", "a b", "demo.x"])
def test_tenant_ids_that_could_escape_the_folder_are_rejected(tenant_id):
    assert not tenant_safety_check(tenant_id)


def test_bad_tenant_id_stops_before_anything_is_read():
    with pytest.raises(InvalidRequest):
        publish_slice("silver_air_quality", "../x")


def test_key_is_tenant_then_table_then_id():
    assert export_key("demo", "silver_air_quality", "01ABC") == "demo/silver_air_quality/01ABC.json"


def test_filter_is_only_the_tenant_by_default():
    assert build_filter("demo", None, None, None) == TENANT


def test_filter_with_a_device():
    expected = And(TENANT, EqualTo("device_id", "sensor-lab-04"))
    assert build_filter("demo", "sensor-lab-04", None, None) == expected


def test_filter_start_is_inclusive_and_end_is_exclusive():
    expected = And(
        And(TENANT, GreaterThanOrEqual("event_time", START.isoformat())),
        LessThan("event_time", END.isoformat()),
    )
    assert build_filter("demo", None, START, END) == expected


def test_filter_with_only_one_end_of_the_range():
    assert build_filter("demo", None, START, None) == And(
        TENANT, GreaterThanOrEqual("event_time", START.isoformat())
    )
    assert build_filter("demo", None, None, END) == And(
        TENANT, LessThan("event_time", END.isoformat())
    )


def test_rows_with_datetimes_become_json():
    rows = [{"device_id": "sensor-lab-04", "event_time": START, "temperature": 21.4}]
    decoded = json.loads(to_json(rows))
    assert decoded[0]["device_id"] == "sensor-lab-04"
    assert decoded[0]["temperature"] == 21.4
    assert decoded[0]["event_time"].startswith("2026-09-10")


def test_no_rows_is_an_empty_list():
    assert to_json([]) == b"[]"
