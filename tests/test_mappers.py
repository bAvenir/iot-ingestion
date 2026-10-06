"""M3 — choosing a mapper for a bronze row: by hint, by the payload's keys, or none."""

import json

import pytest

from app.mappers.registry import get_registry
from app.mappers.resolve import resolve


def row(payload, hint=None) -> dict:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return {"pipeline_hint": hint, "payload": text}


@pytest.fixture(scope="module")
def mappers():
    return get_registry()


def test_hint_picks_the_mapper(mappers):
    # the payload looks like energy, but the producer says otherwise
    found = resolve(row({"power": 1, "energy": 2}, hint="indoor-air-quality"), mappers)
    assert found.id == "indoor-air-quality"


def test_no_hint_matches_on_keys(mappers):
    payload = {"measurements": {"temperature_f": 70.5, "rh": 46.0}}
    assert resolve(row(payload), mappers).id == "vendor-b-airquality"


def test_unknown_hint_still_matches_on_keys(mappers):
    found = resolve(row({"temp": 21.4, "hum": 0.47}, hint="typo-vendor"), mappers)
    assert found.id == "indoor-air-quality"


def test_unknown_shape_is_unresolved(mappers):
    assert resolve(row({"dev": "mystery-01", "t": 19.8, "h": 51}), mappers) is None


def test_missing_one_required_key_is_unresolved(mappers):
    assert resolve(row({"temp": 21.4}), mappers) is None


@pytest.mark.parametrize("text", ["hello, not json", "", "[1, 2, 3]", "123", "null"])
def test_payload_that_cannot_be_read_is_unresolved(mappers, text):
    assert resolve(row(text), mappers) is None
