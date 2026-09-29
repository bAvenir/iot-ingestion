"""M2 — {"temp":1,"hum":2} == {"hum":9,"temp":8}; {"temp":"x"} differs."""

from app.fingerprint import FINGERPRINT_LENGTH, fingerprint


def test_key_order_does_not_matter():
    assert fingerprint({"temp": 1, "hum": 2}) == fingerprint({"hum": 9, "temp": 8})


def test_values_do_not_matter():
    assert fingerprint({"temp": 21.4}) == fingerprint({"temp": -300.0})


def test_type_change_matters():
    assert fingerprint({"temp": 1}) != fingerprint({"temp": "x"})


def test_int_and_float_are_the_same_shape():
    # One sensor sends 21 and 21.4 for the same field; two fingerprints for that
    # would mean two mappers for one vendor.
    assert fingerprint({"temp": 21}) == fingerprint({"temp": 21.4})


def test_bool_is_not_a_number():
    assert fingerprint({"on": True}) != fingerprint({"on": 1})


def test_different_key_names_differ():
    assert fingerprint({"temp": 1, "hum": 2}) != fingerprint({"t": 1, "h": 2})


def test_missing_key_differs():
    assert fingerprint({"temp": 1, "hum": 2}) != fingerprint({"temp": 1})


def test_nested_shape_is_captured():
    vendor_b = {"measurements": {"temperature_f": 70.5, "rh": 46.0}}
    flat = {"temperature_f": 70.5, "rh": 46.0}
    assert fingerprint(vendor_b) != fingerprint(flat)


def test_nested_type_change_matters():
    a = {"measurements": {"temperature_f": 70.5}}
    b = {"measurements": {"temperature_f": "70.5"}}
    assert fingerprint(a) != fingerprint(b)


def test_array_length_does_not_matter():
    assert fingerprint({"readings": [1, 2, 3]}) == fingerprint({"readings": [9]})


def test_array_element_type_matters():
    assert fingerprint({"readings": [1]}) != fingerprint({"readings": ["1"]})


def test_null_is_its_own_type():
    assert fingerprint({"co2": None}) != fingerprint({"co2": 400})


def test_is_short_and_stable():
    fp = fingerprint({"temp": 1, "hum": 2})
    assert len(fp) == FINGERPRINT_LENGTH
    assert fp == fingerprint({"temp": 1, "hum": 2})


def test_empty_payload_has_a_fingerprint():
    assert len(fingerprint({})) == FINGERPRINT_LENGTH
