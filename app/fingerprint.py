"""M2 — Schema fingerprint.

Sorted key set + inferred leaf types, hashed short. Key order must not matter;
a type change must. This is what makes vendor formats distinguishable in M3.

The fingerprint is a hash, so it identifies a shape without describing it: you
can group and match on it, but not read the keys back out. That is why
dead_letter stores the fingerprint *and* a sample payload.
"""

import hashlib
import json

FINGERPRINT_LENGTH = 12


def _type_name(value: object) -> str:
    """JSON type of a leaf.

    int and float both collapse to "number": the same sensor legitimately sends
    21 and 21.4 for one field, and splitting those into two fingerprints would
    mean two mappers for one vendor. bool is checked first — in Python it is a
    subclass of int.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


def _leaves(value: object, path: str = "") -> list[str]:
    """Flatten to sorted "path:type" strings, one per leaf.

    Nested objects recurse with dotted paths. Arrays are described by the set of
    types they contain, not their length — [21.4, 19.8] and [21.4] are the same
    shape, while ["a"] is not.
    """
    if isinstance(value, dict):
        out: list[str] = []
        for key, child in value.items():
            out.extend(_leaves(child, f"{path}.{key}" if path else key))
        return out
    if isinstance(value, list):
        if not value:
            return [f"{path}[]:empty"]
        element_types = sorted({_type_name(item) for item in value})
        out = [f"{path}[]:{'|'.join(element_types)}"]
        # Recurse into object elements so nested arrays of records are described
        # by their fields rather than just "array of object".
        for item in value:
            if isinstance(item, (dict, list)):
                out.extend(_leaves(item, f"{path}[]"))
        return sorted(set(out))
    return [f"{path}:{_type_name(value)}"]


def fingerprint(payload: dict) -> str:
    """Short, stable hash of a payload's shape."""
    signature = json.dumps(sorted(_leaves(payload)), separators=(",", ":"))
    return hashlib.sha256(signature.encode()).hexdigest()[:FINGERPRINT_LENGTH]
