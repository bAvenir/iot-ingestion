"""Dotted-path lookup into a parsed payload, shared by resolve and transform."""

# Distinct from None: {"co2": null} has the key, its value is just null.
MISSING = object()


def get_path(payload: object, path: str) -> object:
    """get_path({"a": {"b": 1}}, "a.b") -> 1. Absent paths give MISSING."""
    for part in path.split("."):
        if not isinstance(payload, dict) or part not in payload:
            return MISSING
        payload = payload[part]
    return payload
