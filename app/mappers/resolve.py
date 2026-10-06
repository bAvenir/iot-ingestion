"""M3 — Resolution: pipeline_hint -> fingerprint key-overlap score -> unresolved.

Pure logic, no I/O, so all three outcomes are unit-testable.
"""

import json

from app.mappers.paths import (
    MISSING,
    get_path,
)


def resolve(row, mappers):
    hint = row["pipeline_hint"]
    if hint and hint in mappers:
        return mappers[hint]

    try:
        payload = json.loads(row["payload"])  # the actual JSON, with its keys
    except json.JSONDecodeError:
        return None

    for mapper in mappers.values():
        if all(
            get_path(payload, key) is not MISSING for key in mapper.match.required_keys
        ):
            return mapper
    return None
