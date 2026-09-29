"""M3 — Load and validate every YAML in mappers/defs/ at startup.

Index by id and by fingerprint. An invalid mapper must fail loudly on startup,
not on first use.
"""

from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import ValidationError

from app.exceptions import MapperConfigError
from app.mappers.schema import Mapper

DEFS_DIR = Path(__file__).parent / "defs"


@lru_cache
def get_registry() -> dict[str, Mapper]:

    mappers: dict[str, Mapper] = {}

    for path in sorted(DEFS_DIR.glob("*.yml")):
        try:
            data = yaml.safe_load(path.read_text())
            mapper = Mapper.model_validate(data)
        except (ValidationError, yaml.YAMLError) as e:
            raise MapperConfigError(f"{path.name}: {e}") from e

        if mapper.id in mappers:
            raise MapperConfigError(f"duplicate mapper id {mapper.id!r} in {path.name}")

        mappers[mapper.id] = mapper

    return mappers
