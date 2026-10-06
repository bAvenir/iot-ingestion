"""M3 — Pydantic model of the mapper YAML. Format documented in README.md.

The semantic fields (unit, property_iri) are not optional decoration: the silver
schema and the RDF export are both derived from this one file.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Column(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    from_: str = Field(alias="from")  # `from` is a Python keyword
    type: Literal["double", "int", "string", "bool", "timestamp"]
    unit: str
    property_iri: str
    scale: float = 1
    convert: Literal["fahrenheit_to_celsius"] | None = None
    min: float | None = None
    max: float | None = None
    required: bool = False


class Match(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_keys: list[str]


class Mapper(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    version: int
    target_table: str
    match: Match
    columns: list[Column]
    event_time_from: str | None = None
    device_id_from: str | None = None
