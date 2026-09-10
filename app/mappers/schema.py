"""M3 — Pydantic model of the mapper YAML.

id, version, target_table, match.required_keys, and columns with
name / from (JSONPath) / type / scale / convert / unit / property_iri.
The semantic fields (unit, property_iri) are not optional decoration: the silver
schema and the RDF export are both derived from this one file.
"""
