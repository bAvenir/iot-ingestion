"""M4 — Silver worker.

Scan only the bronze snapshot range newer than the last processed snapshot id,
hand Arrow to DuckDB, json_extract per mapper column, cast + scale/convert,
range/null validation (failures -> dead_letter with a reason), anti-join dedup on
(device_id, event_time) against the recent partition, append with mapper_id +
mapper_version for lineage.

Open: where the snapshot cursor lives (Postgres row vs small S3 object) — the one
piece of state not held by Iceberg.
"""
