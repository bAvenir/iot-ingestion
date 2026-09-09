"""M1 — POST /ingest.

Generate job_id (ULID), hash the body, pick inline vs reference by size.
Reference mode only: write to landing/ first, so the queue never points at
bytes that do not exist yet. Then XADD tasks:bronze. Return 202 + job_id.
Nothing touches Iceberg on the request path.
"""
