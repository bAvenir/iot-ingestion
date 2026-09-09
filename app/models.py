"""M1 — Job descriptor.

job_id (ULID, time-sortable), trace_id, stage, tenant_id, device_id,
pipeline_hint, received_at, attempt, parent_job_id.

payload is a discriminated union on `mode`:
  inline    -> data (JSON string), size_bytes, sha256
  reference -> s3_key, size_bytes, sha256
Inline at or below the threshold, reference above. Both modes must round-trip JSON.
"""
