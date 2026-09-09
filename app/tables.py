"""M2/M4 — Iceberg table definitions and create-if-missing.

bronze_raw: job_id, trace_id, received_at, event_time, tenant_id, device_id,
source, pipeline_hint, schema_fp, payload (string). Partition day(received_at).
dead_letter: job_id, received_at, reason, schema_fp, s3_key, payload.
silver_*: schema DERIVED from a mapper's columns, never hardcoded.

Set at creation on every table (defaults are wrong for streaming writes):
  write.metadata.delete-after-commit.enabled = true
  write.metadata.previous-versions-max = 100
"""
