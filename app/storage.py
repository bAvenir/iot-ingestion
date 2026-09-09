"""M1/M5 — S3 (RustFS) access.

Only two uses: oversized payloads to landing/{YYYY}/{MM}/{DD}/{job_id}.json
(written BEFORE the enqueue), and published/ RDF + DCAT output in M5.
warehouse/ is owned by pyiceberg, not touched here.
"""
