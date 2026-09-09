"""M1/M2 — Valkey Streams.

Streams per stage: tasks:bronze, tasks:silver, tasks:publish.
XADD with MAXLEN trimming, idempotent XGROUP CREATE ... $ MKSTREAM on startup,
XREADGROUP batching, XACK only after the Iceberg commit, XAUTOCLAIM to recover
messages from a crashed worker.
"""
