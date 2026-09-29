# iot-ingestion

Takes IoT readings over HTTP and lands them in Apache Iceberg tables, so readings from different vendors end up in one queryable place.

> **Work in progress.** Ingestion and the bronze layer work end to end. Silver and RDF publishing are next.

## How it works

```
POST /ingest  →  Valkey stream  →  bronze worker  →  bronze.raw (Iceberg)
                                         │
                                         └─ failures  →  bronze.dead_letter
```

1. **Ingest.** The API accepts any JSON body and puts a job on the `tasks:bronze` stream. Small readings travel inside the message. Anything over 64 KB goes to the `landing` bucket first and the message carries a pointer to it.
2. **Bronze.** A worker reads the stream in batches (up to 100 messages or 10 seconds), verifies each payload's hash, and writes one row per reading. The original JSON is stored untouched, alongside the event time and a fingerprint of its shape.
3. **Failures.** Network hiccups are retried up to three times. Anything that can never succeed (a missing object, a corrupted payload, a malformed message) goes straight to the dead letter table so it can be inspected and reprocessed later.

Messages are only acknowledged after their rows are committed, so a crash means a reading gets redelivered, never lost. Work left behind by a crashed worker is picked up by the next one.

## Stack

FastAPI, Valkey streams, RustFS (S3), Apache Iceberg via pyiceberg, Postgres as the Iceberg catalog, DuckDB for querying.

## Running it locally

You need Docker and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env          # then fill in the passwords
docker compose up -d          # Postgres, Valkey, RustFS
uv sync
```

Start the API and the worker in two terminals:

```bash
uv run dev
```

```bash
uv run python -c "
import asyncio, logging
logging.basicConfig(level='INFO')
from app.workers import bronze
asyncio.run(bronze.run(asyncio.Event()))"
```

Buckets `landing`, `warehouse` and `published` need to exist in RustFS. The Iceberg tables are created automatically on startup.

## Try it

Send a reading:

```bash
curl -X POST localhost:8000/ingest \
  -H 'X-Device-Id: sensor-lab-04' \
  -H 'X-Tenant-Id: t1' \
  -H 'Content-Type: application/json' \
  --data-binary @samples/indoor-air-quality.json
```

Within about ten seconds the worker logs a commit. Then read it back:

```bash
uv run python -c "
import duckdb
from app.catalog import get_catalog
t = get_catalog().load_table('bronze.raw')
con = duckdb.connect(); con.register('bronze', t.scan().to_arrow())
con.sql('SELECT device_id, event_time, schema_fp FROM bronze').show()"
```

More sample payloads, including ones that should fail, are in [`samples/`](samples/README.md).

## Project layout

```
app/
  api/          HTTP endpoints
  workers/      bronze worker (silver and publish to come)
  mappers/      vendor format definitions (not wired up yet)
  models.py     the job message format
  tables.py     Iceberg table definitions
  fingerprint.py
  storage.py    S3 access
  queue.py      Valkey streams
tests/
samples/
```

Run the tests with `uv run pytest`.

## What's next

- **Silver layer.** Match each reading to a vendor mapper and write typed, unit-normalised rows.
- **RDF publishing.** Export a slice of silver as SOSA/QUDT observations with a DCAT description.
- **Housekeeping.** Start the worker from the app itself, add a Dockerfile, and compact Iceberg snapshots.

## Releasing

Releases are cut with the bAvenir release tool (`bvr-ci`). Config lives in `.ci-config.yml`.
