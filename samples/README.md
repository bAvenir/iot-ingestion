Sample payloads for `POST /ingest`.

    curl -i -X POST localhost:8000/ingest \
      -H 'X-Device-Id: sensor-lab-04' \
      -H 'X-Tenant-Id: t1' \
      -H 'X-Pipeline-Hint: indoor-air-quality' \
      -H 'Content-Type: application/json' \
      --data-binary @samples/indoor-air-quality.json

| File | Exercises |
|---|---|
| `indoor-air-quality.json` | the happy path; `ts` as the event-time spelling |
| `iot-energy.json` | second mapper; `timestamp` spelling |
| `vendor-b-fahrenheit.json` | nested payload, Fahrenheit, `time` spelling, vendor-only fields to drop |
| `unknown-shape.json` | no mapper matches -> dead_letter (M4) |

Oversized body, to exercise reference mode (no file committed — generate it):

    python3 -c "import json;print(json.dumps({'blob':'x'*70000}))" > /tmp/big.json
    curl -i -X POST localhost:8000/ingest \
      -H 'X-Device-Id: d1' -H 'X-Tenant-Id: t1' \
      -H 'Content-Type: application/json' --data-binary @/tmp/big.json
