# Mappers

A mapper is a small YAML file that describes one vendor's payload format and how to turn it into a silver row. It's a recipe, not code: the pipeline reads it and does the rest.

Many vendors can report the same thing in different shapes. Each shape gets its own mapper, and mappers for the same kind of measurement point at the same silver table. Onboarding a new vendor means adding one file to `defs/`.

## Example

A sensor that sends nested JSON in Fahrenheit:

```json
{"device_id": "sensor-office-11", "time": "2026-09-10T08:14:30Z",
 "measurements": {"temperature_f": 70.5, "rh": 46.0}, "firmware": "2.3.1"}
```

```yaml
id: vendor-b-airquality
version: 1
target_table: silver_air_quality

match:
  required_keys: [measurements.temperature_f, measurements.rh]

event_time_from: time

columns:
  - name: temperature
    from: measurements.temperature_f
    type: double
    convert: fahrenheit_to_celsius
    unit: unit:DEG_C
    property_iri: qudt:Temperature
    min: -40
    max: 85

  - name: humidity
    from: measurements.rh
    type: double
    unit: unit:PERCENT_RH
    property_iri: qudt:RelativeHumidity
    min: 0
    max: 100
```

That payload becomes `temperature = 21.4`, `humidity = 46.0`. Anything the mapper doesn't mention, like `firmware`, is dropped.

## Top-level fields

| Field | Required | Meaning |
|---|---|---|
| `id` | yes | Names this format, in kebab-case. Also what a producer sends as `X-Pipeline-Hint`. |
| `version` | yes | Integer. Bump it whenever the recipe changes, so rows made by an older version can be found and reprocessed. |
| `target_table` | yes | The silver table rows go to, like `silver_air_quality`. A name no mapper has used before creates a new table when the silver worker starts. |
| `match` | yes | How to recognise this format when the producer doesn't say. See below. |
| `event_time_from` | no | Where the reading's timestamp lives. Without it, bronze's best guess is used. |
| `device_id_from` | no | Where the device id lives, if the vendor calls it something other than `device_id`. |
| `columns` | yes | One entry per silver column. See below. |

## Columns

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | The silver column this fills. |
| `from` | yes | Where to read the value in the payload. |
| `type` | yes | One of `double`, `int`, `string`, `bool`, `timestamp`. |
| `scale` | no | Multiply the value by this. Defaults to 1. Handy for `0.47` becoming `47`. |
| `convert` | no | A named conversion. For now only `fahrenheit_to_celsius`. |
| `unit` | yes | The canonical unit, as a QUDT name like `unit:DEG_C`. |
| `property_iri` | yes | What is being measured, like `qudt:Temperature`. |
| `min`, `max` | no | Valid range. Values outside it are rejected to the dead letter table. |
| `required` | no | If `true`, a missing value rejects the row. Defaults to `false`, so a missing sensor becomes `NULL`. |

`scale` is applied after `convert`.

`unit` and `property_iri` aren't used by silver at all. They're here because the RDF export reads the same file, so the meaning of each column is written down exactly once.

## Paths

`from`, `match.required_keys`, `event_time_from` and `device_id_from` all use dotted paths into the payload:

```
temp                          →  {"temp": 21.4}
measurements.temperature_f    →  {"measurements": {"temperature_f": 70.5}}
```

## Match vs from

They look similar but do different jobs.

- **`match`** is for *recognising* a payload. It answers "is this one mine?"
- **`from`** is for *extracting* a value. It answers "where is the number?"

Often the same keys show up in both.

## How a mapper gets picked

For every bronze row, in this order:

1. If the producer sent a `pipeline_hint`, use the mapper with that `id`.
2. If we've seen this exact payload shape (fingerprint) before, use the same mapper.
3. Otherwise pick the mapper whose `required_keys` are all present in the payload.
4. If nothing fits, the row is unresolved and goes to the dead letter table.

## Adding a mapper

1. Drop a new `.yml` file into `defs/`.
2. Restart the app. Every mapper is validated on startup, so a typo fails right away instead of halfway through a batch.
