"""Scenario run against a live stack: API + bronze worker + silver worker.

    uv run python scripts/scenarios.py            # everything except the poison case
    uv run python scripts/scenarios.py --poison   # also send a non-JSON body (may crash silver)

Every run uses fresh device ids, so it can be repeated. Not a pytest suite: it
needs Valkey, Postgres, RustFS and the three processes running.
"""

import hashlib
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from valkey import Valkey  # noqa: E402

from app.catalog import get_catalog  # noqa: E402
from app.config import get_settings  # noqa: E402
from app.storage import S3Storage  # noqa: E402

API = "http://localhost:8000"
RUN = uuid4().hex[:6]
TENANT = f"tenant-{RUN}"
BASE = datetime(2026, 10, 2, 6, 0, 0, tzinfo=UTC)

settings = get_settings()
vk = Valkey.from_url(settings.VALKEY_URL, decode_responses=True)
results: list[tuple[str, str, str, str]] = []


def dev(case: int, suffix: str = "") -> str:
    return f"t{RUN}-c{case:02d}{suffix}"


def at(case: int, second: int = 0) -> datetime:
    return BASE + timedelta(minutes=case, seconds=second)


def iso(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def record(case: str, status: str, name: str, detail: str = "") -> None:
    results.append((case, status, name, detail))
    print(f"  {case:>3}  {status:<7} {name}" + (f"  ->  {detail}" if detail else ""))


def check(case: int | str, name: str, ok: bool, detail: str = "") -> None:
    record(str(case), "PASS" if ok else "FAIL", name, detail)


def finding(case: int | str, name: str, is_problem: bool, detail: str) -> None:
    """For cases where the question is 'what does it do?' rather than pass/fail."""
    record(str(case), "ISSUE" if is_problem else "OK", name, detail)


def post(device, body, *, tenant=TENANT, hint=None, omit=()):
    if isinstance(body, (dict, list)):
        body = json.dumps(body)
    if isinstance(body, str):
        body = body.encode()
    headers = {"Content-Type": "application/json"}
    if "device" not in omit:
        headers["X-Device-Id"] = device
    if "tenant" not in omit:
        headers["X-Tenant-Id"] = tenant
    if hint:
        headers["X-Pipeline-Hint"] = hint
    request = urllib.request.Request(
        f"{API}/ingest", data=body, headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}
    except (urllib.error.URLError, OSError) as e:
        return f"connection error: {e}", {}


def health() -> dict:
    with urllib.request.urlopen(f"{API}/health", timeout=5) as response:
        return json.loads(response.read())


def descriptor(device: str, text: str) -> dict:
    raw = text.encode()
    return {
        "job_id": f"T{RUN}{uuid4().hex[:16]}".upper(),
        "trace_id": uuid4().hex,
        "stage": "bronze",
        "tenant_id": TENANT,
        "device_id": device,
        "received_at": datetime.now(UTC).isoformat(),
        "payload": {
            "mode": "inline",
            "data": text,
            "size_bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        },
    }


def xadd_job(job: dict | str) -> str:
    return vk.xadd(
        "tasks:bronze", {"job": job if isinstance(job, str) else json.dumps(job)}
    )


def drain(timeout: int = 180) -> bool:
    """Wait until both consumer groups have nothing pending and nothing unread."""
    deadline, quiet = time.time() + timeout, 0
    while time.time() < deadline:
        groups = [*vk.xinfo_groups("tasks:bronze"), *vk.xinfo_groups("tasks:silver")]
        quiet = (
            quiet + 1
            if all(g["pending"] == 0 and not g.get("lag") for g in groups)
            else 0
        )
        if quiet >= 3:
            return True
        time.sleep(1.5)
    return False


class Tables:
    """Rows written by this run, indexed by device id."""

    def __init__(self) -> None:
        catalog = get_catalog()

        def mine(row: dict) -> bool:
            return row["device_id"].startswith(f"t{RUN}") or row[
                "tenant_id"
            ].startswith(TENANT)

        def load(name: str) -> list[dict]:
            return catalog.load_table(name).scan().to_arrow().to_pylist()

        self.bronze = [r for r in load("bronze.raw") if mine(r)]
        self.air = [r for r in load("silver.air_quality") if mine(r)]
        self.energy = [r for r in load("silver.energy") if mine(r)]
        self.dead_all = load("bronze.dead_letter")
        self.dead = [r for r in self.dead_all if mine(r)]

    @staticmethod
    def of(rows: list[dict], device: str) -> list[dict]:
        return [r for r in rows if r["device_id"] == device]

    def reason(self, device: str) -> str | None:
        hits = self.of(self.dead, device)
        return hits[0]["reason"] if hits else None

    def reason_by_job(self, job_id: str) -> str | None:
        hits = [r for r in self.dead_all if r["job_id"] == job_id]
        return hits[0]["reason"] if hits else None


def running(worker: str) -> bool:
    pattern = f"from app.workers import {worker}"
    return subprocess.run(["pgrep", "-f", pattern], capture_output=True).returncode == 0


def air(case: int, second: int = 0, **values) -> dict:
    return {"temp": 21.4, "hum": 0.47, "ts": iso(at(case, second))} | values


def phase_api() -> None:
    print("\nAPI")
    status, body = post(dev(1), air(1))
    check(1, "valid reading", status == 202 and "job_id" in body, f"{status} {body}")
    check(2, "empty body", post(dev(2), b"")[0] == 400, str(post(dev(2), b"")[0]))
    check(3, "missing X-Device-Id", post(dev(3), air(3), omit=("device",))[0] == 422)
    check(4, "missing X-Tenant-Id", post(dev(4), air(4), omit=("tenant",))[0] == 422)
    status = post(dev(5), "x" * 1_100_000)[0]
    check(
        5, "body over 1 MB", status == 413 or "connection" in str(status), str(status)
    )

    big = json.dumps({"batch": [{"i": i, "pad": "x" * 60} for i in range(1000)]})
    status, body = post(dev(6), big)
    landed = False
    if status == 202:
        listing = S3Storage().client.list_objects_v2(Bucket=settings.S3_BUCKET_LANDING)
        landed = any(body["job_id"] in o["Key"] for o in listing.get("Contents", []))
    check(
        6,
        f"{len(big) // 1024} KB body goes to landing/",
        status == 202 and landed,
        f"{status}, object={landed}",
    )
    check(7, "non-UTF-8 body", post(dev(7), b"\xff\xfe\xfa")[0] == 400)
    record("8", "SKIP", "Valkey stopped -> 503", "would take your running workers down")
    status = post("", air(9))[0]
    finding(
        9,
        "empty X-Device-Id value",
        status == 202,
        f"status {status}"
        + (" (accepted with an empty device id)" if status == 202 else ""),
    )


def phase_pipeline() -> dict:
    print("\nsending pipeline cases ...")
    sent: dict = {}
    depth_before = health()["stream_depth"]

    sent[13] = '{  "temp" : 21.4 ,"hum":0.47,   "ts":"' + iso(at(13)) + '" }\n\n'
    post(dev(13), sent[13])
    for suffix, key in (("a", "ts"), ("b", "timestamp"), ("c", "time")):
        post(dev(14, suffix), {"k": 1, key: iso(at(14))})
    post(dev(15), {"k": 1, "recorded": iso(at(15))})
    post(dev(16), {"k": 1, "ts": "2026-10-02T10:00:00+02:00"})
    post(dev(17), {"k": 1, "ts": 1759395600})
    post(dev(18, "a"), {"a": 1, "b": 2})
    post(dev(18, "b"), {"b": 9, "a": 8})
    post(dev(19, "a"), {"v": 21})
    post(dev(19, "b"), {"v": 21.4})

    # bronze failures, straight into the stream
    bad_hash = descriptor(dev(21), json.dumps(air(21)))
    bad_hash["payload"]["sha256"] = "0" * 64
    xadd_job(bad_hash)
    missing = descriptor(dev(22), "{}")
    missing["payload"] = {
        "mode": "reference",
        "s3_key": f"nope/{RUN}.json",
        "size_bytes": 10,
        "sha256": "0" * 64,
    }
    xadd_job(missing)
    sent[23] = xadd_job('{"bad":"json"}')
    sent[24] = vk.xadd("tasks:bronze", {"hello": "world"})
    wrong_size = descriptor(dev(25), json.dumps(air(25)))
    wrong_size["payload"]["size_bytes"] = 999
    sent[25] = xadd_job(wrong_size)
    naive = descriptor(dev(26), json.dumps(air(26)))
    naive["received_at"] = "2026-10-02T10:00:00"
    sent[26] = xadd_job(naive)

    # resolution
    post(dev(28, "a"), air(28))
    post(
        dev(28, "b"),
        {"time": iso(at(28)), "measurements": {"temperature_f": 70.5, "rh": 46.0}},
    )
    post(dev(28, "c"), {"timestamp": iso(at(28)), "power": 1432.5, "energy": 88213.7})
    sent["28d"] = f"m{RUN}"
    post(
        dev(28, "d"),
        {
            "meter": sent["28d"],
            "ts": iso(at(28)),
            "readings": {"active_power_kw": 1.43, "total_kwh": 88.2},
        },
    )
    post(dev(29), air(29), hint="indoor-air-quality")
    post(dev(30), air(30), hint="typo-vendor")
    post(
        dev(31),
        {"timestamp": iso(at(31)), "power": 100.0, "energy": 200.0},
        hint="indoor-air-quality",
    )
    post(dev(32), {"dev": "x", "ts": iso(at(32)), "t": 19.8, "h": 51})
    post(dev(33), air(33) | {"power": 100.0, "energy": 200.0})
    post(dev(35), {"temp": 21.0, "ts": iso(at(35))})

    # transform / validation
    post(dev(36, "a"), air(36, temp=-40))
    post(dev(36, "b"), air(36, temp=85))
    post(dev(37), air(37, temp=85.1))
    post(dev(38), air(38, hum=1.5))
    post(dev(39), air(39, temp="21.4"))
    post(dev(40), air(40, temp="hot"))
    post(dev(41), air(41, temp=None))
    post(dev(42), air(42, temp=True))
    post(
        dev(43),
        {"ts": iso(at(43)), "readings": {"active_power_kw": 2.0, "total_kwh": 5.0}},
    )
    post(
        dev(44), air(44) | {"firmware": "2.3.1", "battery": 87, "nested": {"x": [1, 2]}}
    )

    # deduplication
    post(dev(45), air(45))
    post(dev(45), air(45))
    post(dev(46), air(46))
    post(dev(47, "a"), air(47))
    post(dev(47, "b"), air(47))
    post(dev(48), air(48, temp=20.0))
    post(dev(48), air(48, temp=30.0))
    post(dev(49), air(49), tenant=f"{TENANT}-A")
    post(dev(49), air(49), tenant=f"{TENANT}-B")
    post(dev(50), {"temp": 21.4, "hum": 0.47})
    post(dev(50), {"temp": 21.4, "hum": 0.47})

    sent["depth_before"] = depth_before
    return sent


def check_pipeline(sent: dict) -> None:
    t = Tables()

    def b(device: str) -> list[dict]:
        return t.of(t.bronze, device)

    print("\nBRONZE")
    finding(
        10,
        "/health stream_depth",
        health()["stream_depth"] > sent["depth_before"],
        f"{sent['depth_before']} before, {health()['stream_depth']} after everything was processed: it counts stream length, not backlog",
    )
    record(
        "12", "SKIP", "JSON array body", "run with --poison (crashes the bronze worker)"
    )
    row = b(dev(13))
    check(
        13, "payload stored byte-identical", bool(row) and row[0]["payload"] == sent[13]
    )
    spellings = [
        b(dev(14, s))[0]["event_time"] == at(14) for s in "abc" if b(dev(14, s))
    ]
    check(
        14,
        "ts / timestamp / time parsed",
        spellings == [True, True, True],
        str(spellings),
    )
    row = b(dev(15))
    check(
        15,
        "'recorded' falls back to received_at",
        bool(row) and row[0]["event_time"] == row[0]["received_at"],
    )
    row = b(dev(16))
    check(
        16,
        "+02:00 converted to UTC",
        bool(row) and row[0]["event_time"] == datetime(2026, 10, 2, 8, 0, tzinfo=UTC),
        str(row[0]["event_time"]) if row else "no row",
    )
    row = b(dev(17))
    fell_back = bool(row) and row[0]["event_time"] == row[0]["received_at"]
    finding(
        17,
        "epoch-number timestamp",
        fell_back,
        "ignored, event_time = received_at" if fell_back else "parsed",
    )
    fps = [b(dev(18, s))[0]["schema_fp"] for s in "ab" if b(dev(18, s))]
    check(
        18,
        "key order doesn't change fingerprint",
        len(fps) == 2 and fps[0] == fps[1],
        str(fps),
    )
    fps = [b(dev(19, s))[0]["schema_fp"] for s in "ab" if b(dev(19, s))]
    check(
        19,
        "int vs float same fingerprint",
        len(fps) == 2 and fps[0] == fps[1],
        str(fps),
    )

    print("\nBRONZE FAILURES")
    check(
        21,
        "wrong sha256",
        t.reason(dev(21)) == "sha256_mismatch",
        str(t.reason(dev(21))),
    )
    check(
        22,
        "reference to a missing object",
        t.reason(dev(22)) == "object_missing",
        str(t.reason(dev(22))),
    )
    for case, name in (
        (23, "job is not a descriptor"),
        (24, "entry without a job field"),
        (25, "size_bytes doesn't match data"),
        (26, "received_at without timezone"),
    ):
        reason = t.reason_by_job(sent[case])
        check(case, name, reason == "unparseable_descriptor", str(reason))
    record(
        "27",
        "SKIP",
        "object deleted before bronze reads it",
        "needs the bronze worker stopped",
    )

    print("\nRESOLUTION")
    got = {
        "a": [r["mapper_id"] for r in t.of(t.air, dev(28, "a"))],
        "b": [r["mapper_id"] for r in t.of(t.air, dev(28, "b"))],
        "c": [r["mapper_id"] for r in t.of(t.energy, dev(28, "c"))],
        "d": [r["mapper_id"] for r in t.of(t.energy, sent["28d"])],
    }
    want = {
        "a": ["indoor-air-quality"],
        "b": ["vendor-b-airquality"],
        "c": ["iot-energy"],
        "d": ["vendor-c-energy"],
    }
    check(28, "four shapes, no hint, right mapper each", got == want, str(got))
    check(
        29,
        "correct hint",
        [r["mapper_id"] for r in t.of(t.air, dev(29))] == ["indoor-air-quality"],
    )
    check(
        30,
        "unknown hint falls back to keys",
        [r["mapper_id"] for r in t.of(t.air, dev(30))] == ["indoor-air-quality"],
    )
    rows = t.of(t.air, dev(31))
    if rows:
        finding(
            31,
            "wrong hint (energy payload, air hint)",
            True,
            f"silver.air_quality row with temperature={rows[0]['temperature']}, humidity={rows[0]['humidity']}",
        )
    else:
        finding(
            31,
            "wrong hint (energy payload, air hint)",
            False,
            f"no air row; energy rows={len(t.of(t.energy, dev(31)))}, dead letter={t.reason(dev(31))}",
        )
    check(
        32, "unknown shape", t.reason(dev(32)) == "unresolved", str(t.reason(dev(32)))
    )
    where = [
        n
        for n, rows in (("silver.air_quality", t.air), ("silver.energy", t.energy))
        if t.of(rows, dev(33))
    ]
    finding(
        33,
        "payload matching two mappers",
        True,
        f"went to {where or 'nowhere'}: first mapper in load order wins, the other table gets nothing",
    )
    check(
        35,
        "only one of two required keys",
        t.reason(dev(35)) == "unresolved",
        str(t.reason(dev(35))),
    )

    print("\nTRANSFORM")
    edges = [len(t.of(t.air, dev(36, s))) for s in "ab"]
    check(36, "temp at -40 and at 85 accepted", edges == [1, 1], str(edges))
    check(37, "temp 85.1", t.reason(dev(37)) == "out_of_range", str(t.reason(dev(37))))
    check(
        38,
        "humidity 1.5 -> 150",
        t.reason(dev(38)) == "out_of_range",
        str(t.reason(dev(38))),
    )
    rows = t.of(t.air, dev(39))
    check(39, 'temp as string "21.4"', bool(rows) and rows[0]["temperature"] == 21.4)
    check(40, 'temp "hot"', t.reason(dev(40)) == "bad_value", str(t.reason(dev(40))))
    rows = t.of(t.air, dev(41))
    check(41, "temp null -> NULL column", bool(rows) and rows[0]["temperature"] is None)
    rows = t.of(t.air, dev(42))
    if rows:
        finding(
            42, "temp true", True, f"stored as temperature={rows[0]['temperature']}"
        )
    else:
        finding(42, "temp true", False, f"rejected: {t.reason(dev(42))}")
    check(
        43,
        "vendor C without 'meter' uses the header device id",
        len(t.of(t.energy, dev(43))) == 1,
    )
    check(44, "extra fields ignored", len(t.of(t.air, dev(44))) == 1)

    print("\nDEDUPLICATION")
    check(
        45,
        "same reading twice, back to back",
        (len(b(dev(45))), len(t.of(t.air, dev(45)))) == (2, 1),
        f"bronze={len(b(dev(45)))} silver={len(t.of(t.air, dev(45)))}",
    )
    counts = [len(t.of(t.air, dev(47, s))) for s in "ab"]
    check(47, "same reading, two devices", counts == [1, 1], str(counts))
    rows = t.of(t.air, dev(48))
    finding(
        48,
        "same device + time, different values",
        len(rows) == 1,
        f"{len(rows)} row(s), temperature kept = {[r['temperature'] for r in rows]} (sent 20.0 then 30.0)",
    )
    rows = t.of(t.air, dev(49))
    finding(
        49,
        "same device + time, two tenants",
        len(rows) == 1,
        f"{len(rows)} row(s), tenants kept = {[r['tenant_id'][-1] for r in rows]} (sent A then B)",
    )
    rows = t.of(t.air, dev(50))
    finding(
        50,
        "no timestamp, sent twice",
        len(rows) == 2,
        f"{len(rows)} row(s): each gets its own received_at, so they never match",
    )


def phase_dedup_across_jobs() -> None:
    post(dev(46), air(46))
    drain()
    t = Tables()
    count = len(t.of(t.air, dev(46)))
    check(
        46,
        "same reading again after the first was committed",
        count == 1,
        f"silver={count}, bronze={len(t.of(t.bronze, dev(46)))}",
    )


def phase_burst() -> None:
    print("\nBURST")
    bronze = get_catalog().load_table("bronze.raw")
    before = {s.snapshot_id for s in bronze.snapshots()}
    for i in range(250):
        post(
            f"t{RUN}-c20-{i:03d}",
            air(20, i % 60) | {"ts": iso(BASE + timedelta(hours=2, seconds=i))},
        )
    drained = drain(240)
    bronze.refresh()
    sizes = [
        int(s.summary["added-records"])
        for s in bronze.snapshots()
        if s.snapshot_id not in before
    ]
    t = Tables()
    in_bronze = len([r for r in t.bronze if "-c20-" in r["device_id"]])
    in_silver = len([r for r in t.air if "-c20-" in r["device_id"]])
    check(
        20,
        "250 readings: no batch over 100, none lost",
        drained
        and in_bronze == 250
        and in_silver == 250
        and max(sizes, default=0) <= 100,
        f"bronze={in_bronze} silver={in_silver} batches={sizes}",
    )


def phase_poison() -> None:
    print("\nPOISON PAYLOADS")

    # valid JSON that isn't an object
    bronze_was_alive = running("bronze")
    post(dev(12), [1, 2, 3])
    time.sleep(25)
    pending = vk.xpending("tasks:bronze", "bronze-workers")["pending"]
    if (bronze_was_alive and not running("bronze")) or pending:
        finding(
            12,
            "JSON array body",
            True,
            f"bronze worker crashed; entry left pending={pending} and would crash it again on restart, so acking it now",
        )
        for entry in vk.xpending_range("tasks:bronze", "bronze-workers", "-", "+", 10):
            vk.xack("tasks:bronze", "bronze-workers", entry["message_id"])
        print("       (restart the bronze worker before the next case can be checked)")
        return
    t = Tables()
    check(12, "JSON array lands in bronze", len(t.of(t.bronze, dev(12))) == 1)

    was_alive = running("silver")
    post(dev(11), "hello, not json")
    time.sleep(30)
    t = Tables()
    rows = t.of(t.bronze, dev(11))
    check(
        11,
        "non-JSON body lands in bronze",
        bool(rows) and rows[0]["payload"] == "hello, not json",
    )

    pending = vk.xpending("tasks:silver", "silver-workers")["pending"]
    crashed = was_alive and not running("silver")
    if crashed or pending:
        finding(
            34,
            "non-JSON row reaching silver",
            True,
            f"silver worker {'crashed' if crashed else 'is stuck'}; job left pending={pending}. It would crash again on every restart, so acking it now.",
        )
        for entry in vk.xpending_range("tasks:silver", "silver-workers", "-", "+", 10):
            vk.xack("tasks:silver", "silver-workers", entry["message_id"])
    else:
        finding(
            34,
            "non-JSON row reaching silver",
            False,
            f"handled; dead letter reason = {t.reason(dev(11))}",
        )


def main() -> None:
    print(f"run {RUN}  (devices t{RUN}-cNN, tenant {TENANT})")
    phase_api()
    sent = phase_pipeline()
    print("waiting for bronze and silver to drain ...")
    if not drain():
        print(
            "  !! pipeline did not drain within the timeout; results below may be incomplete"
        )
    check_pipeline(sent)
    phase_dedup_across_jobs()
    phase_burst()
    if "--poison" in sys.argv:
        phase_poison()
    else:
        record("11", "SKIP", "non-JSON body lands in bronze", "run with --poison")
        record(
            "34",
            "SKIP",
            "non-JSON row reaching silver",
            "run with --poison (may crash the silver worker)",
        )

    tally: dict[str, int] = {}
    for _, status, _, _ in results:
        tally[status] = tally.get(status, 0) + 1
    print("\n" + "  ".join(f"{k}: {v}" for k, v in sorted(tally.items())))


if __name__ == "__main__":
    main()
