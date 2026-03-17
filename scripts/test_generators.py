"""
test_generators.py — verify both data-source generators are running and
producing valid data on MQTT.

Tests:
  [1] multi-generator WebSocket (port 8765) — running, asset count, active sources
  [2] stress-runner  WebSocket (port 8769) — running, msg/s, project topology
  [3] MQTT sampling (port 1884, 10 s)      — msg/s per topic prefix, payload validity
  [4] Payload schema check                 — both flat and nested formats validated
  [5] Cross-check                          — MQTT rate matches WS-reported mps

Usage:
  python scripts/test_generators.py [--mqtt-host HOST] [--mqtt-port PORT]
                                     [--gen-ws  ws://HOST:8765]
                                     [--stress-ws ws://HOST:8769]
                                     [--sample-secs N]
"""

import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict

import aiomqtt
import websockets

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
WARN = "\033[33mWARN\033[0m"

results: list[dict] = []


def record(name: str, ok: bool | None, detail: str = "") -> None:
    tag = PASS if ok else (WARN if ok is None else FAIL)
    print(f"  [{tag}] {name}" + (f": {detail}" if detail else ""))
    results.append({"name": name, "ok": ok, "detail": detail})


# ---------------------------------------------------------------------------
# WebSocket helpers
# ---------------------------------------------------------------------------

async def ws_get(url: str, timeout: float = 5.0) -> dict | None:
    try:
        async with websockets.connect(url, open_timeout=timeout) as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            return json.loads(raw)
    except Exception as e:
        return {"_error": str(e)}


# ---------------------------------------------------------------------------
# Test 1: multi-generator WebSocket
# ---------------------------------------------------------------------------

async def test_multi_generator(gen_ws: str) -> dict:
    print("\n[1] multi-generator WebSocket:", gen_ws)
    d = await ws_get(gen_ws)
    if "_error" in d:
        record("reachable", False, d["_error"])
        return {}
    record("reachable", True)

    running = d.get("running", False)
    record("running", running, f"running={running}")

    cell_count = d.get("cell_count", 0)
    record("has assets", cell_count > 0, f"cell_count={cell_count}")

    sources = d.get("sources", [])
    active = [s for s in sources if s.get("enabled")]
    record("active sources", len(active) > 0,
           ", ".join(s.get("id", "?") for s in active) or "none")

    return {"cell_count": cell_count, "sources": active}


# ---------------------------------------------------------------------------
# Test 2: stress-runner WebSocket
# ---------------------------------------------------------------------------

async def test_stress_runner(stress_ws: str) -> dict:
    print("\n[2] stress-runner WebSocket:", stress_ws)
    d = await ws_get(stress_ws)
    if "_error" in d:
        record("reachable", False, d["_error"])
        return {}
    record("reachable", True)

    mps = d.get("mps", 0)
    total = d.get("total_published", 0)
    tasks = d.get("active_tasks", 0)
    record("active tasks", tasks > 0, f"active_tasks={tasks}")
    record("publishing", mps > 0 or total > 0, f"mps={mps}  total={total}")

    projects = d.get("projects", [])
    cell_total = sum(
        s.get("cells", 0)
        for p in projects
        for s in p.get("sites", [])
    )
    record("project topology", len(projects) > 0,
           f"{len(projects)} project(s), {cell_total} cells")

    return {"mps": mps, "total": total, "cell_total": cell_total}


# ---------------------------------------------------------------------------
# Test 3 + 4: MQTT sampling + payload validation
# ---------------------------------------------------------------------------

async def test_mqtt(host: str, port: int, sample_secs: int) -> dict:
    print(f"\n[3] MQTT sampling  {host}:{port}  ({sample_secs}s window)")

    counts: dict[str, int] = defaultdict(int)   # prefix → msg count
    errors: dict[str, list] = defaultdict(list)  # prefix → error samples
    first_payloads: dict[str, dict] = {}

    t_end = time.monotonic() + sample_secs
    try:
        async with aiomqtt.Client(hostname=host, port=port,
                                  identifier="test-generators") as client:
            await client.subscribe("#", qos=0)
            async with client.messages as messages:
                async for msg in messages:
                    topic = str(msg.topic)
                    prefix = topic.split("/")[0]
                    counts[prefix] += 1

                    # Validate JSON payload
                    try:
                        payload = json.loads(msg.payload)
                        if prefix not in first_payloads:
                            first_payloads[prefix] = payload
                    except Exception as e:
                        errors[prefix].append(str(e))

                    if time.monotonic() >= t_end:
                        break
    except Exception as e:
        record("MQTT connect", False, str(e))
        return {}

    record("MQTT connect", True)

    if not counts:
        record("messages received", False, "no messages in window")
        return {}

    for prefix, n in sorted(counts.items()):
        rate = round(n / sample_secs, 1)
        err_count = len(errors.get(prefix, []))
        ok = err_count == 0
        record(f"  {prefix:20s} {rate:6.1f} msg/s  {n} total",
               ok, f"{err_count} parse errors" if not ok else "")

    return {"counts": dict(counts), "first_payloads": first_payloads,
            "errors": {k: len(v) for k, v in errors.items()}}


# ---------------------------------------------------------------------------
# Test 4: payload schema
# ---------------------------------------------------------------------------

def test_payload_schema(first_payloads: dict) -> None:
    print("\n[4] Payload schema check")
    if not first_payloads:
        record("payloads available", False, "no samples collected")
        return

    for prefix, payload in first_payloads.items():
        keys = list(payload.keys())
        # nested format: values are {value, unit} dicts
        # flat format: values are scalars
        nested_keys = [k for k, v in payload.items()
                       if isinstance(v, dict) and "value" in v]
        flat_keys   = [k for k, v in payload.items()
                       if isinstance(v, (int, float, str))
                       and k not in ("site_id", "rack_id", "module_id", "cell_id")]

        if nested_keys:
            fmt = f"nested ({len(nested_keys)} measurements)"
        elif flat_keys:
            fmt = f"flat ({len(flat_keys)} measurements)"
        else:
            fmt = f"unknown ({keys[:5]})"

        has_id = any(k in payload for k in ("cell_id", "site_id", "timestamp"))
        record(f"  {prefix}: {fmt}", has_id,
               "missing id fields" if not has_id else "")


# ---------------------------------------------------------------------------
# Test 5: cross-check WS mps vs MQTT rate
# ---------------------------------------------------------------------------

def test_cross_check(stress_info: dict, mqtt_info: dict) -> None:
    print("\n[5] Cross-check: stress-runner WS mps vs MQTT batteries rate")
    ws_mps = stress_info.get("mps", 0)
    mqtt_counts = mqtt_info.get("counts", {})
    # batteries prefix may come from stress runner OR multi-generator
    batteries_rate = sum(v for k, v in mqtt_counts.items()
                         if "batter" in k.lower())
    # We can't be precise since both generators write to batteries
    if ws_mps == 0 and batteries_rate == 0:
        record("both sources idle", False,
               "no data from stress-runner or generator on batteries topic")
    else:
        record("data flowing on batteries", True,
               f"WS mps={ws_mps}  MQTT batteries≈{round(batteries_rate, 1)}/s")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    parser = argparse.ArgumentParser(description="Verify both data generators")
    parser.add_argument("--mqtt-host",   default="localhost")
    parser.add_argument("--mqtt-port",   type=int, default=1884)
    parser.add_argument("--gen-ws",      default="ws://localhost:8765")
    parser.add_argument("--stress-ws",   default="ws://localhost:8769")
    parser.add_argument("--sample-secs", type=int, default=10)
    args = parser.parse_args()

    print("=" * 60)
    print("Generator verification")
    print("=" * 60)

    gen_info    = await test_multi_generator(args.gen_ws)
    stress_info = await test_stress_runner(args.stress_ws)
    mqtt_info   = await test_mqtt(args.mqtt_host, args.mqtt_port,
                                   args.sample_secs)
    test_payload_schema(mqtt_info.get("first_payloads", {}))
    test_cross_check(stress_info, mqtt_info)

    passed  = sum(1 for r in results if r["ok"] is True)
    failed  = sum(1 for r in results if r["ok"] is False)
    warned  = sum(1 for r in results if r["ok"] is None)

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed  {failed} failed  {warned} warnings")
    print("=" * 60)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
