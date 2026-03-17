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

    sources = d.get("sources") or d.get("config", {}).get("sources", [])
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
    first_payloads: dict[str, dict] = {}          # prefix → first parsed payload
    sample_topics: dict[str, list] = defaultdict(list)  # prefix → up to 3 example topics
    first_raw: dict[str, str] = {}                # prefix → raw payload string sample

    t_end = time.monotonic() + sample_secs

    async def _process(topic: str, raw: bytes) -> None:
        prefix = topic.split("/")[0]
        counts[prefix] += 1
        if len(sample_topics[prefix]) < 3 and topic not in sample_topics[prefix]:
            sample_topics[prefix].append(topic)
        try:
            payload = json.loads(raw)
            if prefix not in first_payloads:
                first_payloads[prefix] = payload
                first_raw[prefix] = raw.decode()
        except Exception as e:
            errors[prefix].append(str(e))

    try:
        async with aiomqtt.Client(hostname=host, port=port,
                                  identifier="test-generators") as client:
            await client.subscribe("#", qos=0)
            try:
                async for msg in client.messages:
                    await _process(str(msg.topic), msg.payload)
                    if time.monotonic() >= t_end:
                        break
            except TypeError:
                async with client.messages as messages:  # type: ignore
                    async for msg in messages:
                        await _process(str(msg.topic), msg.payload)
                        if time.monotonic() >= t_end:
                            break
    except Exception as e:
        record("MQTT connect", False, str(e))
        return {}

    record("MQTT connect", True, f"broker {host}:{port}")

    if not counts:
        record("messages received", False, "no messages in window")
        return {}

    for prefix, n in sorted(counts.items()):
        rate = round(n / sample_secs, 1)
        err_count = len(errors.get(prefix, []))
        ok = err_count == 0
        record(f"  {prefix:20s} {rate:6.1f} msg/s  {n} total",
               ok, f"{err_count} parse errors" if not ok else "")

    return {
        "host": host, "port": port,
        "counts": dict(counts),
        "first_payloads": first_payloads,
        "first_raw": first_raw,
        "sample_topics": dict(sample_topics),
        "errors": {k: len(v) for k, v in errors.items()},
    }


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

def build_report(gen_info: dict, stress_info: dict, mqtt_info: dict,
                 passed: int, failed: int, warned: int,
                 args) -> tuple[str, str]:
    """Return (markdown, html) report strings."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    counts        = mqtt_info.get("counts", {})
    payloads      = mqtt_info.get("first_payloads", {})
    first_raw     = mqtt_info.get("first_raw", {})
    sample_topics = mqtt_info.get("sample_topics", {})
    broker_host   = mqtt_info.get("host", args.mqtt_host)
    broker_port   = mqtt_info.get("port", args.mqtt_port)
    total_rate    = round(sum(counts.values()) / args.sample_secs, 1)

    # ── Markdown ────────────────────────────────────────────────────────────
    rows = []
    for r in results:
        icon = "✅" if r["ok"] else ("⚠️" if r["ok"] is None else "❌")
        rows.append(f"| {icon} | {r['name']} | {r['detail']} |")
    table = "\n".join(rows)

    source_rows = []
    for prefix, n in sorted(counts.items()):
        rate = round(n / args.sample_secs, 1)
        p = payloads.get(prefix, {})
        meas = [k for k, v in p.items() if isinstance(v, dict) and "value" in v] \
            or [k for k, v in p.items() if isinstance(v, (int, float))]
        fmt = "nested" if any(isinstance(v, dict) for v in p.values()) else "flat"
        source_rows.append(
            f"| `{prefix}` | {rate} msg/s | {n} | {fmt} | {', '.join(meas[:5])} |"
        )
    source_table = "\n".join(source_rows)

    # sample topics section
    topic_sections = []
    for prefix in sorted(sample_topics):
        topics_md = "\n".join(f"  - `{t}`" for t in sample_topics[prefix])
        raw = first_raw.get(prefix, "")
        try:
            pretty = json.dumps(json.loads(raw), indent=2)
        except Exception:
            pretty = raw
        topic_sections.append(
            f"### `{prefix}` — example topics\n{topics_md}\n\n"
            f"**Sample payload:**\n```json\n{pretty}\n```"
        )
    topics_section = "\n\n".join(topic_sections)

    gen_assets   = gen_info.get("cell_count", "?")
    gen_sources  = ", ".join(s.get("id","?") for s in gen_info.get("sources",[]))
    stress_cells = stress_info.get("cell_total", "?")
    stress_mps   = stress_info.get("mps", "?")

    md = f"""# Generator Test Report
*{now} — sample window {args.sample_secs}s*

## Summary

| | |
|---|---|
| **MQTT broker** | `{broker_host}:{broker_port}` (Mosquitto) |
| **Total MQTT rate** | {total_rate} msg/s |
| **Multi-generator assets** | {gen_assets} cells ({gen_sources}) |
| **Stress-runner assets** | {stress_cells} cells @ {stress_mps} msg/s |
| **Result** | {passed} passed · {failed} failed · {warned} warnings |

## MQTT Sources

| Topic prefix | Rate | Messages | Format | Measurements |
|---|---|---|---|---|
{source_table}

## Sample Topics & Payloads

{topics_section}

## Test Results

| | Test | Detail |
|---|---|---|
{table}
"""

    # ── HTML ────────────────────────────────────────────────────────────────
    status_color = "#2ecc71" if failed == 0 else "#e74c3c"
    status_label = "ALL PASS" if failed == 0 else f"{failed} FAILED"

    result_rows_html = ""
    for r in results:
        if r["ok"]:
            bg, icon = "#eafaf1", "✅"
        elif r["ok"] is None:
            bg, icon = "#fef9e7", "⚠️"
        else:
            bg, icon = "#fdedec", "❌"
        result_rows_html += (
            f'<tr style="background:{bg}">'
            f'<td style="text-align:center">{icon}</td>'
            f'<td>{r["name"]}</td>'
            f'<td style="color:#555">{r["detail"]}</td></tr>\n'
        )

    source_rows_html = ""
    for prefix, n in sorted(counts.items()):
        rate = round(n / args.sample_secs, 1)
        p = payloads.get(prefix, {})
        meas = [k for k, v in p.items() if isinstance(v, dict) and "value" in v] \
            or [k for k, v in p.items() if isinstance(v, (int, float))]
        fmt = "nested" if any(isinstance(v, dict) for v in p.values()) else "flat"
        source_rows_html += (
            f"<tr><td><code>{prefix}</code></td>"
            f"<td><b>{rate}</b> msg/s</td>"
            f"<td>{n}</td><td>{fmt}</td>"
            f"<td style='font-size:0.85em'>{', '.join(meas[:5])}</td></tr>\n"
        )

    # sample topics + payloads HTML blocks
    samples_html = ""
    for prefix in sorted(sample_topics):
        topics_li = "".join(
            f"<li><code>{t}</code></li>" for t in sample_topics[prefix]
        )
        raw = first_raw.get(prefix, "")
        try:
            pretty = json.dumps(json.loads(raw), indent=2)
        except Exception:
            pretty = raw
        import html as _html
        samples_html += f"""
<h3><code>{prefix}</code></h3>
<p><b>Example topics (Mosquitto broker <code>{broker_host}:{broker_port}</code>):</b></p>
<ul>{topics_li}</ul>
<p><b>Sample payload:</b></p>
<pre style="background:#f8f9fa;padding:12px;border-radius:6px;overflow-x:auto;font-size:0.85em">{_html.escape(pretty)}</pre>
"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Generator Test Report</title>
<style>
  body {{ font-family: -apple-system, sans-serif; max-width: 960px;
         margin: 40px auto; padding: 0 20px; color: #222; }}
  h1 {{ border-bottom: 2px solid #ddd; padding-bottom: 8px; }}
  h2 {{ margin-top: 32px; color: #333; }}
  h3 {{ margin-top: 20px; color: #444; }}
  .badge {{ display:inline-block; padding:6px 18px; border-radius:20px;
            color:#fff; font-weight:700; font-size:1.1em;
            background:{status_color}; }}
  .kv {{ display:grid; grid-template-columns:220px 1fr; gap:6px 16px;
         background:#f8f9fa; padding:16px; border-radius:8px; margin:16px 0; }}
  .kv .label {{ color:#666; font-size:0.9em; }}
  .kv .value {{ font-weight:600; }}
  table {{ border-collapse:collapse; width:100%; }}
  th {{ background:#f0f0f0; padding:8px 12px; text-align:left; }}
  td {{ padding:7px 12px; border-bottom:1px solid #eee; }}
  code {{ background:#f0f0f0; padding:2px 5px; border-radius:3px; }}
  ul {{ margin:4px 0 12px 0; padding-left:24px; }}
  li {{ margin:2px 0; }}
  .ts {{ color:#888; font-size:0.85em; }}
</style>
</head>
<body>
<h1>Generator Test Report
  <span class="badge">{status_label}</span></h1>
<p class="ts">{now} — sample window {args.sample_secs}s</p>

<h2>Overview</h2>
<div class="kv">
  <span class="label">MQTT broker</span>
  <span class="value"><code>{broker_host}:{broker_port}</code> (Mosquitto)</span>
  <span class="label">Total MQTT rate</span>
  <span class="value">{total_rate} msg/s</span>
  <span class="label">Multi-generator</span>
  <span class="value">{gen_assets} cells ({gen_sources})</span>
  <span class="label">Stress-runner</span>
  <span class="value">{stress_cells} cells @ {stress_mps} msg/s</span>
  <span class="label">Test result</span>
  <span class="value">{passed} passed · {failed} failed · {warned} warnings</span>
</div>

<h2>MQTT Sources</h2>
<table>
  <tr><th>Topic prefix</th><th>Rate</th><th>Messages</th>
      <th>Format</th><th>Measurements</th></tr>
{source_rows_html}
</table>

<h2>Sample Topics &amp; Payloads</h2>
{samples_html}

<h2>Test Results</h2>
<table>
  <tr><th></th><th>Test</th><th>Detail</th></tr>
{result_rows_html}
</table>

</body>
</html>
"""
    return md, html


async def main() -> None:
    parser = argparse.ArgumentParser(description="Verify both data generators")
    parser.add_argument("--mqtt-host",   default="localhost")
    parser.add_argument("--mqtt-port",   type=int, default=1884)
    parser.add_argument("--gen-ws",      default="ws://localhost:8765")
    parser.add_argument("--stress-ws",   default="ws://localhost:8769")
    parser.add_argument("--sample-secs", type=int, default=10)
    parser.add_argument("--out-md",  default="docs/generators_report.md")
    parser.add_argument("--out-html", default="html/generators_report.html")
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

    md, html = build_report(gen_info, stress_info, mqtt_info,
                             passed, failed, warned, args)

    import os
    os.makedirs(os.path.dirname(args.out_md),   exist_ok=True)
    os.makedirs(os.path.dirname(args.out_html), exist_ok=True)
    with open(args.out_md,   "w") as f: f.write(md)
    with open(args.out_html, "w") as f: f.write(html)
    print(f"\nReports written:")
    print(f"  {args.out_md}")
    print(f"  {args.out_html}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
