#!/usr/bin/env python3
"""
Schema comparison benchmark: Normalized Long vs Long+compound vs Wide (pivot).
12 units × 34 signals (24 float + 10 int), 1 Hz for 600 simulated seconds.
All writers subscribe to the same MQTT stream simultaneously.

Options:
  --cov         Simulate change-on-value: floats publish only when value
                drifts >1% from last published; integers/booleans only on change.
                Produces realistic gaps to exercise forward-fill queries.
"""
import subprocess, time, os, sys, json, shutil, signal, random, argparse
import paho.mqtt.client as mqtt
import pyarrow.parquet as pq
import pyarrow as pa

parser = argparse.ArgumentParser()
parser.add_argument("--cov", action="store_true",
                    help="Change-on-value: skip publishing unchanged signals")
parser.add_argument("--sweeps", type=int, default=600,
                    help="Number of sweeps to run (default 600)")
parser.add_argument("--writer", default=None,
                    help="Path to parquet_writer binary")
parser.add_argument("--cfgdir", default="/tmp/bench2",
                    help="Directory containing config YAML files (default /tmp/bench2)")
parser.add_argument("--outdir", default=None,
                    help="Base output directory (default: parent of cfgdir)")
parser.add_argument("--results", default=None,
                    help="Path to write results.json")
parser.add_argument("--site", default="SITE_A",
                    help="Site ID embedded in parquet partitioning (default SITE_A)")
parser.add_argument("--mqtt-host", default="localhost",
                    help="MQTT broker host (default localhost)")
parser.add_argument("--mqtt-port", type=int, default=1883,
                    help="MQTT broker port (default 1883)")
args = parser.parse_args()

# Resolve paths — support both /home/phil/work/gen-ai/data_server (dev)
# and /home/phil/data_server (.34 / lp3 style) automatically
_self_dir  = os.path.dirname(os.path.abspath(__file__))
_repo_root = os.path.abspath(os.path.join(_self_dir, '..', '..'))

def _default_writer():
    candidate = os.path.join(_self_dir, 'parquet_writer')
    if os.path.isfile(candidate):
        return candidate
    raise FileNotFoundError(f"parquet_writer binary not found at {candidate}. "
                             "Build with: cd source/parquet_writer && make")

WRITER     = args.writer or _default_writer()
_cfgdir    = args.cfgdir
_outbase   = args.outdir or os.path.dirname(_cfgdir)

CFG_NORM_LONG  = os.path.join(_cfgdir, "config_normalized_long_bench.yaml")
CFG_LONG_CMP   = os.path.join(_cfgdir, "config_long_compound.yaml")
CFG_WIDE_PIVOT = os.path.join(_cfgdir, "config_wide_pivot.yaml")
OUT_NORM_LONG  = os.path.join(_outbase, "bench-norm-long")
OUT_LONG_CMP   = os.path.join(_outbase, "bench-long-cmp")
OUT_WIDE_PIVOT = os.path.join(_outbase, "bench-wide")
RESULTS_FILE   = args.results or os.path.join(_cfgdir, "results.json")
SITE_ID        = args.site
MQTT_HOST      = args.mqtt_host
MQTT_PORT      = args.mqtt_port

# Rewrite configs with correct output paths and site_id at runtime
import yaml as _yaml

def _write_cfg(src_path, out_path, overrides):
    """Load a YAML config, apply overrides dict to output section, write back."""
    with open(src_path) as f:
        cfg = _yaml.safe_load(f)
    for k, v in overrides.items():
        cfg['output'][k] = v
    cfg['mqtt']['host'] = MQTT_HOST
    cfg['mqtt']['port'] = MQTT_PORT
    with open(src_path, 'w') as f:
        _yaml.dump(cfg, f, default_flow_style=False)

if os.path.isfile(CFG_NORM_LONG):
    _write_cfg(CFG_NORM_LONG, CFG_NORM_LONG,
               {'base_path': OUT_NORM_LONG, 'site_id': SITE_ID})
if os.path.isfile(CFG_LONG_CMP):
    _write_cfg(CFG_LONG_CMP,  CFG_LONG_CMP,
               {'base_path': OUT_LONG_CMP,  'site_id': SITE_ID})
if os.path.isfile(CFG_WIDE_PIVOT):
    _write_cfg(CFG_WIDE_PIVOT, CFG_WIDE_PIVOT,
               {'base_path': OUT_WIDE_PIVOT, 'site_id': SITE_ID})

UNITS = [f"{0x0215D1D8 + i:08X}" for i in range(12)]
FLOAT_SIGNALS = (
    [("bms", "bms_1", f"Batt1_Cell{c}_Voltage",    "float") for c in range(1, 11)] +
    [("bms", "bms_1", f"Batt1_Cell{c}_Temperature", "float") for c in range(1, 6)]  +
    [("bms", "bms_1", "Pack_Current",                "float"),
     ("bms", "bms_1", "Pack_Voltage",                "float"),
     ("bms", "bms_1", "Pack_SOC",                    "float"),
     ("bms", "bms_1", "Pack_SOH",                    "float"),
     ("pcs", "pcs_1", "GridFrequency",               "float"),
     ("pcs", "pcs_1", "ActivePower",                 "float"),
     ("pcs", "pcs_1", "ReactivePower",               "float"),
     ("pcs", "pcs_1", "DCBusVoltage",                "float"),
     ("pcs", "pcs_1", "OutputCurrent",               "float")]
)
INT_SIGNALS = [
    ("bms", "bms_1", "Pack_ContactorState",  "integer"),
    ("bms", "bms_1", "Batt1_CellBalancing",  "integer"),
    ("bms", "bms_1", "BMS_FaultCode",        "integer"),
    ("bms", "bms_1", "BMS_WarningCode",      "integer"),
    ("pcs", "pcs_1", "PCS_State",            "integer"),
    ("pcs", "pcs_1", "PCS_FaultCode",        "integer"),
    ("pcs", "pcs_1", "GridConnected",        "integer"),
    ("pcs", "pcs_1", "AlarmActive",          "integer"),
    ("rack","rack_1","Rack_FanState",        "integer"),
    ("rack","rack_1","Rack_DoorOpen",        "integer"),
]
ALL_SIGNALS  = FLOAT_SIGNALS + INT_SIGNALS
SWEEPS       = args.sweeps
SIGNALS_PS   = len(UNITS) * len(ALL_SIGNALS)
TOTAL_MSGS   = SWEEPS * SIGNALS_PS
SIM_DURATION = SWEEPS

# COV thresholds: floats publish only when value drifts >1% from last sent;
# integers only when the value changes.
COV_FLOAT_PCT = 0.01
last_pub = {}   # (unit, device, instance, signame) → last published value

def should_publish(unit, device, instance, signame, dtype, val):
    if not args.cov:
        return True
    key = (unit, device, instance, signame)
    prev = last_pub.get(key)
    if prev is None:
        last_pub[key] = val
        return True
    if dtype == "float":
        if abs(val - prev) / max(abs(prev), 1e-6) > COV_FLOAT_PCT:
            last_pub[key] = val
            return True
        return False
    else:  # integer — publish only on change
        if val != prev:
            last_pub[key] = val
            return True
        return False

print(f"=== Schema Benchmark: Normalized Long vs Long+compound vs Wide ===\n")
print(f"Topology : {len(UNITS)} units × {len(ALL_SIGNALS)} signals "
      f"({len(FLOAT_SIGNALS)} float + {len(INT_SIGNALS)} int)")
print(f"Sweeps   : {SWEEPS:,}  ({SIM_DURATION}s simulated at 1 Hz)")
print(f"Messages : {TOTAL_MSGS:,}  (max; COV={'on' if args.cov else 'off'})\n")

for d in (OUT_NORM_LONG, OUT_LONG_CMP, OUT_WIDE_PIVOT):
    if os.path.exists(d): shutil.rmtree(d)

print("[1] Starting writers...")
procs = {
    "norm-long":  subprocess.Popen([WRITER, "--config", CFG_NORM_LONG],
                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True),
    "long+cmp":   subprocess.Popen([WRITER, "--config", CFG_LONG_CMP],
                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True),
    "wide-pivot": subprocess.Popen([WRITER, "--config", CFG_WIDE_PIVOT],
                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True),
}
time.sleep(1.2)

print(f"[2] Publishing {TOTAL_MSGS:,} messages...")
client = mqtt.Client(client_id="bench2-publisher")
client.connect(MQTT_HOST, MQTT_PORT)
client.loop_start()
time.sleep(0.3)

t0 = time.time()
base_ts = 1743494400.0   # 2026-04-01T10:00:00Z
pub_count = 0

# Each sweep publishes all N_MSGS in a random burst of BURST_MIN–BURST_MAX ms.
# Crucially: each message's payload ts is derived from the simulated clock
# (base_ts + sweep + intra-burst offset), so ts values are monotonically
# increasing and each message has a distinct timestamp within the burst.
# The writer's time_window_ms: 250 sees real per-message ts spread and must
# decide whether to coalesce (burst ≤ 250 ms) or open a new window.
BURST_MIN_MS = 50    # ms — minimum burst spread per sweep
BURST_MAX_MS = 200   # ms — maximum burst spread (< 250ms window)
SWEEP_PERIOD = 1.0   # seconds — 1 Hz inter-sweep interval
N_MSGS       = len(UNITS) * len(ALL_SIGNALS)

def fmt_ts(unix_s):
    """Format a float unix timestamp as ISO-8601 UTC with ms precision."""
    import datetime
    dt = datetime.datetime.utcfromtimestamp(unix_s)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

for sweep in range(SWEEPS):
    sweep_wall_start = time.time()
    sweep_base_ts = base_ts + sweep * 1.0   # simulated 1 Hz tick (monotonic)

    # Monotonically increasing intra-burst delays (seconds) for this sweep.
    # Sorted so wall-clock delivery and simulated ts advance together.
    burst_s = random.uniform(BURST_MIN_MS, BURST_MAX_MS) * 1e-3
    delays  = sorted(random.uniform(0, burst_s) for _ in range(N_MSGS))

    msg_idx = 0
    for unit in UNITS:
        for (device, instance, signame, dtype) in ALL_SIGNALS:
            val = round(random.uniform(0.0, 100.0), 4) if dtype == "float" else random.randint(0, 3)

            # Simulated ts = sweep base + intra-burst offset → monotonic, unique per msg
            msg_ts = sweep_base_ts + delays[msg_idx]
            ts_str = fmt_ts(msg_ts)

            # Hold wall clock in sync with simulated ts offset
            target = sweep_wall_start + delays[msg_idx]
            now    = time.time()
            if target > now:
                time.sleep(target - now)

            if should_publish(unit, device, instance, signame, dtype, val):
                topic = f"bench/{unit}/{device}/{instance}/{signame}/{dtype}"
                client.publish(topic, json.dumps({"ts": ts_str, "value": val}), qos=0)
                pub_count += 1
            msg_idx += 1

    # Sleep out the remainder of the 1-second interval
    elapsed_sweep = time.time() - sweep_wall_start
    remainder = SWEEP_PERIOD - elapsed_sweep
    if remainder > 0:
        time.sleep(remainder)

    if sweep % 60 == 0:
        elapsed = time.time() - t0
        rate    = pub_count / max(elapsed, 0.001)
        print(f"   sweep {sweep:5d}/{SWEEPS}  msgs={pub_count:>9,}  rate={rate:>7,.0f}/s  {100*sweep//SWEEPS}%")

elapsed_pub = time.time() - t0
client.loop_stop()
client.disconnect()
print(f"   Done. {pub_count:,} msgs in {elapsed_pub:.1f}s  ({pub_count/elapsed_pub:,.0f} msg/s)\n")

print("[3] Stopping writers (forces final flush)...")
logs = {}
for name, proc in procs.items():
    proc.send_signal(signal.SIGTERM)
for name, proc in procs.items():
    log, _ = proc.communicate(timeout=20)
    logs[name] = log

# ── analysis ───────────────────────────────────────────────────────────────────

def dir_stats(path):
    total, files, rows = 0, [], 0
    for root, _, fnames in os.walk(path):
        for f in fnames:
            if f.endswith(".parquet"):
                fp = os.path.join(root, f)
                sz = os.path.getsize(fp)
                total += sz
                files.append(fp)
    if files:
        t = pa.concat_tables([pq.read_table(f) for f in files])
        rows   = t.num_rows
        schema = t.schema
    else:
        schema = None
    return total, len(files), rows, schema

SCHEMA_KEYS = ["norm-long", "long+cmp", "wide-pivot"]

stats = {
    "norm-long":  dir_stats(OUT_NORM_LONG),
    "long+cmp":   dir_stats(OUT_LONG_CMP),
    "wide-pivot": dir_stats(OUT_WIDE_PIVOT),
}

def flush_ms(log):
    times = []
    for line in log.splitlines():
        if "[flush]" in line and "total=" in line:
            for part in line.split():
                if part.startswith("total=") and part.endswith("ms"):
                    try: times.append(int(part[6:-2]))
                    except: pass
    return times

flush_times = {name: flush_ms(logs[name]) for name in procs}

W = 14
print(f"\n{'='*72}")
print(f"  RESULTS  ({pub_count:,} msgs · {SIM_DURATION}s · {len(UNITS)} units · {len(ALL_SIGNALS)} signals · COV={'on' if args.cov else 'off'})")
print(f"{'='*72}\n")
print(f"{'':28} {'NORM LONG':>{W}}  {'LONG+COMPOUND':>{W}}  {'WIDE (pivot)':>{W}}")
print(f"{'─'*72}")

def row(label, vals, fmt=lambda v: str(v)):
    print(f"{label:28} {fmt(vals[0]):>{W}}  {fmt(vals[1]):>{W}}  {fmt(vals[2]):>{W}}")

sizes  = [stats[k][0] for k in SCHEMA_KEYS]
nfiles = [stats[k][1] for k in SCHEMA_KEYS]
nrows  = [stats[k][2] for k in SCHEMA_KEYS]
ncols  = [len(stats[k][3]) if stats[k][3] else 0 for k in SCHEMA_KEYS]
ftimes = [flush_times[k] for k in SCHEMA_KEYS]

row("Parquet files",       nfiles, fmt=lambda v: f"{v:,}")
row("Rows written",        nrows,  fmt=lambda v: f"{v:,}")
row("Total size (bytes)",  sizes,  fmt=lambda v: f"{v:,}")
row("Total size (KB)",     [s//1024 for s in sizes], fmt=lambda v: f"{v:,}")
row("Schema width (cols)", ncols,  fmt=lambda v: f"{v}")
row("Bytes per row",       [s//max(r,1) for s,r in zip(sizes,nrows)], fmt=lambda v: f"{v}")

print()
for k, sz in zip(SCHEMA_KEYS[1:], sizes[1:]):
    if sizes[0] and sz:
        pct = 100 * (sz - sizes[0]) / sizes[0]
        print(f"  {k} vs norm-long: {pct:+.1f}%")

print(f"\n  Flush times (ms per flush):")
for label, ft in zip(SCHEMA_KEYS, ftimes):
    if ft:
        print(f"    {label:20} max={max(ft)}ms  avg={sum(ft)//len(ft)}ms")
    else:
        print(f"    {label:20} no flushes recorded")

print(f"\n  Schemas:")
for label in SCHEMA_KEYS:
    sz, nf, nr, schema = stats[label]
    print(f"\n  [{label}]  {len(schema) if schema else 0} columns:")
    if schema:
        fields = list(schema)
        for f in fields[:10]:
            print(f"    {f.name:<50} {f.type}")
        if len(fields) > 10:
            print(f"    ... and {len(fields)-10} more signal columns")

print(f"\n  Writer logs (flush lines only):")
for label in SCHEMA_KEYS:
    print(f"\n  [{label}]")
    for line in logs[label].splitlines():
        if "[flush]" in line or "connected" in line:
            print(f"    {line}")

# Save raw results for HTML generation
import json as _json
result = {
    "topology": {"units": len(UNITS), "signals": len(ALL_SIGNALS),
                 "float_signals": len(FLOAT_SIGNALS), "int_signals": len(INT_SIGNALS),
                 "sweeps": SWEEPS, "sim_seconds": SIM_DURATION, "total_msgs": TOTAL_MSGS},
    "norm_long": {
        "size_bytes": sizes[0], "files": nfiles[0], "rows": nrows[0], "cols": ncols[0],
        "flush_ms": ftimes[0],
        "schema": [{"name": f.name, "type": str(f.type)} for f in list(stats["norm-long"][3])] if stats["norm-long"][3] else []
    },
    "long_cmp": {
        "size_bytes": sizes[1], "files": nfiles[1], "rows": nrows[1], "cols": ncols[1],
        "flush_ms": ftimes[1],
        "schema": [{"name": f.name, "type": str(f.type)} for f in list(stats["long+cmp"][3])] if stats["long+cmp"][3] else []
    },
    "wide_pivot": {
        "size_bytes": sizes[2], "files": nfiles[2], "rows": nrows[2], "cols": ncols[2],
        "flush_ms": ftimes[2],
        "schema": [{"name": f.name, "type": str(f.type)} for f in list(stats["wide-pivot"][3])[:40]] if stats["wide-pivot"][3] else []
    },
}
os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
with open(RESULTS_FILE, "w") as fh:
    _json.dump(result, fh, indent=2)
print(f"\n  Results saved to {RESULTS_FILE}")

# ── column_samples.json — sample values for each wide column ─────────────────
try:
    import glob as _glob
    wide_dir = os.path.join(OUTDIR, "bench-wide")
    pq_files = _glob.glob(os.path.join(wide_dir, "**", "*.parquet"), recursive=True)
    if pq_files:
        import pyarrow.parquet as _pq
        # read first file only for samples
        tbl = _pq.read_table(pq_files[0])
        meta_cols = {"ts", "unit_id", "site", "dtype_hint"}
        samples = {}
        for col in tbl.schema:
            if col.name in meta_cols:
                continue
            arr = tbl.column(col.name).drop_null()
            vals = arr[:80].to_pylist()
            samples[col.name] = [round(v, 6) if isinstance(v, float) else v for v in vals]
        samples_file = os.path.join(OUTDIR, "column_samples.json")
        with open(samples_file, "w") as fh:
            _json.dump(samples, fh)
        print(f"  Column samples saved to {samples_file} ({len(samples)} columns)")
    else:
        print("  No wide parquet files found for column samples")
except Exception as e:
    print(f"  WARNING: could not generate column_samples.json: {e}")
