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
import subprocess, time, os, sys, json, shutil, signal, random, argparse, math
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
parser.add_argument("--csv", default=None,
                    help="Path to SBESS3 zip for CSV replay (default: use physics model)")
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

# ── Signals excluded from CSV replay ─────────────────────────────────────────
# Identified from SBESS3 UC 61B5 real data (200-sample analysis):
#   MONOTONIC  — increment every sweep; compress very poorly (entropy = max)
#   CONSTANT   — single unique value across entire run; waste column space
CSV_EXCLUDE = {
    # Monotonic — heartbeats / counters / internal timestamps
    'BMS_SysHB', 'SysHB', 'Counter', 'ItemDeletionTime',
    # Constants — zero analytical value in this dataset
    'AvgCellT', 'BMS_AvgCellT',
    'BMS_ChaCurrLimit', 'BMS_DischCurrLimit',
    'BMS_MaxCellT',
    'BMS_MaxP_Bus1', 'BMS_MaxP_Bus2', 'BMS_MaxP_Bus3',
    'BMS_MinP_Bus1', 'BMS_MinP_Bus2', 'BMS_MinP_Bus3',
    'BMS_RackCount', 'BMS_SysSOH',
    'ChaCurrLimit', 'ConnectingStatus', 'DischCurrLimit',
    'MaxCellT', 'MinCellT', 'RackCount', 'SN', 'SysSOH', 'TS',
    'ACBreaker', 'Bank1Enable', 'Bank2Enable', 'Bank3Enable',
    'DisableEvenBMS', 'DisableOddBMS',
}

# ── CSV replay loader ─────────────────────────────────────────────────────────
def load_csv_replay(zip_path):
    """Parse InfluxDB Flux CSV from SBESS3 zip.
    Returns (signals, values) where:
      signals: list of (device, instance, point_name, dtype) tuples
      values:  dict {(device, instance, point_name, dtype): [float, ...]}
    Only loads bms_1 and pcs_1 device data; skips log- prefixed signals.
    """
    import zipfile, io, csv as _csv
    print(f"[csv] Loading real data from {zip_path} ...")
    # files to load (bms and pcs for UC 61B5 — smallest complete pair)
    TARGET_FILES = [
        "SBESS3 bms data - UC 61B5.csv",
        "SBESS3 pcs data - UC 61B5.csv",
    ]
    TARGET_DEVICES = {'bms_1', 'pcs_1'}
    values = {}   # (device, instance, point_name, dtype) -> [val, ...]
    with zipfile.ZipFile(zip_path) as zf:
        available = zf.namelist()
        for target in TARGET_FILES:
            if target not in available:
                print(f"[csv] WARNING: {target!r} not found in zip, skipping")
                continue
            print(f"[csv]   reading {target} ...", flush=True)
            with zf.open(target) as raw:
                reader = _csv.reader(io.TextIOWrapper(raw, encoding='utf-8'))
                for i, row in enumerate(reader):
                    if i < 4: continue   # skip Flux annotation header
                    if len(row) < 14: continue
                    dtype    = row[7]    # _field: float / integer / string
                    if dtype not in ('float', 'integer'): continue
                    meas     = row[8]    # _measurement: bms / pcs
                    point    = row[9]    # point_name
                    instance = row[11]   # source_device_id: bms_1, pcs_1, ...
                    val_str  = row[6]    # _value
                    if point.startswith('log-'): continue
                    if instance not in TARGET_DEVICES: continue
                    try:
                        val = float(val_str)
                    except ValueError:
                        continue
                    key = (meas, instance, point, dtype)
                    if key not in values:
                        values[key] = []
                    values[key].append(val)
    # build sorted signal list (float first, then integer)
    float_sigs   = sorted(k for k in values if k[3] == 'float')
    integer_sigs = sorted(k for k in values if k[3] == 'integer')
    signals = float_sigs + integer_sigs
    n_steps = min(len(v) for v in values.values()) if values else 0
    print(f"[csv] Loaded {len(signals)} signals, {n_steps} time steps each")
    return signals, values

if args.csv:
    ALL_SIGNALS, _csv_values = load_csv_replay(args.csv)
    FLOAT_SIGNALS = [s for s in ALL_SIGNALS if s[3] == 'float']
    INT_SIGNALS   = [s for s in ALL_SIGNALS if s[3] == 'integer']
else:
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
    ALL_SIGNALS = FLOAT_SIGNALS + INT_SIGNALS
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

# ── CPU sampling setup ────────────────────────────────────────────────────────
try:
    import psutil as _psutil
    _cpu_procs = {name: _psutil.Process(p.pid) for name, p in procs.items()}
    # prime cpu_percent (first call always returns 0.0)
    for cp in _cpu_procs.values():
        try: cp.cpu_percent(interval=None)
        except: pass
    _cpu_samples = {name: [] for name in procs}
    _psutil_ok = True
except ImportError:
    _psutil_ok = False
    _cpu_samples = {name: [] for name in procs}

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

# ── Physics-based signal generator (calibrated from SBESS3 real site data) ───
# SBESS3: utility-scale US BESS, 342S cells, 60 Hz grid
# Cell V: 3.328–3.393 V, Temps: 20–29°C, SOC: 78–92% over 3 hr

# Li-NMC OCV lookup: (soc_fraction, ocv_V_per_cell)
_OCV_TABLE = [
    (0.00, 3.000), (0.05, 3.100), (0.10, 3.200), (0.20, 3.280),
    (0.30, 3.320), (0.40, 3.340), (0.50, 3.360), (0.60, 3.380),
    (0.70, 3.400), (0.80, 3.420), (0.90, 3.440), (1.00, 3.500),
]

def _ocv(soc_frac):
    """Interpolate OCV per cell from SOC fraction."""
    for i in range(len(_OCV_TABLE) - 1):
        s0, v0 = _OCV_TABLE[i]
        s1, v1 = _OCV_TABLE[i + 1]
        if s0 <= soc_frac <= s1:
            t = (soc_frac - s0) / (s1 - s0)
            return v0 + t * (v1 - v0)
    return _OCV_TABLE[-1][1]

class UnitPhysics:
    """Per-unit physics state machine, 1s time steps.
    Calibrated to SBESS3: 342S cells, 60 Hz US grid, SOC 78–92%."""

    N_CELLS = 10   # cells modelled per unit (Batt1_Cell1..10)
    N_TEMPS = 5    # temp sensors modelled (Batt1_Cell1..5_Temperature)
    CAPACITY_AH = 200.0   # nominal pack capacity

    def __init__(self, seed=None):
        rng = random.Random(seed)
        # SOC: start somewhere in 78–92% range, drift slowly
        self.soc     = rng.uniform(0.78, 0.92)
        # Charge/discharge: positive = discharging, sign flips on cycle
        self._current_A   = rng.uniform(80.0, 120.0) * rng.choice([-1, 1])
        self._cycle_timer = rng.uniform(0, 1800)   # seconds into current cycle
        self._cycle_half  = rng.uniform(1200, 2400) # half-cycle length (s)

        # Cell voltage spreads: each cell has a fixed small offset ±20mV
        self._cell_dv = [rng.uniform(-0.020, 0.020) for _ in range(self.N_CELLS)]

        # RC thermal model: τ=600s, ambient 20–25°C, initial temp 20–29°C
        self._ambient  = rng.uniform(20.0, 25.0)
        self._temps    = [rng.uniform(20.0, 29.0) for _ in range(self.N_TEMPS)]
        self._tau      = 600.0

        # Grid frequency: 60 Hz US, small random walk
        self._freq     = rng.gauss(60.0, 0.01)

        # Integer states: normal operation
        self._contactor = 2    # closed
        self._pcs_state = 3    # running
        self._grid_conn = 1
        self._fan_state = 1
        self._balancing = 0

        # Fault state (Poisson: λ≈0.1 events/day → p≈1.16e-6/s)
        self._bms_fault = 0
        self._bms_warn  = 0
        self._pcs_fault = 0
        self._alarm     = 0
        self._door_open = 0
        self._fault_timer = 0  # clears fault after ~30s

        self._rng = rng

    def step(self, dt=1.0):
        rng = self._rng
        # ── Charge/discharge cycle ───────────────────────────────────────────
        self._cycle_timer += dt
        if self._cycle_timer >= self._cycle_half:
            self._cycle_timer = 0.0
            self._cycle_half  = rng.uniform(1200, 2400)
            self._current_A   = rng.uniform(80.0, 120.0) * (-1 if self._current_A > 0 else 1)

        # SOC integrator: ΔQ = I × dt / 3600 / C
        dsoc = -(self._current_A * dt) / (3600.0 * self.CAPACITY_AH)
        self.soc = max(0.15, min(0.98, self.soc + dsoc + rng.gauss(0, 1e-5)))

        # ── Cell voltages via OCV + cell spread + noise ──────────────────────
        ocv = _ocv(self.soc)
        ri  = 0.001   # internal resistance Ω per cell
        v_drop = self._current_A * ri
        self._cell_v = [
            round(ocv - v_drop + self._cell_dv[i] + rng.gauss(0, 0.0005), 4)
            for i in range(self.N_CELLS)
        ]

        # ── RC thermal: τ=600s, heat from I²R ───────────────────────────────
        heat_rise = (self._current_A ** 2) * 0.0002   # °C/s per sensor
        for i in range(self.N_TEMPS):
            delta = (self._ambient + heat_rise - self._temps[i]) * dt / self._tau
            self._temps[i] = round(self._temps[i] + delta + rng.gauss(0, 0.05), 3)

        # ── Grid frequency: random walk ──────────────────────────────────────
        self._freq = round(
            max(59.80, min(60.20, self._freq + rng.gauss(0, 0.005))), 4)

        # ── Rare fault events (Poisson ~0.1/day) ────────────────────────────
        if self._fault_timer > 0:
            self._fault_timer -= dt
            if self._fault_timer <= 0:
                self._bms_fault = self._bms_warn = self._pcs_fault = self._alarm = 0
        elif rng.random() < 1.16e-4:   # ≈0.1/day at 1 Hz
            self._bms_fault = rng.randint(1, 15)
            self._bms_warn  = rng.randint(1, 7)
            self._alarm     = 1
            self._fault_timer = rng.uniform(20, 60)

        # ── Rare door open event ─────────────────────────────────────────────
        if self._door_open:
            if rng.random() < 0.02:
                self._door_open = 0
        elif rng.random() < 5e-5:
            self._door_open = 1

    def get(self, device, instance, signame, dtype):
        """Return physics-based value for the given signal."""
        if dtype == "float":
            # Cell voltages
            if signame.startswith("Batt1_Cell") and signame.endswith("_Voltage"):
                idx = int(signame.split("Cell")[1].split("_")[0]) - 1
                return self._cell_v[idx % self.N_CELLS]
            # Cell temperatures
            if signame.startswith("Batt1_Cell") and signame.endswith("_Temperature"):
                idx = int(signame.split("Cell")[1].split("_")[0]) - 1
                return self._temps[idx % self.N_TEMPS]
            if signame == "Pack_SOC":
                return round(self.soc * 100.0, 3)
            if signame == "Pack_SOH":
                return round(self._rng.gauss(96.0, 0.05), 3)
            if signame == "Pack_Current":
                return round(self._current_A + self._rng.gauss(0, 0.5), 3)
            if signame == "Pack_Voltage":
                # 342 series cells
                return round(sum(self._cell_v) * 34.2, 2)
            if signame == "DCBusVoltage":
                return round(sum(self._cell_v) * 34.2 + self._rng.gauss(0, 1.0), 2)
            if signame == "GridFrequency":
                return self._freq
            if signame == "ActivePower":
                return round(self._current_A * sum(self._cell_v) * 34.2 / 1000.0, 2)
            if signame == "ReactivePower":
                return round(self._rng.gauss(5.0, 2.0), 2)
            if signame == "OutputCurrent":
                return round(self._current_A + self._rng.gauss(0, 0.3), 3)
            # fallback
            return round(self._rng.uniform(0.0, 100.0), 4)
        else:  # integer
            if signame == "Pack_ContactorState":  return self._contactor
            if signame == "Batt1_CellBalancing":  return self._balancing
            if signame == "BMS_FaultCode":        return self._bms_fault
            if signame == "BMS_WarningCode":      return self._bms_warn
            if signame == "PCS_State":            return self._pcs_state
            if signame == "PCS_FaultCode":        return self._pcs_fault
            if signame == "GridConnected":        return self._grid_conn
            if signame == "AlarmActive":          return self._alarm
            if signame == "Rack_FanState":        return self._fan_state
            if signame == "Rack_DoorOpen":        return self._door_open
            return 0

# ── CSV replay class ──────────────────────────────────────────────────────────
class CsvReplay:
    """Plays back real SBESS3 CSV values for one benchmark unit.
    Each unit gets a different starting offset so they are not synchronised."""

    def __init__(self, csv_values, offset=0):
        self._values = csv_values   # {(device, instance, point, dtype): [vals]}
        self._idx    = offset
        self._lens   = {k: len(v) for k, v in csv_values.items()}

    def step(self, dt=1.0):
        self._idx += 1

    def get(self, device, instance, signame, dtype):
        key  = (device, instance, signame, dtype)
        vals = self._values.get(key)
        if not vals:
            return 0.0 if dtype == 'float' else 0
        return vals[self._idx % self._lens[key]]

# One physics/replay instance per unit — seeded by unit index for repeatability
if args.csv:
    # stagger each unit's start by ~90 s so they are not all in lock-step
    _n_steps = min(len(v) for v in _csv_values.values())
    _step_sz = max(1, _n_steps // len(UNITS))
    _physics = {unit: CsvReplay(_csv_values, offset=i * _step_sz)
                for i, unit in enumerate(UNITS)}
else:
    _physics = {unit: UnitPhysics(seed=i) for i, unit in enumerate(UNITS)}

for sweep in range(SWEEPS):
    sweep_wall_start = time.time()
    sweep_base_ts = base_ts + sweep * 1.0   # simulated 1 Hz tick (monotonic)

    # Monotonically increasing intra-burst delays (seconds) for this sweep.
    # Sorted so wall-clock delivery and simulated ts advance together.
    burst_s = random.uniform(BURST_MIN_MS, BURST_MAX_MS) * 1e-3
    delays  = sorted(random.uniform(0, burst_s) for _ in range(N_MSGS))

    # Advance physics state for each unit this sweep
    for unit in UNITS:
        _physics[unit].step(dt=1.0)

    msg_idx = 0
    for unit in UNITS:
        for (device, instance, signame, dtype) in ALL_SIGNALS:
            val = _physics[unit].get(device, instance, signame, dtype)

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

    # sample CPU every 10 sweeps
    if _psutil_ok and sweep % 10 == 0 and sweep > 0:
        for name, cp in _cpu_procs.items():
            try: _cpu_samples[name].append(cp.cpu_percent(interval=None))
            except: pass

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
        t = pa.concat_tables([pq.read_table(f) for f in files], promote_options="default")
        rows   = t.num_rows
        schema = t.schema
    else:
        schema = None
    return total, len(files), rows, schema

def compact_stats(path, chunk_minutes=10):
    """Merge all parquet files in each leaf directory into one file per leaf.
    Returns (total_bytes, file_count, row_count) of compacted output."""
    import tempfile, collections
    leaf_dirs = collections.defaultdict(list)
    for root, dirs, fnames in os.walk(path):
        if not dirs:  # leaf directory
            for f in fnames:
                if f.endswith(".parquet"):
                    leaf_dirs[root].append(os.path.join(root, f))
    if not leaf_dirs:
        return 0, 0, 0
    total_bytes, total_files, total_rows = 0, 0, 0
    with tempfile.TemporaryDirectory() as tmpdir:
        for leaf, files in leaf_dirs.items():
            if not files:
                continue
            try:
                tables = [pq.read_table(f) for f in files]
                merged = pa.concat_tables(tables)
                out = os.path.join(tmpdir, f"compact_{os.path.basename(leaf)}.parquet")
                pq.write_table(merged, out, compression='snappy')
                total_bytes += os.path.getsize(out)
                total_files += 1
                total_rows  += merged.num_rows
            except Exception as e:
                print(f"  compact_stats warning: {leaf}: {e}")
    return total_bytes, total_files, total_rows

print("[4] Computing compacted stats...")
compact = {
    "norm-long":  compact_stats(OUT_NORM_LONG),
    "long+cmp":   compact_stats(OUT_LONG_CMP),
    "wide-pivot": compact_stats(OUT_WIDE_PIVOT),
}
for k, (sz, nf, nr) in compact.items():
    print(f"  {k}: {nf} files → {sz//1024} KB  ({nr:,} rows)")

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
def cpu_stats(samples):
    if not samples: return {"avg_pct": None, "peak_pct": None}
    return {"avg_pct": round(sum(samples)/len(samples), 1),
            "peak_pct": round(max(samples), 1)}

import multiprocessing as _mp
_ncores = _mp.cpu_count()

result = {
    "topology": {"units": len(UNITS), "signals": len(ALL_SIGNALS),
                 "float_signals": len(FLOAT_SIGNALS), "int_signals": len(INT_SIGNALS),
                 "sweeps": SWEEPS, "sim_seconds": SIM_DURATION, "total_msgs": TOTAL_MSGS,
                 "cpu_cores": _ncores},
    "norm_long": {
        "size_bytes": sizes[0], "files": nfiles[0], "rows": nrows[0], "cols": ncols[0],
        "flush_ms": ftimes[0], "cpu": cpu_stats(_cpu_samples["norm-long"]),
        "compacted": {"size_bytes": compact["norm-long"][0], "files": compact["norm-long"][1], "rows": compact["norm-long"][2]},
        "schema": [{"name": f.name, "type": str(f.type)} for f in list(stats["norm-long"][3])] if stats["norm-long"][3] else []
    },
    "long_cmp": {
        "size_bytes": sizes[1], "files": nfiles[1], "rows": nrows[1], "cols": ncols[1],
        "flush_ms": ftimes[1], "cpu": cpu_stats(_cpu_samples["long+cmp"]),
        "compacted": {"size_bytes": compact["long+cmp"][0], "files": compact["long+cmp"][1], "rows": compact["long+cmp"][2]},
        "schema": [{"name": f.name, "type": str(f.type)} for f in list(stats["long+cmp"][3])] if stats["long+cmp"][3] else []
    },
    "wide_pivot": {
        "size_bytes": sizes[2], "files": nfiles[2], "rows": nrows[2], "cols": ncols[2],
        "flush_ms": ftimes[2], "cpu": cpu_stats(_cpu_samples["wide-pivot"]),
        "compacted": {"size_bytes": compact["wide-pivot"][0], "files": compact["wide-pivot"][1], "rows": compact["wide-pivot"][2]},
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
        # add first/last timestamp
        all_files = sorted(pq_files)
        first_tbl = _pq.read_table(all_files[0],  columns=["ts"])
        last_tbl  = _pq.read_table(all_files[-1], columns=["ts"])
        ts_f = first_tbl.column("ts").drop_null()
        ts_l = last_tbl.column("ts").drop_null()
        samples["_first_ts"] = str(ts_f[0].as_py())  if len(ts_f) else None
        samples["_last_ts"]  = str(ts_l[-1].as_py()) if len(ts_l) else None

        samples_file = os.path.join(OUTDIR, "column_samples.json")
        with open(samples_file, "w") as fh:
            _json.dump(samples, fh)
        print(f"  Column samples saved to {samples_file} ({len(samples)-2} columns + timestamps)")
    else:
        print("  No wide parquet files found for column samples")
except Exception as e:
    print(f"  WARNING: could not generate column_samples.json: {e}")
