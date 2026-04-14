#!/usr/bin/env python3
"""
continuous_publisher.py — publishes 12-unit × N-signal BMS/PCS data at 1 Hz forever.

Modes:
  Physics model (default):  synthetic data calibrated to SBESS3 real-site ranges
  CSV replay (--csv):       real SBESS3 Flux CSV data, cycling indefinitely

Topics: bench/{unit_id}/{device}/{instance}/{signal}/{dtype}
"""
import time, json, random, signal as _signal, sys, math, os, argparse, zipfile, io, csv as _csv

parser = argparse.ArgumentParser()
parser.add_argument("--csv",       default=None,  help="Path to SBESS3 zip file for real-data replay")
parser.add_argument("--host",      default=os.environ.get("MQTT_HOST","localhost"))
parser.add_argument("--port",      type=int, default=int(os.environ.get("MQTT_PORT","1883")))
parser.add_argument("--rate",      type=float, default=1.0, help="Sweeps per second (default 1.0)")
parser.add_argument("--cov",       action="store_true", help="Publisher-side COV filtering")
parser.add_argument("--cov-reset", type=int, default=0,  help="Clear COV cache every N seconds (0=never); forces republish of constants")
parser.add_argument("--units",     type=int, default=12)
args = parser.parse_args()

MQTT_HOST    = args.host
MQTT_PORT    = args.port
SWEEP_PERIOD = 1.0 / args.rate
BURST_MIN_MS = 50
BURST_MAX_MS = 200
COV_FLOAT_PCT = 0.01

import paho.mqtt.client as mqtt

UNITS = [f"{0x0215D1D8 + i:08X}" for i in range(args.units)]

FLOAT_SIGNALS = (
    [("bms","bms_1",f"Batt1_Cell{c}_Voltage",   "float") for c in range(1,11)] +
    [("bms","bms_1",f"Batt1_Cell{c}_Temperature","float") for c in range(1,6)]  +
    [("bms","bms_1","Pack_Current",  "float"),
     ("bms","bms_1","Pack_Voltage",  "float"),
     ("bms","bms_1","Pack_SOC",      "float"),
     ("bms","bms_1","Pack_SOH",      "float"),
     ("pcs","pcs_1","GridFrequency", "float"),
     ("pcs","pcs_1","ActivePower",   "float"),
     ("pcs","pcs_1","ReactivePower", "float"),
     ("pcs","pcs_1","DCBusVoltage",  "float"),
     ("pcs","pcs_1","OutputCurrent", "float")]
)
INT_SIGNALS = [
    ("bms","bms_1","Pack_ContactorState","integer"),
    ("bms","bms_1","Batt1_CellBalancing","integer"),
    ("bms","bms_1","BMS_FaultCode",      "integer"),
    ("bms","bms_1","BMS_WarningCode",    "integer"),
    ("pcs","pcs_1","PCS_State",          "integer"),
    ("pcs","pcs_1","PCS_FaultCode",      "integer"),
    ("pcs","pcs_1","GridConnected",      "integer"),
    ("pcs","pcs_1","AlarmActive",        "integer"),
    ("rack","rack_1","Rack_FanState",    "integer"),
    ("rack","rack_1","Rack_DoorOpen",    "integer"),
]

# ── CSV replay ────────────────────────────────────────────────────────────────
CSV_EXCLUDE = {
    # Monotonics only — heartbeats/counters that increment every sweep
    # Constants are kept; null_fill_unchanged compresses them to near-zero
    'BMS_SysHB', 'SysHB', 'Counter', 'ItemDeletionTime',
}
TARGET_DEVICES = {'bms_1','pcs_1'}

def load_csv(zip_path):
    """Load SBESS3 Flux CSV from zip. Returns (signals, values_dict)."""
    TARGET_FILES = ["SBESS3 bms data - UC 61B5.csv", "SBESS3 pcs data - UC 61B5.csv"]
    values = {}
    with zipfile.ZipFile(zip_path) as zf:
        for target in TARGET_FILES:
            if target not in zf.namelist():
                print(f"[csv] WARNING: {target!r} not in zip, skipping", flush=True)
                continue
            print(f"[csv] reading {target} ...", flush=True)
            with zf.open(target) as raw:
                reader = _csv.reader(io.TextIOWrapper(raw, encoding='utf-8'))
                for i, row in enumerate(reader):
                    if i < 4: continue
                    if len(row) < 14: continue
                    dtype    = row[7]
                    if dtype not in ('float','integer'): continue
                    meas     = row[8]
                    point    = row[9]
                    instance = row[11]
                    val_str  = row[6]
                    if point.startswith('log-'): continue
                    if instance not in TARGET_DEVICES: continue
                    if point in CSV_EXCLUDE: continue
                    try: val = float(val_str)
                    except ValueError: continue
                    key = (meas, instance, point, dtype)
                    if key not in values: values[key] = []
                    values[key].append(val)
    float_sigs   = sorted(k for k in values if k[3]=='float')
    integer_sigs = sorted(k for k in values if k[3]=='integer')
    signals = float_sigs + integer_sigs
    n_steps = min(len(v) for v in values.values()) if values else 0
    print(f"[csv] {len(signals)} signals, {n_steps} steps each (cycling)", flush=True)
    return signals, values, n_steps

# ── Physics model ─────────────────────────────────────────────────────────────
_OCV_TABLE = [
    (0.00,3.000),(0.05,3.100),(0.10,3.200),(0.20,3.280),(0.30,3.320),
    (0.40,3.340),(0.50,3.360),(0.60,3.380),(0.70,3.400),(0.80,3.420),
    (0.90,3.440),(1.00,3.500),
]
def _ocv(s):
    for i in range(len(_OCV_TABLE)-1):
        s0,v0=_OCV_TABLE[i]; s1,v1=_OCV_TABLE[i+1]
        if s0<=s<=s1: return v0+(s-s0)/(s1-s0)*(v1-v0)
    return _OCV_TABLE[-1][1]

class UnitPhysics:
    N_CELLS=10; N_TEMPS=5; CAPACITY_AH=200.0
    def __init__(self,seed=None):
        rng=random.Random(seed)
        self.soc=rng.uniform(0.78,0.92)
        self._current_A=rng.uniform(80.0,120.0)*rng.choice([-1,1])
        self._cycle_timer=rng.uniform(0,1800); self._cycle_half=rng.uniform(1200,2400)
        self._cell_dv=[rng.uniform(-0.020,0.020) for _ in range(self.N_CELLS)]
        self._ambient=rng.uniform(20.0,25.0)
        self._temps=[rng.uniform(20.0,29.0) for _ in range(self.N_TEMPS)]
        self._tau=600.0; self._freq=rng.gauss(60.0,0.01)
        self._contactor=2;self._pcs_state=3;self._grid_conn=1;self._fan_state=1;self._balancing=0
        self._bms_fault=0;self._bms_warn=0;self._pcs_fault=0;self._alarm=0;self._door_open=0;self._fault_timer=0
        self._rng=rng
    def step(self,dt=1.0):
        rng=self._rng
        self._cycle_timer+=dt
        if self._cycle_timer>=self._cycle_half:
            self._cycle_timer=0.0;self._cycle_half=rng.uniform(1200,2400)
            self._current_A=rng.uniform(80.0,120.0)*(-1 if self._current_A>0 else 1)
        dsoc=-(self._current_A*dt)/(3600.0*self.CAPACITY_AH)
        self.soc=max(0.15,min(0.98,self.soc+dsoc+rng.gauss(0,1e-5)))
        ocv=_ocv(self.soc);ri=0.001;v_drop=self._current_A*ri
        self._cell_v=[round(ocv-v_drop+self._cell_dv[i]+rng.gauss(0,0.0005),4) for i in range(self.N_CELLS)]
        heat_rise=(self._current_A**2)*0.0002
        for i in range(self.N_TEMPS):
            delta=(self._ambient+heat_rise-self._temps[i])*dt/self._tau
            self._temps[i]=round(self._temps[i]+delta+rng.gauss(0,0.05),3)
        self._freq=round(max(59.80,min(60.20,self._freq+rng.gauss(0,0.005))),4)
        if self._fault_timer>0:
            self._fault_timer-=dt
            if self._fault_timer<=0: self._bms_fault=self._bms_warn=self._pcs_fault=self._alarm=0
        elif rng.random()<1.16e-4:
            self._bms_fault=rng.randint(1,15);self._bms_warn=rng.randint(1,7);self._alarm=1;self._fault_timer=rng.uniform(20,60)
        if self._door_open:
            if rng.random()<0.02: self._door_open=0
        elif rng.random()<5e-5: self._door_open=1
    def get(self,device,instance,signame,dtype):
        if dtype=="float":
            if signame.startswith("Batt1_Cell") and signame.endswith("_Voltage"):
                return self._cell_v[int(signame.split("Cell")[1].split("_")[0])-1]
            if signame.startswith("Batt1_Cell") and signame.endswith("_Temperature"):
                return self._temps[(int(signame.split("Cell")[1].split("_")[0])-1)%self.N_TEMPS]
            if signame=="Pack_SOC": return round(self.soc*100.0,3)
            if signame=="Pack_SOH": return round(self._rng.gauss(96.0,0.05),3)
            if signame=="Pack_Current": return round(self._current_A+self._rng.gauss(0,0.5),3)
            if signame=="Pack_Voltage": return round(sum(self._cell_v)*34.2,2)
            if signame=="DCBusVoltage": return round(sum(self._cell_v)*34.2+self._rng.gauss(0,1.0),2)
            if signame=="GridFrequency": return self._freq
            if signame=="ActivePower": return round(self._current_A*sum(self._cell_v)*34.2/1000.0,2)
            if signame=="ReactivePower": return round(self._rng.gauss(5.0,2.0),2)
            if signame=="OutputCurrent": return round(self._current_A+self._rng.gauss(0,0.3),3)
            return round(self._rng.uniform(0.0,100.0),4)
        else:
            if signame=="Pack_ContactorState": return self._contactor
            if signame=="Batt1_CellBalancing": return self._balancing
            if signame=="BMS_FaultCode": return self._bms_fault
            if signame=="BMS_WarningCode": return self._bms_warn
            if signame=="PCS_State": return self._pcs_state
            if signame=="PCS_FaultCode": return self._pcs_fault
            if signame=="GridConnected": return self._grid_conn
            if signame=="AlarmActive": return self._alarm
            if signame=="Rack_FanState": return self._fan_state
            if signame=="Rack_DoorOpen": return self._door_open
            return 0

def fmt_ts(unix_s):
    import datetime
    dt=datetime.datetime.utcfromtimestamp(unix_s)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.")+f"{dt.microsecond//1000:03d}Z"

# ── Load data source ──────────────────────────────────────────────────────────
if args.csv:
    ALL_SIGNALS, _csv_values, _n_steps = load_csv(args.csv)
    # Stagger each unit's start offset so they're not synchronised
    _step_sz = max(1, _n_steps // len(UNITS))
    _offsets = {unit: i * _step_sz for i, unit in enumerate(UNITS)}
    _indices  = dict(_offsets)   # mutable per-unit step index

    def get_val(unit, device, instance, signame, dtype):
        key  = (device, instance, signame, dtype)
        vals = _csv_values.get(key)
        if not vals: return 0.0 if dtype=='float' else 0
        return vals[_indices[unit] % len(vals)]

    def step_unit(unit):
        _indices[unit] += 1
        if _indices[unit] % _n_steps == 0:
            print(f"[pub] unit {unit} cycled through CSV data (step {_indices[unit]})", flush=True)

    mode = f"CSV replay: {args.csv}  ({len(ALL_SIGNALS)} signals, {_n_steps} steps, cycling)"
else:
    ALL_SIGNALS = FLOAT_SIGNALS + INT_SIGNALS
    _physics = {unit: UnitPhysics(seed=i) for i, unit in enumerate(UNITS)}

    def get_val(unit, device, instance, signame, dtype):
        return _physics[unit].get(device, instance, signame, dtype)

    def step_unit(unit):
        _physics[unit].step(dt=1.0)

    mode = "physics model (synthetic, calibrated to SBESS3 ranges)"

# ── COV state ─────────────────────────────────────────────────────────────────
_last_pub = {}

def should_pub(unit, device, instance, signame, dtype, val):
    if not args.cov: return True
    key = (unit, device, instance, signame)
    prev = _last_pub.get(key)
    if prev is None: _last_pub[key] = val; return True
    if dtype == "float":
        if abs(val - prev) / max(abs(prev), 1e-6) > COV_FLOAT_PCT:
            _last_pub[key] = val; return True
        return False
    else:
        if val != prev: _last_pub[key] = val; return True
        return False

# ── MQTT ──────────────────────────────────────────────────────────────────────
_running = True
def _stop(sig, frame):
    global _running
    print("\n[pub] stopping...", flush=True)
    _running = False

_signal.signal(_signal.SIGTERM, _stop)
_signal.signal(_signal.SIGINT,  _stop)

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
client = mqtt.Client(client_id="bench-continuous-publisher")
client.connect(MQTT_HOST, MQTT_PORT)
client.loop_start()

N_MSGS   = len(UNITS) * len(ALL_SIGNALS)
sweep    = 0
pub_count= 0
t0       = time.time()
print(f"[pub] started — {len(UNITS)} units × {len(ALL_SIGNALS)} signals @ {args.rate} Hz", flush=True)
print(f"[pub] mode: {mode}", flush=True)
print(f"[pub] COV filtering: {'on' if args.cov else 'off'}", flush=True)
print(f"[pub] broker: {MQTT_HOST}:{MQTT_PORT}", flush=True)

while _running:
    sweep_wall_start = time.time()
    burst_s = random.uniform(BURST_MIN_MS, BURST_MAX_MS) * 1e-3
    delays  = sorted(random.uniform(0, burst_s) for _ in range(N_MSGS))

    for unit in UNITS:
        step_unit(unit)

    msg_idx = 0
    for unit in UNITS:
        for (device, instance, signame, dtype) in ALL_SIGNALS:
            val    = get_val(unit, device, instance, signame, dtype)
            ts_str = fmt_ts(sweep_wall_start + delays[msg_idx])
            target = sweep_wall_start + delays[msg_idx]
            now    = time.time()
            if target > now: time.sleep(target - now)
            if should_pub(unit, device, instance, signame, dtype, val):
                topic = f"bench/{unit}/{device}/{instance}/{signame}/{dtype}"
                client.publish(topic, json.dumps({"ts": ts_str, "value": val}), qos=0)
                pub_count += 1
            msg_idx += 1

    elapsed = time.time() - sweep_wall_start
    remainder = SWEEP_PERIOD - elapsed
    if remainder > 0: time.sleep(remainder)

    sweep += 1
    if sweep % 60 == 0:
        elapsed_total = time.time() - t0
        print(f"[pub] sweep={sweep}  msgs={pub_count:,}  rate={pub_count/elapsed_total:,.0f}/s  uptime={elapsed_total:.0f}s", flush=True)
    if args.cov_reset > 0 and sweep > 0 and (time.time() - t0) % args.cov_reset < SWEEP_PERIOD:
        _last_pub.clear()
        print(f"[pub] COV cache cleared at sweep={sweep} (reset_interval={args.cov_reset}s)", flush=True)

client.loop_stop()
client.disconnect()
print(f"[pub] stopped after {sweep} sweeps, {pub_count:,} msgs", flush=True)
