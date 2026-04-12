# parquet_writer bench — project context

## Machines

| Host | Role | Address |
|------|------|---------|
| phil-dev | dev/git/html | local |
| .34 | writer + publisher (bench target) | 192.168.86.34 |

SSH: `ssh phil@192.168.86.34`

---

## Key paths

### On .34
| Path | What |
|------|------|
| `/home/phil/data_server/source/parquet_writer/parquet_writer` | writer binary |
| `/home/phil/data_server/scripts/continuous_publisher.py` | continuous MQTT publisher |
| `/mnt/tort-sdi/bench/continuous_wide.yaml` | active writer config |
| `/mnt/tort-sdi/bench/bench-wide/bench/2026/04/11/` | parquet output (today) |
| `/mnt/tort-sdi/bench/compact.log` | compactor log (JSON lines) |
| `/home/phil/SBESS3_UC61B5.zip` | SBESS3 source data zip |
| `/tmp/pub.log` | publisher stdout |
| `/tmp/writer.log` | writer stdout |

### On phil-dev
| Path | What |
|------|------|
| `/home/phil/work/gen-ai/data_server/html/writer/writer_design.html` | design doc |
| `/home/phil/work/gen-ai/data_server/source/parquet_writer/` | writer source |
| `/home/phil/fractal/zip/SBESS3 Influx Data.zip` | canonical SBESS3 zip |

---

## Active run (continuous wide-pivot)

### Start writer
```bash
nohup /home/phil/data_server/source/parquet_writer/parquet_writer \
  --config /mnt/tort-sdi/bench/continuous_wide.yaml > /tmp/writer.log 2>&1 &
```

### Start publisher
```bash
nohup python3 /home/phil/data_server/scripts/continuous_publisher.py \
  --csv /home/phil/SBESS3_UC61B5.zip --cov --cov-reset 300 > /tmp/pub.log 2>&1 &
```

**Always restart publisher after restarting writer** — the publisher's COV cache must be cleared so the writer sees all signals on sweep 1.

### Check status
```bash
ps aux | grep -E 'parquet_writer|continuous_publisher' | grep -v grep
curl -s http://192.168.86.34:8771/health | python3 -m json.tool
```

---

## Writer config: continuous_wide.yaml (key settings)

```yaml
mqtt:
  compound_field_name: [device, instance, point_name]   # wide-pivot column name
  topic_segments: [source_type, unit_id, device, instance, point_name, dtype_hint]
  drop_columns: [source_type]      # must be under mqtt:, not output:
  partition_field: unit_id
output:
  base_path: /mnt/tort-sdi/bench/bench-wide
  partitions: ['{year}', '{month}', '{day}']   # source_type is auto-prepended — do NOT add {source_type} here
  wide_point_name: true
  store_mqtt_topic: false          # suppress mqtt_topic column
  store_sample_count: false        # suppress sample_count column
compact:
  interval_seconds: 300
```

---

## Schema decisions

- **Wide-pivot is primary format** — one row per unit per timestamp, one column per signal path
- **Removed columns**: `sample_count`, `mqtt_topic`, `source_type` — were degrading compression metrics
- **Column name format**: `device/instance/signal` e.g. `bms/bms_1/SysSOC`
- **Expected columns**: `ts` + 63 signals = 64 total (after COV reset fires)

---

## Source data: SBESS3 UC 61B5

- Utility-scale US BESS, 3-hour recording
- 2 devices: `bms_1`, `pcs_1`
- 63 signals after filtering (67 raw − 4 monotonics excluded)
- 848 steps per signal, cycling at 1 Hz → ~14 min per cycle

### CSV_EXCLUDE (monotonics only — counters/heartbeats that increment every sweep)
```python
CSV_EXCLUDE = {
    'BMS_SysHB', 'SysHB', 'Counter', 'ItemDeletionTime',
}
```
**Constants are kept** — null_fill_unchanged (or --cov-reset) handles compression.

---

## COV behaviour

| Layer | Mechanism | Effect |
|-------|-----------|--------|
| Publisher `--cov` | float suppress <1% drift; int on-change | Reduces MQTT traffic |
| Publisher `--cov-reset 300` | Clears COV cache every 300s | Forces constants to republish every 5 min |
| Writer `null_fill_unchanged` | (not yet enabled) | Would null unchanged cols at flush; needs publisher sending all signals |

**Why --cov-reset matters**: without it, constants are published once at startup then suppressed forever. After any writer restart, constants never re-appear in the schema until the publisher is also restarted or the reset fires.

---

## Compactor

- Runs every 5 min, merges flush files → `compact_<ts>.parquet`
- `min_files: 3` — needs at least 3 flush files before compacting
- `null_pct` in compact.log: ~78% normal (dynamic signals only); ~85% after adding constants (expected)

---

## Verify column count
```bash
/home/sysdcs/work/claude/check_columns.sh
```
Or directly:
```bash
ssh phil@192.168.86.34 "python3 -c \"
import pyarrow.parquet as pq, glob
files = sorted(f for f in glob.glob('/mnt/tort-sdi/bench/bench-wide/bench/\$(date +%Y/%m/%d)/*.parquet') if 'compact' not in f)
s = pq.read_schema(files[-1]); print(len(s.names), 'cols:', files[-1].split('/')[-1])
\""
```

---

## Design doc

`writer_design.html` on phil-dev — committed 2026-04-11 (commit 4a4a182)
Sections: Wide-Pivot Schema (primary), Source Data SBESS3, Schema Format Comparison, full config reference
Links to: `evelyn_data_analysis.html` (noise analysis), `ems_physics_proposal.html` (signal calibration)
