"""
Phase 1 integration tests — run against fractal-phil (192.168.86.51).

Requires:
  - fractal-phil reachable on 192.168.86.51
  - InfluxDB2 running on :8086 with token battery-token-secret
  - Data flowing (stress runner + Telegraf active)

Run from phil-dev:
  cd source/subscriber_api/tests
  FRACTAL=192.168.86.51 python -m pytest test_phase1_integration.py -v

Skip gracefully when fractal-phil is unreachable.

Notes on InfluxDB2 performance with this schema:
  - battery_cell measurement has ~9984 series (one per cell × field)
  - Flux limit() is PER SERIES, not global
  - Data in WAL/cache (last ~5 min) returns in < 5 s
  - Older data reads TSM files; 1-hour range can take > 30 s
  - Tests use narrow windows (≤ 5 min) to stay in cache
"""

import os
import sys
import time
import unittest

sys.path.insert(0, "..")
from backends.influxdb2 import InfluxDB2Backend
from router import QueryRouter

FRACTAL = os.environ.get("FRACTAL", "192.168.86.51")

# fractal-phil tag values use key=value style: site_id="site=0", not "0"
INFLUX_CFG = {
    "influxdb2": {
        "url":             f"http://{FRACTAL}:8086",
        "token":           "battery-token-secret",
        "org":             "battery-org",
        "bucket":          "battery-data",
        "measurement":     "battery_cell",
        "site_tag":        "site_id",
        "site_tag_prefix": "site=",    # stored as "site=0", queried as "0"
        # Scope to rack=0 module=0 (104 series) to keep query latency < 2 s.
        # Without this, site=0 has ~9984 series; even a 10-s range takes 3+ s.
        "extra_filters":   {"rack_id": "rack=0", "module_id": "module=0"},
        "timeout":         15,
    }
}

# Router hot window: InfluxDB2 handles last 10 min (matches config.fractal.yaml)
ROUTER_HOT_WINDOW_S = 600   # 10 minutes
# Query window for tests: shorter than the router window so queries land in hot
HOT_WINDOW_S = 120   # 2 minutes — comfortably inside the 10-min hot window


def _reachable() -> bool:
    try:
        from urllib.request import urlopen
        urlopen(f"http://{FRACTAL}:8086/health", timeout=10)
        return True
    except Exception:
        return False


def skip_if_unreachable(fn):
    """Decorator: skip test if fractal-phil is not reachable."""
    def wrapper(self, *a, **kw):
        if not _reachable():
            self.skipTest(f"fractal-phil {FRACTAL}:8086 not reachable")
        return fn(self, *a, **kw)
    return wrapper


# ---------------------------------------------------------------------------
# InfluxDB2Backend integration
# ---------------------------------------------------------------------------

class TestInfluxDB2Integration(unittest.TestCase):

    def _backend(self):
        return InfluxDB2Backend(INFLUX_CFG)

    @skip_if_unreachable
    def test_health_returns_ok(self):
        h = self._backend().health()
        self.assertTrue(h["ok"], h)
        self.assertGreater(h["latency_ms"], 0)
        self.assertLess(h["latency_ms"], 2000)

    @skip_if_unreachable
    def test_query_returns_rows(self):
        """Query last 5 min site=0 — must return at least 1 row (WAL-resident)."""
        b = self._backend()
        to_ts   = time.time()
        from_ts = to_ts - HOT_WINDOW_S
        cols, rows = b.query("0", "unknown", from_ts, to_ts, 50)
        self.assertIsInstance(cols, list)
        self.assertGreater(len(cols), 0, "No columns returned")
        self.assertGreater(len(rows), 0, "No rows returned — is data flowing?")

    @skip_if_unreachable
    def test_query_cols_include_time(self):
        """Response columns must include a time field."""
        b = self._backend()
        to_ts   = time.time()
        from_ts = to_ts - HOT_WINDOW_S
        cols, rows = b.query("0", "unknown", from_ts, to_ts, 50)
        time_cols = {"_time", "timestamp", "ts", "time"}
        self.assertTrue(
            any(c in time_cols for c in cols),
            f"No time column in {cols}"
        )

    @skip_if_unreachable
    def test_no_rows_for_nonexistent_site(self):
        """site_id=999 should return 0 rows, not an error."""
        b = self._backend()
        to_ts   = time.time()
        from_ts = to_ts - HOT_WINDOW_S
        cols, rows = b.query("999", "unknown", from_ts, to_ts, 100)
        self.assertEqual(rows, [])

    @skip_if_unreachable
    def test_query_hot_window_under_10s(self):
        """5-min WAL query for site=0 returns in < 10 s."""
        b = self._backend()
        to_ts   = time.time()
        from_ts = to_ts - HOT_WINDOW_S
        t0 = time.monotonic()
        b.query("0", "unknown", from_ts, to_ts, 50)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 10.0,
                        f"5-min InfluxDB2 query took {elapsed:.2f}s > 10s")


# ---------------------------------------------------------------------------
# QueryRouter integration — hot path routes to InfluxDB2
# ---------------------------------------------------------------------------

class TestRouterIntegration(unittest.TestCase):

    def _router(self):
        from backends.duckdb_backend import DuckDBBackend
        cold_cfg = {
            "s3": {
                "local_path": "/srv/data/parquet-aws-sim/batteries/project=0",
                "region": "us-east-1",
            }
        }
        hot  = InfluxDB2Backend(INFLUX_CFG)
        cold = DuckDBBackend(cold_cfg)
        return QueryRouter(hot, cold, hot_window_seconds=ROUTER_HOT_WINDOW_S)

    @skip_if_unreachable
    def test_recent_query_goes_to_hot_backend(self):
        """Query within last 5 min → hot (InfluxDB2) → returns rows."""
        r = self._router()
        to_ts   = time.time()
        from_ts = to_ts - HOT_WINDOW_S
        cols, rows, backend = r.query("0", "unknown", from_ts, to_ts, 50)
        self.assertEqual(backend, "hot")
        self.assertGreater(len(rows), 0)

    @skip_if_unreachable
    def test_backend_field_is_hot_for_recent(self):
        r = self._router()
        to_ts   = time.time()
        from_ts = to_ts - HOT_WINDOW_S
        _, _, backend = r.query("0", "unknown", from_ts, to_ts, 10)
        self.assertEqual(backend, "hot")


if __name__ == "__main__":
    unittest.main(verbosity=2)
