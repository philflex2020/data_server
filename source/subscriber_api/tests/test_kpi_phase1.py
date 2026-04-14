"""
Phase 1 KPI tests — QueryRouter, InfluxDB2Backend CSV parsing, DuckDBBackend,
hot_window_seconds.

No network. InfluxDB2 HTTP calls are mocked. DuckDB uses local temp parquet.

KPIs verified:
  - _merge:           5k+5k rows merged and sorted in < 50 ms
  - _parse_csv:       1000-row annotated CSV parsed in < 100 ms
  - _flux:            query string with 50 fields built in < 1 ms
  - Router dispatch:  1000 hot/cold/union decisions in < 50 ms total
  - Router union:     DuckDB cold + mocked hot merged correctly in < 500 ms
  - hot_window_seconds: 600 s window routes queries correctly
  - DuckDB no-files:  missing parquet returns ([], []) in < 50 ms
  - DuckDB limit:     LIMIT is respected end-to-end through router
"""

import io
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import os

sys.path.insert(0, "..")
from backends.duckdb_backend import DuckDBBackend
from backends.influxdb2 import InfluxDB2Backend
from router import QueryRouter, _merge


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()


def _make_parquet(path: str, from_ts: float, n_rows: int):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    step = 1.0
    table = pa.table({
        "timestamp": [from_ts + i * step for i in range(n_rows)],
        "value":     [float(i) for i in range(n_rows)],
    })
    pq.write_table(table, path)


def _local_cfg(tmpdir: str) -> dict:
    return {"s3": {"local_path": tmpdir, "region": "us-east-1"}}


def _influx_cfg() -> dict:
    return {
        "influxdb2": {
            "url": "http://localhost:8086",
            "token": "t", "org": "o", "bucket": "b",
            "measurement": "m", "site_tag": "site_id",
        }
    }


def _annotated_csv(n_rows: int) -> str:
    """Build a minimal InfluxDB2 annotated CSV with n_rows data rows."""
    lines = [
        "#group,false,false,true,true,false,false,true",
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string",
        "#default,_result,,,,,,",
        ",result,table,_start,_stop,_time,_value,_field",
    ]
    for i in range(n_rows):
        lines.append(
            f",_result,0,2026-01-01T00:00:00Z,2026-01-02T00:00:00Z,"
            f"2026-01-01T{i // 3600:02d}:{(i % 3600) // 60:02d}:{i % 60:02d}Z,"
            f"{float(i)},value"
        )
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# _merge performance and correctness
# ---------------------------------------------------------------------------

class TestMergeKpi(unittest.TestCase):

    def test_merge_5k_plus_5k_under_50ms(self):
        cold = (["timestamp", "v"], [(float(i), float(i)) for i in range(5000)])
        hot  = (["timestamp", "v"], [(float(i + 5000), float(i)) for i in range(5000)])
        t0 = time.monotonic()
        cols, rows = _merge(cold, hot)
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertEqual(len(rows), 10000)
        self.assertLess(elapsed_ms, 50, f"_merge 10k rows took {elapsed_ms:.1f}ms > 50ms")

    def test_merge_result_sorted(self):
        cold = (["timestamp", "v"], [(200.0, 2.0), (400.0, 4.0)])
        hot  = (["timestamp", "v"], [(100.0, 1.0), (300.0, 3.0)])
        _, rows = _merge(cold, hot)
        ts = [r[0] for r in rows]
        self.assertEqual(ts, sorted(ts))

    def test_merge_empty_cold_fast(self):
        cold = (["timestamp", "v"], [])
        hot  = (["timestamp", "v"], [(float(i), 0.0) for i in range(5000)])
        t0 = time.monotonic()
        _, rows = _merge(cold, hot)
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertEqual(len(rows), 5000)
        self.assertLess(elapsed_ms, 10)


# ---------------------------------------------------------------------------
# InfluxDB2Backend._parse_csv performance
# ---------------------------------------------------------------------------

class TestParseCsvKpi(unittest.TestCase):

    def test_parse_1000_rows_under_100ms(self):
        csv = _annotated_csv(1000)
        t0 = time.monotonic()
        cols, rows = InfluxDB2Backend._parse_csv(csv)
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertEqual(len(rows), 1000)
        self.assertLess(elapsed_ms, 100,
                        f"_parse_csv 1000 rows took {elapsed_ms:.1f}ms > 100ms")

    def test_parse_returns_floats(self):
        cols, rows = InfluxDB2Backend._parse_csv(_annotated_csv(5))
        val_idx = cols.index("_value")
        for row in rows:
            self.assertIsInstance(row[val_idx], float)

    def test_parse_empty_fast(self):
        t0 = time.monotonic()
        cols, rows = InfluxDB2Backend._parse_csv("")
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertEqual(cols, [])
        self.assertLess(elapsed_ms, 5)


# ---------------------------------------------------------------------------
# InfluxDB2Backend._flux performance
# ---------------------------------------------------------------------------

class TestFluxGenKpi(unittest.TestCase):

    def test_flux_50_fields_under_1ms(self):
        b = InfluxDB2Backend(_influx_cfg())
        fields = [f"field_{i}" for i in range(50)]
        t0 = time.monotonic()
        q = b._flux("site-1", "inst-1", 1000.0, 2000.0, 500, fields)
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertIn("field_0", q)
        self.assertIn("field_49", q)
        self.assertLess(elapsed_ms, 1, f"_flux with 50 fields took {elapsed_ms:.2f}ms > 1ms")


# ---------------------------------------------------------------------------
# QueryRouter dispatch performance
# ---------------------------------------------------------------------------

class TestRouterDispatchKpi(unittest.TestCase):

    NOW = 1_000_000.0

    def test_1000_dispatch_decisions_under_50ms(self):
        hot  = MagicMock()
        cold = MagicMock()
        hot.name  = "hot"
        cold.name = "cold"
        hot.query.return_value  = (["ts"], [(1.0,)])
        cold.query.return_value = (["ts"], [(0.5,)])
        r = QueryRouter(hot, cold, hot_window_seconds=600)

        with patch("router.time.time", return_value=self.NOW):
            t0 = time.monotonic()
            for i in range(1000):
                # alternate hot / cold
                from_ts = self.NOW - 300 if i % 2 == 0 else self.NOW - 1200
                to_ts   = from_ts + 60
                r.query("s", "i", from_ts, to_ts, 10)
            elapsed_ms = (time.monotonic() - t0) * 1000

        self.assertLess(elapsed_ms, 50,
                        f"1000 router dispatches took {elapsed_ms:.1f}ms > 50ms")


# ---------------------------------------------------------------------------
# hot_window_seconds routing correctness
# ---------------------------------------------------------------------------

class TestHotWindowSeconds(unittest.TestCase):

    NOW = 2_000_000.0

    def _router(self, window_s):
        hot  = MagicMock(); hot.name = "hot";  hot.query.return_value  = (["ts"], [(1.0,)])
        cold = MagicMock(); cold.name = "cold"; cold.query.return_value = (["ts"], [(0.5,)])
        return QueryRouter(hot, cold, hot_window_seconds=window_s), hot, cold

    def test_query_within_window_goes_hot(self):
        with patch("router.time.time", return_value=self.NOW):
            r, hot, cold = self._router(600)
            r.query("s", "i", self.NOW - 300, self.NOW, 10)
            hot.query.assert_called_once()
            cold.query.assert_not_called()

    def test_query_outside_window_goes_cold(self):
        with patch("router.time.time", return_value=self.NOW):
            r, hot, cold = self._router(600)
            r.query("s", "i", self.NOW - 3600, self.NOW - 1200, 10)
            cold.query.assert_called_once()
            hot.query.assert_not_called()

    def test_small_window_10s(self):
        """10 s window — query from 30 s ago goes cold."""
        with patch("router.time.time", return_value=self.NOW):
            r, hot, cold = self._router(10)
            r.query("s", "i", self.NOW - 30, self.NOW - 20, 10)
            cold.query.assert_called_once()
            hot.query.assert_not_called()

    def test_window_seconds_overrides_days(self):
        """hot_window_seconds takes precedence even when hot_window_days also passed."""
        with patch("router.time.time", return_value=self.NOW):
            hot  = MagicMock(); hot.name = "hot";  hot.query.return_value  = (["ts"], [])
            cold = MagicMock(); cold.name = "cold"; cold.query.return_value = (["ts"], [])
            r = QueryRouter(hot, cold, hot_window_days=365, hot_window_seconds=10)
            # 30 s ago — inside 365-day window but outside 10-s window → cold
            r.query("s", "i", self.NOW - 30, self.NOW - 20, 10)
            cold.query.assert_called_once()
            hot.query.assert_not_called()


# ---------------------------------------------------------------------------
# DuckDB "no files found" returns empty quickly
# ---------------------------------------------------------------------------

class TestDuckDBNoFiles(unittest.TestCase):

    def test_missing_parquet_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            b = DuckDBBackend(_local_cfg(tmpdir))
            t0 = time.monotonic()
            cols, rows = b.query("site-x", "inst-y",
                                 _ts("2026-01-01T00:00:00"),
                                 _ts("2026-01-01T01:00:00"), 100)
            elapsed_ms = (time.monotonic() - t0) * 1000
            self.assertEqual(cols, [])
            self.assertEqual(rows, [])
            self.assertLess(elapsed_ms, 50,
                            f"no-files DuckDB took {elapsed_ms:.1f}ms > 50ms")


# ---------------------------------------------------------------------------
# Router union: DuckDB cold + mocked hot — merged, limited, sorted
# ---------------------------------------------------------------------------

class TestRouterUnionKpi(unittest.TestCase):
    """
    Union path: cold parquet (2 h ago) + mocked hot (30 min ago).
    Router window = 3600 s. Query spans 3 h → must hit union.
    Timestamps are relative to a fixed NOW so the test is deterministic.
    """

    NOW = 1_800_000_000.0   # arbitrary fixed "now"
    WINDOW_S = 3600         # 1-hour hot window

    def _setup(self, n_cold: int, n_hot: int):
        tmpdir = tempfile.mkdtemp()

        # Cold parquet: rows 2 h before NOW
        cold_ts = self.NOW - 7200
        dt = datetime.fromtimestamp(cold_ts, tz=timezone.utc)
        date_path = os.path.join(
            tmpdir, "site-a", "inst-b",
            dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"),
            "data.parquet",
        )
        _make_parquet(date_path, cold_ts, n_cold)

        # Hot mock: rows 30 min before NOW
        hot_ts = self.NOW - 1800
        hot = MagicMock()
        hot.name = "hot"
        hot.query.return_value = (
            ["timestamp", "value"],
            [(hot_ts + float(i), float(i)) for i in range(n_hot)],
        )

        cold = DuckDBBackend(_local_cfg(tmpdir))
        router = QueryRouter(hot, cold, hot_window_seconds=self.WINDOW_S)
        return router, tmpdir

    def test_union_merge_correct_and_fast(self):
        router, tmpdir = self._setup(n_cold=200, n_hot=200)
        try:
            with patch("router.time.time", return_value=self.NOW):
                t0 = time.monotonic()
                cols, rows, backend = router.query(
                    "site-a", "inst-b",
                    self.NOW - 10800,   # 3 h ago — spans cold+hot boundary
                    self.NOW,
                    500,
                )
                elapsed_ms = (time.monotonic() - t0) * 1000

            self.assertEqual(backend, "union")
            self.assertGreater(len(rows), 0)
            self.assertLessEqual(len(rows), 500)
            ts_idx = cols.index("timestamp")
            ts_vals = [r[ts_idx] for r in rows if r[ts_idx] is not None]
            self.assertEqual(ts_vals, sorted(ts_vals))
            self.assertLess(elapsed_ms, 500,
                            f"union query took {elapsed_ms:.1f}ms > 500ms")
        finally:
            import shutil; shutil.rmtree(tmpdir)

    def test_union_limit_respected(self):
        router, tmpdir = self._setup(n_cold=100, n_hot=100)
        try:
            with patch("router.time.time", return_value=self.NOW):
                _, rows, _ = router.query(
                    "site-a", "inst-b",
                    self.NOW - 10800,
                    self.NOW,
                    50,
                )
            self.assertLessEqual(len(rows), 50)
        finally:
            import shutil; shutil.rmtree(tmpdir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
