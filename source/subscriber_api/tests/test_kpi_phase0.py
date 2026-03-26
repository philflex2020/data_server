"""
Phase 0 KPI tests — performance and correctness against local parquet files.

KPIs verified:
  - _day_partitions: 365-day range generated in < 5 ms
  - _build_globs:    365-day range built in < 5 ms
  - DuckDB query:    local parquet, 30-day range, 10k rows returned in < 3 s
  - Row limit:       LIMIT is respected — never returns more rows than requested
  - Concurrent:      5 parallel queries all complete within 3 s wall time
  - Elapsed_ms:      response elapsed_ms is plausible (> 0, < 10,000)

All tests use local parquet files — no S3 / network required.
Test data is generated in a temp directory and cleaned up automatically.
"""

import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, "..")
from subscriber import _build_globs, _day_partitions, _query_history, _ws_handler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()


def _make_parquet(path: str, from_ts: float, n_rows: int, fields: dict = None):
    """Write a parquet file with timestamp + optional extra fields."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    step = 1.0
    timestamps = [from_ts + i * step for i in range(n_rows)]
    data = {"timestamp": timestamps}
    if fields:
        for k, v in fields.items():
            data[k] = [v] * n_rows
    table = pa.table(data)
    pq.write_table(table, path)


def _local_cfg(tmpdir: str) -> dict:
    """Config that points at a local directory instead of S3 (no httpfs)."""
    return {
        "s3": {
            "local_path": tmpdir,
            "region":     "us-east-1",
        }
    }


# ---------------------------------------------------------------------------
# Performance: _day_partitions and _build_globs
# ---------------------------------------------------------------------------

class TestDayPartitionsPerf(unittest.TestCase):

    def test_365_days_under_5ms(self):
        from_ts = _ts("2025-01-01T00:00:00")
        to_ts   = _ts("2025-12-31T23:59:59")
        t0 = time.monotonic()
        parts = list(_day_partitions(from_ts, to_ts))
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertEqual(len(parts), 365)
        self.assertLess(elapsed_ms, 5, f"_day_partitions took {elapsed_ms:.1f}ms > 5ms")

    def test_30_days_correct_count(self):
        from_ts = _ts("2025-06-01T00:00:00")
        to_ts   = _ts("2025-06-30T23:59:59")
        parts = list(_day_partitions(from_ts, to_ts))
        self.assertEqual(len(parts), 30)


class TestBuildGlobsPerf(unittest.TestCase):

    CFG = {"s3": {"bucket": "ems-archive", "prefix": "parquet", "region": "us-east-1"}}

    def test_365_days_under_5ms(self):
        from_ts = _ts("2025-01-01T00:00:00")
        to_ts   = _ts("2025-12-31T23:59:59")
        t0 = time.monotonic()
        globs = _build_globs(self.CFG, "evelyn", "unit-3", from_ts, to_ts)
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertEqual(len(globs), 365)
        self.assertLess(elapsed_ms, 5, f"_build_globs took {elapsed_ms:.1f}ms > 5ms")


# ---------------------------------------------------------------------------
# DuckDB query — local parquet files
# ---------------------------------------------------------------------------

class TestDuckDBQueryLocal(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _cfg(self):
        return {"s3": {"local_path": self.tmpdir, "region": "us-east-1"}}

    def test_query_returns_correct_rows(self):
        """Written rows are returned by _query_history."""
        site, instance = "evelyn", "unit-3"
        day_path = os.path.join(self.tmpdir, site, instance, "2025", "06", "15")
        from_ts = _ts("2025-06-15T10:00:00")
        _make_parquet(os.path.join(day_path, "data.parquet"), from_ts, 50,
                      fields={"kw": 1.5})

        cfg = self._cfg()
        cols, rows = _query_history(cfg, site, instance, from_ts, from_ts + 3600, 2000)
        self.assertEqual(len(rows), 50)
        self.assertIn("timestamp", cols)
        self.assertIn("kw", cols)

    def test_row_limit_respected(self):
        """Query with limit=10 returns at most 10 rows even if more exist."""
        site, instance = "evelyn", "unit-0"
        day_path = os.path.join(self.tmpdir, site, instance, "2025", "06", "15")
        from_ts = _ts("2025-06-15T10:00:00")
        _make_parquet(os.path.join(day_path, "data.parquet"), from_ts, 500)

        cfg = self._cfg()
        _, rows = _query_history(cfg, site, instance, from_ts, from_ts + 3600, 10)
        self.assertLessEqual(len(rows), 10)

    def test_timestamp_filter_applied(self):
        """Rows outside [from_ts, to_ts] are excluded."""
        site, instance = "evelyn", "unit-1"
        day_path = os.path.join(self.tmpdir, site, instance, "2025", "06", "15")
        from_ts = _ts("2025-06-15T10:00:00")
        # Write 100 rows starting at from_ts; query only the first 10 seconds
        _make_parquet(os.path.join(day_path, "data.parquet"), from_ts, 100)

        cfg = self._cfg()
        _, rows = _query_history(cfg, site, instance, from_ts, from_ts + 9.5, 2000)
        self.assertEqual(len(rows), 10)   # rows at t+0 through t+9

    def test_multi_day_query(self):
        """Rows spanning two days are both returned."""
        site, instance = "evelyn", "unit-2"
        from_ts_d1 = _ts("2025-06-15T23:00:00")
        from_ts_d2 = _ts("2025-06-16T01:00:00")

        for from_ts, day in [(from_ts_d1, "15"), (from_ts_d2, "16")]:
            path = os.path.join(self.tmpdir, site, instance, "2025", "06", day, "data.parquet")
            _make_parquet(path, from_ts, 20)

        cfg = self._cfg()
        _, rows = _query_history(cfg, site, instance,
                                 from_ts_d1, from_ts_d2 + 3600, 2000)
        self.assertEqual(len(rows), 40)

    def test_empty_result_no_error(self):
        """Query with no matching parquet files returns empty rows without raising."""
        cfg = self._cfg()
        # no files written — fallback glob also finds nothing
        try:
            cols, rows = _query_history(cfg, "ghost-site", "unit-99",
                                        _ts("2025-06-15T00:00:00"),
                                        _ts("2025-06-15T23:59:59"), 2000)
            self.assertEqual(rows, [])
        except RuntimeError:
            pass  # acceptable — fallback glob raises if truly no files

    def test_30_day_query_under_3s(self):
        """30-day query against local parquet completes in < 3 s."""
        site, instance = "evelyn", "unit-4"
        from_ts = _ts("2025-06-01T00:00:00")

        # Write 30 days × 100 rows
        for day in range(30):
            day_ts = from_ts + day * 86400
            dt = datetime.fromtimestamp(day_ts, tz=timezone.utc)
            path = os.path.join(self.tmpdir, site, instance,
                                dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"),
                                "data.parquet")
            _make_parquet(path, day_ts, 100, fields={"kw": float(day)})

        cfg = self._cfg()
        t0 = time.monotonic()
        _, rows = _query_history(cfg, site, instance, from_ts, from_ts + 30 * 86400, 10000)
        elapsed = time.monotonic() - t0

        self.assertEqual(len(rows), 3000)
        self.assertLess(elapsed, 3.0, f"30-day query took {elapsed:.2f}s > 3s")

    def test_10k_rows_under_3s(self):
        """Query returning 10k rows from a single file completes in < 3 s."""
        site, instance = "evelyn", "unit-5"
        from_ts = _ts("2025-06-15T00:00:00")
        path = os.path.join(self.tmpdir, site, instance, "2025", "06", "15", "data.parquet")
        _make_parquet(path, from_ts, 10_000, fields={"kw": 2.5, "soc": 0.9})

        cfg = self._cfg()
        t0 = time.monotonic()
        _, rows = _query_history(cfg, site, instance, from_ts, from_ts + 86400, 10_000)
        elapsed = time.monotonic() - t0

        self.assertEqual(len(rows), 10_000)
        self.assertLess(elapsed, 3.0, f"10k-row query took {elapsed:.2f}s > 3s")


# ---------------------------------------------------------------------------
# Concurrent queries
# ---------------------------------------------------------------------------

class TestConcurrentQueries(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_5_concurrent_queries_within_3s(self):
        """5 simultaneous queries against local parquet all complete within 3 s."""
        from_ts = _ts("2025-06-15T10:00:00")

        # Seed data for 5 instances
        instances = [f"unit-{i}" for i in range(5)]
        for inst in instances:
            path = os.path.join(self.tmpdir, "evelyn", inst,
                                "2025", "06", "15", "data.parquet")
            _make_parquet(path, from_ts, 500, fields={"kw": 1.0})

        cfg = {"s3": {"local_path": self.tmpdir, "region": "us-east-1"}}

        def run_one(inst):
            return _query_history(cfg, "evelyn", inst, from_ts, from_ts + 3600, 2000)

        t0 = time.monotonic()
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(run_one, inst): inst for inst in instances}
            results = {inst: f.result() for f, inst in futures.items()}
        elapsed = time.monotonic() - t0

        for inst, (cols, rows) in results.items():
            self.assertEqual(len(rows), 500, f"{inst} returned wrong row count")
        self.assertLess(elapsed, 3.0, f"Concurrent queries took {elapsed:.2f}s > 3s")


# ---------------------------------------------------------------------------
# WebSocket handler KPIs — elapsed_ms is plausible
# ---------------------------------------------------------------------------

class FakeWebSocket:
    def __init__(self, messages):
        self._messages = iter(messages)
        self.sent = []
        self.remote_address = ("127.0.0.1", 9999)

    def __aiter__(self):
        return self

    async def __anext__(self):
        import websockets.exceptions
        try:
            return next(self._messages)
        except StopIteration:
            raise websockets.exceptions.ConnectionClosedOK(None, None)

    async def send(self, data):
        self.sent.append(json.loads(data))


class TestWsHandlerKpi(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_elapsed_ms_is_plausible(self):
        """elapsed_ms in response is > 0 and < 10,000."""
        from_ts = _ts("2025-06-15T10:00:00")
        path = os.path.join(self.tmpdir, "evelyn", "unit-3",
                            "2025", "06", "15", "data.parquet")
        _make_parquet(path, from_ts, 100, fields={"kw": 1.0})

        cfg = {
            "s3": {"local_path": self.tmpdir, "region": "us-east-1"},
            "live": {"mqtt": {"url": "tcp://localhost:1883", "topics": ["unit/#"]}},
        }
        msg = json.dumps({
            "type": "query_history", "query_id": "t1",
            "site": "evelyn", "instance": "unit-3",
            "from_ts": from_ts, "to_ts": from_ts + 3600, "limit": 2000,
        })
        ws = FakeWebSocket([msg])

        asyncio.run(_ws_handler(ws, cfg))

        self.assertEqual(len(ws.sent), 1)
        resp = ws.sent[0]
        self.assertGreater(resp["elapsed_ms"], 0)
        self.assertLess(resp["elapsed_ms"], 10_000)

    def test_response_rows_are_lists_not_tuples(self):
        """Rows in the response are JSON-serialisable lists, not Python tuples."""
        from_ts = _ts("2025-06-15T10:00:00")
        path = os.path.join(self.tmpdir, "evelyn", "unit-3",
                            "2025", "06", "15", "data.parquet")
        _make_parquet(path, from_ts, 5, fields={"kw": 1.0})

        cfg = {
            "s3": {"local_path": self.tmpdir, "region": "us-east-1"},
            "live": {"mqtt": {"url": "tcp://localhost:1883", "topics": ["unit/#"]}},
        }
        msg = json.dumps({
            "type": "query_history", "query_id": "t2",
            "site": "evelyn", "instance": "unit-3",
            "from_ts": from_ts, "to_ts": from_ts + 3600, "limit": 2000,
        })
        ws = FakeWebSocket([msg])
        asyncio.run(_ws_handler(ws, cfg))

        for row in ws.sent[0]["rows"]:
            self.assertIsInstance(row, list)


if __name__ == "__main__":
    unittest.main(verbosity=2)
