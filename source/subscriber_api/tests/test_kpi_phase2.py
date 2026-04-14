"""
Phase 2 KPI tests — HTTP server throughput and latency.

KPIs verified:
  - /query response time < 50 ms for mocked router (no backend I/O)
  - /health response time < 10 ms for mocked backends
  - /live/status response time < 5 ms
  - 50 concurrent /query requests all complete < 2 s wall time
  - Response body is valid JSON for large result sets (10k rows)
  - Unknown path (404) responds < 5 ms
"""

import asyncio
import json
import sys
import time
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, "..")
import http_server


# ---------------------------------------------------------------------------
# Helpers (duplicated from test_phase2 to keep files independent)
# ---------------------------------------------------------------------------

def _mock_router(n_rows=2):
    r = MagicMock()
    r.hot.name  = "influxdb2"
    r.cold.name = "duckdb"
    r._window_s = 600
    r.query.return_value = (
        ["timestamp", "value"],
        [(float(i), float(i)) for i in range(n_rows)],
        "hot",
    )
    r.hot.health.return_value  = {"ok": True, "latency_ms": 1, "detail": "ok"}
    r.cold.health.return_value = {"ok": True, "latency_ms": 1, "detail": "ok"}
    return r


class _FakeWriter:
    def __init__(self):
        self._buf = b""
    def write(self, data):
        self._buf += data
    async def drain(self):
        pass
    def close(self):
        pass
    async def wait_closed(self):
        pass
    def json_body(self):
        _, _, body = self._buf.partition(b"\r\n\r\n")
        return json.loads(body)


async def _get(path: str, router=None):
    request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
    reader = asyncio.StreamReader()
    reader.feed_data(request)
    reader.feed_eof()
    writer = _FakeWriter()
    await http_server._dispatch(reader, writer, router, None)
    return writer.json_body()


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Individual endpoint latency
# ---------------------------------------------------------------------------

class TestEndpointLatency(unittest.TestCase):

    def test_query_under_50ms(self):
        r = _mock_router(n_rows=10)
        t0 = time.monotonic()
        _run(_get("/query?site=0&from_ts=1000&to_ts=2000&limit=10", r))
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertLess(elapsed_ms, 50,
                        f"/query took {elapsed_ms:.1f}ms > 50ms")

    def test_health_under_10ms(self):
        r = _mock_router()
        t0 = time.monotonic()
        _run(_get("/health", r))
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertLess(elapsed_ms, 10,
                        f"/health took {elapsed_ms:.1f}ms > 10ms")

    def test_live_status_under_5ms(self):
        http_server.set_live_status(True, "tcp://localhost:1884", ["unit/#"])
        t0 = time.monotonic()
        _run(_get("/live/status"))
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertLess(elapsed_ms, 5,
                        f"/live/status took {elapsed_ms:.1f}ms > 5ms")

    def test_404_under_5ms(self):
        t0 = time.monotonic()
        _run(_get("/unknown"))
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertLess(elapsed_ms, 5)


# ---------------------------------------------------------------------------
# Concurrent requests
# ---------------------------------------------------------------------------

class TestConcurrency(unittest.TestCase):

    def test_50_concurrent_query_under_2s(self):
        r = _mock_router(n_rows=50)

        async def run_all():
            tasks = [
                _get(f"/query?site=0&from_ts={1000+i}&to_ts={2000+i}&limit=50", r)
                for i in range(50)
            ]
            t0 = time.monotonic()
            results = await asyncio.gather(*tasks)
            return time.monotonic() - t0, results

        elapsed, results = _run(run_all())
        self.assertLess(elapsed, 2.0,
                        f"50 concurrent /query took {elapsed:.2f}s > 2s")
        for body in results:
            self.assertIn("columns", body)
            self.assertEqual(body["total"], 50)

    def test_50_concurrent_health_under_1s(self):
        r = _mock_router()

        async def run_all():
            tasks = [_get("/health", r) for _ in range(50)]
            t0 = time.monotonic()
            results = await asyncio.gather(*tasks)
            return time.monotonic() - t0, results

        elapsed, results = _run(run_all())
        self.assertLess(elapsed, 1.0,
                        f"50 concurrent /health took {elapsed:.2f}s > 1s")
        for body in results:
            self.assertTrue(body["ok"])


# ---------------------------------------------------------------------------
# Large response
# ---------------------------------------------------------------------------

class TestLargeResponse(unittest.TestCase):

    def test_10k_rows_valid_json_under_200ms(self):
        r = _mock_router(n_rows=10_000)
        t0 = time.monotonic()
        body = _run(_get("/query?site=0&from_ts=1000&to_ts=2000&limit=10000", r))
        elapsed_ms = (time.monotonic() - t0) * 1000
        self.assertEqual(body["total"], 10_000)
        self.assertEqual(len(body["rows"]), 10_000)
        self.assertLess(elapsed_ms, 200,
                        f"10k-row response took {elapsed_ms:.1f}ms > 200ms")

    def test_response_rows_are_serializable(self):
        """All row values survive JSON round-trip."""
        r = _mock_router(n_rows=100)
        body = _run(_get("/query?site=0&from_ts=1000&to_ts=2000", r))
        # Re-serialise to ensure no non-serialisable types slipped through
        reserialized = json.loads(json.dumps(body))
        self.assertEqual(len(reserialized["rows"]), 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
