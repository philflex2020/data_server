"""
Phase 2 integration tests — HTTP endpoint on fractal-phil (192.168.86.51:8768).

Requires:
  - subscriber_api running on fractal-phil with config.fractal.yaml
  - HTTP server listening on :8768

Run from phil-dev:
  cd source/subscriber_api/tests
  FRACTAL=192.168.86.51 python -m pytest test_phase2_integration.py -v

Skip gracefully when fractal-phil is unreachable.

To deploy before running:
  ssh phil@192.168.86.51 'cd ~/data_server && git pull'
  ssh phil@192.168.86.51 'sudo systemctl restart subscriber-api'
"""

import json
import os
import sys
import time
import unittest
from urllib.request import Request, urlopen
from urllib.error import URLError

FRACTAL   = os.environ.get("FRACTAL", "192.168.86.51")
HTTP_BASE = f"http://{FRACTAL}:8768"


def _get(path: str, timeout: int = 10) -> dict:
    req = Request(f"{HTTP_BASE}{path}")
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _reachable() -> bool:
    try:
        _get("/health", timeout=5)
        return True
    except Exception:
        return False


def skip_if_unreachable(fn):
    def wrapper(self, *a, **kw):
        if not _reachable():
            self.skipTest(f"fractal-phil HTTP {HTTP_BASE} not reachable")
        return fn(self, *a, **kw)
    return wrapper


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealthEndpoint(unittest.TestCase):

    @skip_if_unreachable
    def test_health_returns_ok(self):
        body = _get("/health")
        self.assertIn("ok", body)
        self.assertIn("hot_backend",  body)
        self.assertIn("cold_backend", body)

    @skip_if_unreachable
    def test_health_hot_backend_name(self):
        body = _get("/health")
        self.assertEqual(body["hot_backend"]["name"], "influxdb2")

    @skip_if_unreachable
    def test_health_cold_backend_name(self):
        body = _get("/health")
        self.assertEqual(body["cold_backend"]["name"], "duckdb")

    @skip_if_unreachable
    def test_health_latency_field_present(self):
        body = _get("/health")
        self.assertIn("latency_ms", body["hot_backend"])
        self.assertIn("latency_ms", body["cold_backend"])

    @skip_if_unreachable
    def test_health_responds_under_5s(self):
        t0 = time.monotonic()
        _get("/health")
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 5.0, f"/health took {elapsed:.2f}s > 5s")


# ---------------------------------------------------------------------------
# GET /query
# ---------------------------------------------------------------------------

class TestQueryEndpoint(unittest.TestCase):

    def _query(self, **kw):
        now = time.time()
        params = {
            "site":     "0",
            "instance": "unknown",
            "from_ts":  now - 120,   # last 2 min — in InfluxDB WAL
            "to_ts":    now,
            "limit":    20,
        }
        params.update(kw)
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        return _get(f"/query?{qs}")

    @skip_if_unreachable
    def test_query_returns_200_shape(self):
        body = self._query()
        for key in ("columns", "rows", "total", "elapsed_ms", "backend"):
            self.assertIn(key, body, f"Missing key: {key}")

    @skip_if_unreachable
    def test_query_returns_rows(self):
        body = self._query()
        self.assertGreater(body["total"], 0, "No rows returned — is data flowing?")
        self.assertGreater(len(body["columns"]), 0)

    @skip_if_unreachable
    def test_query_backend_is_hot(self):
        body = self._query()
        self.assertEqual(body["backend"], "hot")

    @skip_if_unreachable
    def test_query_elapsed_ms_plausible(self):
        body = self._query()
        self.assertGreater(body["elapsed_ms"], 0)
        self.assertLess(body["elapsed_ms"], 15_000)

    @skip_if_unreachable
    def test_nonexistent_site_returns_empty(self):
        body = self._query(site="999")
        self.assertEqual(body["total"], 0)
        self.assertEqual(body["rows"], [])

    @skip_if_unreachable
    def test_rows_are_lists(self):
        body = self._query()
        if body["rows"]:
            self.assertIsInstance(body["rows"][0], list)


# ---------------------------------------------------------------------------
# GET /live/status
# ---------------------------------------------------------------------------

class TestLiveStatus(unittest.TestCase):

    @skip_if_unreachable
    def test_live_status_shape(self):
        body = _get("/live/status")
        self.assertIn("connected", body)
        self.assertIn("broker",    body)
        self.assertIn("topics",    body)

    @skip_if_unreachable
    def test_live_status_connected(self):
        body = _get("/live/status")
        self.assertTrue(body["connected"],
                        "MQTT not connected — is FlashMQ running on fractal-phil?")


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling(unittest.TestCase):

    @skip_if_unreachable
    def test_unknown_path_returns_404(self):
        try:
            _get("/nonexistent")
            self.fail("Expected HTTP error")
        except Exception as exc:
            self.assertIn("404", str(exc))

    @skip_if_unreachable
    def test_bad_from_ts_returns_400(self):
        try:
            _get("/query?site=0&from_ts=notanumber&to_ts=2000")
            self.fail("Expected HTTP error")
        except Exception as exc:
            self.assertIn("400", str(exc))


if __name__ == "__main__":
    unittest.main(verbosity=2)
