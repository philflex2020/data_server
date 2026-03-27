"""
Phase 2 unit tests — HTTP server (http_server.py).

No network. Router is mocked. Tests cover:
  - GET /query  — param parsing, correct JSON shape, error paths
  - GET /health — structure, both-ok, one-down
  - GET /live/status — reflects set_live_status()
  - 404 for unknown paths
  - Bad parameter types return 400
  - POST returns 400 (GET only)
"""

import asyncio
import json
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, "..")
import http_server


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_router(cols=None, rows=None, backend="hot", hot_health=None, cold_health=None):
    r = MagicMock()
    r.hot.name  = "influxdb2"
    r.cold.name = "duckdb"
    r._window_s = 600
    r.query.return_value = (
        cols or ["_time", "value"],
        rows or [(1.0, 2.0), (2.0, 3.0)],
        backend,
    )
    r.hot.health.return_value  = hot_health  or {"ok": True,  "latency_ms": 5,  "detail": "ok"}
    r.cold.health.return_value = cold_health or {"ok": True,  "latency_ms": 2,  "detail": "ok"}
    return r


class _FakeWriter:
    """Captures bytes written by http_server._send()."""
    def __init__(self):
        self._buf = b""
        self.closed = False

    def write(self, data: bytes):
        self._buf += data

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass

    def response(self) -> dict:
        """Parse status code and JSON body from buffered response."""
        header, _, body = self._buf.partition(b"\r\n\r\n")
        status_line = header.split(b"\r\n")[0].decode()
        code = int(status_line.split()[1])
        return {"status": code, "body": json.loads(body)}


def _run(coro):
    return asyncio.run(coro)


async def _dispatch(path: str, router=None, executor=None):
    """Simulate a GET request and return the parsed response."""
    request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
    reader = asyncio.StreamReader()
    reader.feed_data(request)
    reader.feed_eof()
    writer = _FakeWriter()
    await http_server._dispatch(reader, writer, router, executor)
    return writer.response()


# ---------------------------------------------------------------------------
# GET /query
# ---------------------------------------------------------------------------

class TestQueryEndpoint(unittest.TestCase):

    def test_returns_200(self):
        r = _mock_router()
        resp = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000&limit=10", r))
        self.assertEqual(resp["status"], 200)

    def test_response_has_required_keys(self):
        r = _mock_router()
        body = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000", r))["body"]
        for key in ("columns", "rows", "total", "elapsed_ms", "backend"):
            self.assertIn(key, body, f"Missing key: {key}")

    def test_columns_forwarded(self):
        r = _mock_router(cols=["ts", "kw", "soc"])
        body = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000", r))["body"]
        self.assertEqual(body["columns"], ["ts", "kw", "soc"])

    def test_rows_are_lists_not_tuples(self):
        r = _mock_router(rows=[(1.0, 2.0)])
        body = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000", r))["body"]
        self.assertIsInstance(body["rows"][0], list)

    def test_total_matches_row_count(self):
        r = _mock_router(rows=[(1.0, 2.0), (2.0, 3.0), (3.0, 4.0)])
        body = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000", r))["body"]
        self.assertEqual(body["total"], 3)
        self.assertEqual(len(body["rows"]), 3)

    def test_backend_field_present(self):
        r = _mock_router(backend="cold")
        body = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000", r))["body"]
        self.assertEqual(body["backend"], "cold")

    def test_elapsed_ms_non_negative(self):
        r = _mock_router()
        body = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000", r))["body"]
        self.assertGreaterEqual(body["elapsed_ms"], 0)

    def test_bad_from_ts_returns_400(self):
        r = _mock_router()
        resp = _run(_dispatch("/query?site=0&from_ts=notanumber&to_ts=2000", r))
        self.assertEqual(resp["status"], 400)

    def test_bad_limit_returns_400(self):
        r = _mock_router()
        resp = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000&limit=abc", r))
        self.assertEqual(resp["status"], 400)

    def test_router_exception_returns_500(self):
        r = _mock_router()
        r.query.side_effect = RuntimeError("backend exploded")
        resp = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000", r))
        self.assertEqual(resp["status"], 500)
        self.assertIn("error", resp["body"])

    def test_no_router_returns_empty(self):
        body = _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000", None))["body"]
        self.assertEqual(body["columns"], [])
        self.assertEqual(body["rows"], [])
        self.assertEqual(body["backend"], "none")

    def test_fields_param_forwarded(self):
        r = _mock_router()
        _run(_dispatch("/query?site=0&from_ts=1000&to_ts=2000&fields=kw,soc", r))
        _, call_kwargs = r.query.call_args
        # fields is the 6th positional arg
        call_fields = r.query.call_args[0][5]
        self.assertEqual(call_fields, ["kw", "soc"])

    def test_site_and_instance_forwarded(self):
        r = _mock_router()
        _run(_dispatch("/query?site=evelyn&instance=unit-3&from_ts=1000&to_ts=2000", r))
        args = r.query.call_args[0]
        self.assertEqual(args[0], "evelyn")
        self.assertEqual(args[1], "unit-3")


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealthEndpoint(unittest.TestCase):

    def test_returns_200(self):
        resp = _run(_dispatch("/health", _mock_router()))
        self.assertEqual(resp["status"], 200)

    def test_ok_true_when_both_backends_healthy(self):
        body = _run(_dispatch("/health", _mock_router()))["body"]
        self.assertTrue(body["ok"])

    def test_ok_false_when_hot_down(self):
        r = _mock_router(hot_health={"ok": False, "latency_ms": -1, "detail": "refused"})
        body = _run(_dispatch("/health", r))["body"]
        self.assertFalse(body["ok"])

    def test_ok_false_when_cold_down(self):
        r = _mock_router(cold_health={"ok": False, "latency_ms": -1, "detail": "refused"})
        body = _run(_dispatch("/health", r))["body"]
        self.assertFalse(body["ok"])

    def test_structure_has_backend_dicts(self):
        body = _run(_dispatch("/health", _mock_router()))["body"]
        self.assertIn("hot_backend",  body)
        self.assertIn("cold_backend", body)
        self.assertIn("name", body["hot_backend"])
        self.assertIn("name", body["cold_backend"])

    def test_hot_window_seconds_present(self):
        body = _run(_dispatch("/health", _mock_router()))["body"]
        self.assertEqual(body["hot_window_seconds"], 600)

    def test_no_router_returns_ok_false(self):
        body = _run(_dispatch("/health", None))["body"]
        self.assertFalse(body["ok"])


# ---------------------------------------------------------------------------
# GET /live/status
# ---------------------------------------------------------------------------

class TestLiveStatus(unittest.TestCase):

    def setUp(self):
        http_server.set_live_status(False, "", [])

    def test_returns_200(self):
        resp = _run(_dispatch("/live/status"))
        self.assertEqual(resp["status"], 200)

    def test_reflects_set_live_status(self):
        http_server.set_live_status(True, "tcp://localhost:1884", ["unit/#"])
        body = _run(_dispatch("/live/status"))["body"]
        self.assertTrue(body["connected"])
        self.assertEqual(body["broker"], "tcp://localhost:1884")
        self.assertEqual(body["topics"], ["unit/#"])

    def test_disconnected_state(self):
        http_server.set_live_status(False, "tcp://localhost:1884", ["unit/#"])
        body = _run(_dispatch("/live/status"))["body"]
        self.assertFalse(body["connected"])

    def test_live_path_alias(self):
        resp = _run(_dispatch("/live"))
        self.assertEqual(resp["status"], 200)


# ---------------------------------------------------------------------------
# Routing / error cases
# ---------------------------------------------------------------------------

class TestRouting(unittest.TestCase):

    def test_unknown_path_returns_404(self):
        resp = _run(_dispatch("/nonexistent"))
        self.assertEqual(resp["status"], 404)

    def test_post_returns_400(self):
        async def go():
            request = b"POST /query HTTP/1.1\r\nHost: localhost\r\n\r\n"
            reader = asyncio.StreamReader()
            reader.feed_data(request)
            reader.feed_eof()
            writer = _FakeWriter()
            await http_server._dispatch(reader, writer, None, None)
            return writer.response()
        resp = _run(go())
        self.assertEqual(resp["status"], 400)

    def test_root_returns_404(self):
        resp = _run(_dispatch("/"))
        self.assertEqual(resp["status"], 404)


if __name__ == "__main__":
    unittest.main(verbosity=2)
