"""
Phase 0 unit tests — pure logic, no network, no S3.

Tests:
  _flatten            — {value,unit} dict flattening
  _day_partitions     — date range generation
  _build_globs        — S3 path construction
  _ws_handler         — WebSocket message routing and response shape
  _broadcast          — fan-out to zero or multiple clients
"""

import asyncio
import json
import sys
import time
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, "..")
from subscriber import _broadcast, _build_globs, _day_partitions, _flatten, _ws_handler


# ---------------------------------------------------------------------------
# _flatten
# ---------------------------------------------------------------------------

class TestFlatten(unittest.TestCase):

    def test_plain_values_unchanged(self):
        row = {"kw": 1.5, "soc": 0.8, "name": "cell-1"}
        self.assertEqual(_flatten(row), row)

    def test_value_unit_dict_extracts_value(self):
        row = {"kw": {"value": 3.14, "unit": "kW"}}
        self.assertEqual(_flatten(row)["kw"], 3.14)

    def test_mixed_row(self):
        row = {"kw": {"value": 2.0, "unit": "kW"}, "ts": 1234567890}
        result = _flatten(row)
        self.assertEqual(result["kw"], 2.0)
        self.assertEqual(result["ts"], 1234567890)

    def test_empty_row(self):
        self.assertEqual(_flatten({}), {})

    def test_none_value_preserved(self):
        row = {"kw": None}
        self.assertEqual(_flatten(row)["kw"], None)

    def test_dict_without_value_key_preserved(self):
        # A dict that isn't a {value, unit} shape is kept as-is
        row = {"meta": {"a": 1, "b": 2}}
        self.assertEqual(_flatten(row)["meta"], {"a": 1, "b": 2})


# ---------------------------------------------------------------------------
# _day_partitions
# ---------------------------------------------------------------------------

class TestDayPartitions(unittest.TestCase):

    def _ts(self, iso: str) -> float:
        return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()

    def test_single_day(self):
        t = self._ts("2025-06-15T10:00:00")
        parts = list(_day_partitions(t, t + 3600))
        self.assertEqual(parts, [("2025", "06", "15")])

    def test_three_days(self):
        from_ts = self._ts("2025-06-14T23:00:00")
        to_ts   = self._ts("2025-06-16T01:00:00")
        parts = list(_day_partitions(from_ts, to_ts))
        self.assertEqual(parts, [
            ("2025", "06", "14"),
            ("2025", "06", "15"),
            ("2025", "06", "16"),
        ])

    def test_month_boundary(self):
        from_ts = self._ts("2025-01-31T12:00:00")
        to_ts   = self._ts("2025-02-01T12:00:00")
        parts = list(_day_partitions(from_ts, to_ts))
        self.assertEqual(parts, [("2025", "01", "31"), ("2025", "02", "01")])

    def test_year_boundary(self):
        from_ts = self._ts("2024-12-31T12:00:00")
        to_ts   = self._ts("2025-01-01T12:00:00")
        parts = list(_day_partitions(from_ts, to_ts))
        self.assertEqual(parts, [("2024", "12", "31"), ("2025", "01", "01")])

    def test_zero_length_range(self):
        t = self._ts("2025-06-15T10:00:00")
        parts = list(_day_partitions(t, t))
        self.assertEqual(len(parts), 1)

    def test_day_zero_padded(self):
        t = self._ts("2025-03-05T00:00:00")
        parts = list(_day_partitions(t, t))
        self.assertEqual(parts[0], ("2025", "03", "05"))


# ---------------------------------------------------------------------------
# _build_globs
# ---------------------------------------------------------------------------

class TestBuildGlobs(unittest.TestCase):

    BASE_CFG = {
        "s3": {
            "bucket": "ems-archive",
            "prefix": "parquet",
            "region": "us-east-1",
        }
    }

    def _ts(self, iso: str) -> float:
        return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp()

    def test_single_day_with_prefix(self):
        t = self._ts("2025-06-15T10:00:00")
        globs = _build_globs(self.BASE_CFG, "evelyn", "unit-3", t, t + 3600)
        self.assertEqual(len(globs), 1)
        self.assertEqual(globs[0],
            "s3://ems-archive/parquet/evelyn/unit-3/2025/06/15/*.parquet")

    def test_no_prefix(self):
        cfg = {"s3": {"bucket": "ems-archive", "region": "us-east-1"}}
        t = self._ts("2025-06-15T10:00:00")
        globs = _build_globs(cfg, "evelyn", "unit-3", t, t + 3600)
        self.assertEqual(globs[0],
            "s3://ems-archive/evelyn/unit-3/2025/06/15/*.parquet")

    def test_prefix_strips_slashes(self):
        cfg = {"s3": {"bucket": "ems-archive", "prefix": "/parquet/", "region": "us-east-1"}}
        t = self._ts("2025-06-15T10:00:00")
        globs = _build_globs(cfg, "evelyn", "unit-3", t, t + 3600)
        # Should not have double slashes
        self.assertNotIn("//", globs[0].replace("s3://", ""))

    def test_three_day_range_gives_three_globs(self):
        from_ts = self._ts("2025-06-14T23:00:00")
        to_ts   = self._ts("2025-06-16T01:00:00")
        globs = _build_globs(self.BASE_CFG, "evelyn", "unit-3", from_ts, to_ts)
        self.assertEqual(len(globs), 3)

    def test_site_and_instance_in_path(self):
        t = self._ts("2025-06-15T10:00:00")
        globs = _build_globs(self.BASE_CFG, "longbow", "rack-7", t, t + 3600)
        self.assertIn("longbow/rack-7", globs[0])

    def test_all_globs_end_with_parquet_wildcard(self):
        from_ts = self._ts("2025-06-14T00:00:00")
        to_ts   = self._ts("2025-06-16T00:00:00")
        globs = _build_globs(self.BASE_CFG, "evelyn", "unit-0", from_ts, to_ts)
        for g in globs:
            self.assertTrue(g.endswith("/*.parquet"), g)


# ---------------------------------------------------------------------------
# _broadcast
# ---------------------------------------------------------------------------

class TestBroadcast(unittest.TestCase):

    def test_broadcast_empty_clients(self):
        """Broadcast with no connected clients completes without error."""
        import subscriber
        original = subscriber._clients.copy()
        subscriber._clients.clear()
        try:
            asyncio.run(_broadcast({"type": "live", "payload": {}}))
        finally:
            subscriber._clients.update(original)

    def test_broadcast_reaches_all_clients(self):
        """Each connected client receives the message."""
        import subscriber

        sent = []

        async def run():
            async def _record(tag, x):
                sent.append((tag, x))

            ws1 = AsyncMock()
            ws2 = AsyncMock()
            ws1.send.side_effect = lambda x: sent.append(("ws1", x))
            ws2.send.side_effect = lambda x: sent.append(("ws2", x))

            original = subscriber._clients.copy()
            subscriber._clients.clear()
            subscriber._clients.update({ws1, ws2})
            try:
                await _broadcast({"type": "live", "payload": {"kw": 1.0}})
            finally:
                subscriber._clients.clear()
                subscriber._clients.update(original)

            self.assertEqual(ws1.send.call_count, 1)
            self.assertEqual(ws2.send.call_count, 1)

        asyncio.run(run())


# ---------------------------------------------------------------------------
# _ws_handler — message routing
# ---------------------------------------------------------------------------

class FakeWebSocket:
    """Minimal websocket double for testing _ws_handler."""

    def __init__(self, messages):
        self._messages = iter(messages)
        self.sent = []
        self.remote_address = ("127.0.0.1", 9999)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._messages)
        except StopIteration:
            raise websockets.exceptions.ConnectionClosedOK(None, None)

    async def send(self, data):
        self.sent.append(json.loads(data))


# need websockets for ConnectionClosedOK
import websockets.exceptions


class TestWsHandler(unittest.TestCase):

    CFG = {
        "s3": {"bucket": "test-bucket", "region": "us-east-1"},
        "live": {"mqtt": {"url": "tcp://localhost:1883", "topics": ["unit/#"]}},
    }

    def _run(self, messages, mock_result=None, mock_error=None):
        ws = FakeWebSocket([json.dumps(m) for m in messages])

        async def go():
            import subscriber

            def fake_query(cfg, site, instance, from_ts, to_ts, limit):
                if mock_error:
                    raise mock_error
                return mock_result or (["timestamp", "kw"], [(1.0, 2.0)])

            with patch("subscriber._query_history", side_effect=fake_query):
                await _ws_handler(ws, self.CFG)

        asyncio.run(go())
        return ws.sent

    def test_query_history_returns_history_type(self):
        msgs = [{"type": "query_history", "query_id": "42",
                 "site": "evelyn", "instance": "unit-3",
                 "from_ts": 1000.0, "to_ts": 2000.0, "limit": 100}]
        sent = self._run(msgs, mock_result=(["timestamp", "kw"], [(1000.0, 5.5)]))
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["type"], "history")

    def test_query_id_echoed(self):
        msgs = [{"type": "query_history", "query_id": "xyz",
                 "site": "evelyn", "instance": "unit-3",
                 "from_ts": 1000.0, "to_ts": 2000.0}]
        sent = self._run(msgs)
        self.assertEqual(sent[0]["query_id"], "xyz")

    def test_response_has_required_fields(self):
        msgs = [{"type": "query_history", "site": "evelyn", "instance": "unit-3",
                 "from_ts": 1000.0, "to_ts": 2000.0}]
        sent = self._run(msgs, mock_result=(["ts", "kw"], [(1000.0, 1.5)]))
        resp = sent[0]
        for field in ("type", "query_id", "columns", "rows", "total", "elapsed_ms", "backend"):
            self.assertIn(field, resp, f"missing field: {field}")

    def test_backend_field_is_duckdb(self):
        msgs = [{"type": "query_history", "site": "evelyn", "instance": "unit-3",
                 "from_ts": 1000.0, "to_ts": 2000.0}]
        sent = self._run(msgs)
        self.assertEqual(sent[0]["backend"], "duckdb")

    def test_row_count_matches_total(self):
        rows = [(float(i), float(i) * 0.1) for i in range(7)]
        msgs = [{"type": "query_history", "site": "evelyn", "instance": "unit-3",
                 "from_ts": 1000.0, "to_ts": 2000.0}]
        sent = self._run(msgs, mock_result=(["ts", "kw"], rows))
        self.assertEqual(sent[0]["total"], 7)
        self.assertEqual(len(sent[0]["rows"]), 7)

    def test_unknown_message_type_ignored(self):
        msgs = [{"type": "subscribe", "topic": "unit/#"}]
        sent = self._run(msgs)
        self.assertEqual(len(sent), 0)

    def test_bad_json_ignored(self):
        ws = FakeWebSocket(["{not valid json}"])

        async def go():
            await _ws_handler(ws, self.CFG)

        asyncio.run(go())
        self.assertEqual(len(ws.sent), 0)

    def test_query_error_returns_error_type(self):
        msgs = [{"type": "query_history", "site": "evelyn", "instance": "unit-3",
                 "from_ts": 1000.0, "to_ts": 2000.0}]
        sent = self._run(msgs, mock_error=RuntimeError("S3 unreachable"))
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["type"], "error")
        self.assertIn("S3 unreachable", sent[0]["message"])

    def test_error_response_has_query_id(self):
        msgs = [{"type": "query_history", "query_id": "err-1",
                 "site": "evelyn", "instance": "unit-3",
                 "from_ts": 1000.0, "to_ts": 2000.0}]
        sent = self._run(msgs, mock_error=RuntimeError("boom"))
        self.assertEqual(sent[0]["query_id"], "err-1")

    def test_site_and_instance_passed_to_query(self):
        calls = []
        msgs = [{"type": "query_history", "site": "longbow", "instance": "rack-2",
                 "from_ts": 1000.0, "to_ts": 2000.0}]
        ws = FakeWebSocket([json.dumps(m) for m in msgs])

        async def go():
            def fake_query(cfg, site, instance, from_ts, to_ts, limit):
                calls.append((site, instance))
                return ["ts"], []

            with patch("subscriber._query_history", side_effect=fake_query):
                await _ws_handler(ws, self.CFG)

        asyncio.run(go())
        self.assertEqual(calls[0], ("longbow", "rack-2"))

    def test_default_limit_is_2000(self):
        calls = []
        msgs = [{"type": "query_history", "site": "evelyn", "instance": "unit-0",
                 "from_ts": 1000.0, "to_ts": 2000.0}]
        ws = FakeWebSocket([json.dumps(m) for m in msgs])

        async def go():
            def fake_query(cfg, site, instance, from_ts, to_ts, limit):
                calls.append(limit)
                return ["ts"], []

            with patch("subscriber._query_history", side_effect=fake_query):
                await _ws_handler(ws, self.CFG)

        asyncio.run(go())
        self.assertEqual(calls[0], 2000)


if __name__ == "__main__":
    unittest.main(verbosity=2)
