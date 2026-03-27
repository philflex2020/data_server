"""
Phase 1 unit tests — QueryRouter, InfluxDB2Backend CSV parsing, DuckDBBackend.

No network. All backends are mocked or use local temp parquet files.

Tests:
  _merge                   — union result assembly and timestamp sort
  QueryRouter              — hot/cold/union dispatch for 6 time-range scenarios
                             limit split in union, respects total limit
  InfluxDB2Backend._flux   — Flux query string construction
  InfluxDB2Backend._parse_csv — annotated CSV → (cols, rows)
  DuckDBBackend            — verify it satisfies QueryBackend protocol
"""

import sys
import time
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, "..")
from backends.base import QueryBackend
from backends.duckdb_backend import DuckDBBackend
from backends.influxdb2 import InfluxDB2Backend
from router import QueryRouter, _merge


# ---------------------------------------------------------------------------
# _merge
# ---------------------------------------------------------------------------

class TestMerge(unittest.TestCase):

    def test_empty_cold_returns_hot(self):
        hot = (["ts", "v"], [(200.0, 2.0), (201.0, 2.1)])
        cold = (["ts", "v"], [])
        cols, rows = _merge(cold, hot)
        self.assertEqual(rows, hot[1])

    def test_empty_hot_returns_cold(self):
        cold = (["ts", "v"], [(100.0, 1.0)])
        hot  = (["ts", "v"], [])
        cols, rows = _merge(cold, hot)
        self.assertEqual(rows, cold[1])

    def test_rows_sorted_by_timestamp(self):
        cold = (["timestamp", "v"], [(100.0, 1.0), (102.0, 1.2)])
        hot  = (["timestamp", "v"], [(101.0, 1.1), (103.0, 1.3)])
        _, rows = _merge(cold, hot)
        ts = [r[0] for r in rows]
        self.assertEqual(ts, sorted(ts))

    def test_total_row_count(self):
        cold = (["ts", "v"], [(float(i), float(i)) for i in range(5)])
        hot  = (["ts", "v"], [(float(i+10), float(i)) for i in range(5)])
        _, rows = _merge(cold, hot)
        self.assertEqual(len(rows), 10)

    def test_different_columns_aligned(self):
        cold = (["ts", "a"], [(1.0, "x")])
        hot  = (["ts", "b"], [(2.0, "y")])
        cols, rows = _merge(cold, hot)
        # cold column order is authoritative
        self.assertEqual(cols, ["ts", "a"])
        # hot row: b not in cold cols → None for "a"
        hot_row = [r for r in rows if r[0] == 2.0][0]
        self.assertIsNone(hot_row[1])


# ---------------------------------------------------------------------------
# QueryRouter
# ---------------------------------------------------------------------------

def _mock_backend(name, rows=None):
    b = MagicMock()
    b.name = name
    b.query.return_value = (["ts", "v"], rows or [(1.0, 1.0)])
    return b


class TestQueryRouter(unittest.TestCase):

    NOW = 1_000_000.0   # fixed "now"

    def _router(self, window_days=1, window_seconds=None):
        hot  = _mock_backend("hot")
        cold = _mock_backend("cold")
        if window_seconds is not None:
            r = QueryRouter(hot, cold, hot_window_seconds=window_seconds)
        else:
            r = QueryRouter(hot, cold, hot_window_days=window_days)
        return r, hot, cold

    def _cutoff(self, window_days=1):
        return self.NOW - window_days * 86400

    def _with_now(self, fn, window_days=1):
        """Run fn with time.time() patched to NOW."""
        with patch("router.time.time", return_value=self.NOW):
            return fn(window_days)

    # --- routing decisions ---

    def test_entirely_hot(self):
        def go(w):
            r, hot, cold = self._router(w)
            cutoff = self._cutoff(w)
            r.query("s", "i", cutoff + 100, cutoff + 200, 100)
            hot.query.assert_called_once()
            cold.query.assert_not_called()
        self._with_now(go)

    def test_entirely_cold(self):
        def go(w):
            r, hot, cold = self._router(w)
            cutoff = self._cutoff(w)
            r.query("s", "i", cutoff - 200, cutoff - 100, 100)
            cold.query.assert_called_once()
            hot.query.assert_not_called()
        self._with_now(go)

    def test_union_when_spanning_boundary(self):
        def go(w):
            r, hot, cold = self._router(w)
            cutoff = self._cutoff(w)
            r.query("s", "i", cutoff - 100, cutoff + 100, 100)
            cold.query.assert_called_once()
            hot.query.assert_called_once()
        self._with_now(go)

    def test_from_ts_exactly_at_cutoff_goes_hot(self):
        def go(w):
            r, hot, cold = self._router(w)
            cutoff = self._cutoff(w)
            r.query("s", "i", cutoff, cutoff + 100, 100)
            hot.query.assert_called_once()
            cold.query.assert_not_called()
        self._with_now(go)

    def test_to_ts_just_below_cutoff_goes_cold(self):
        def go(w):
            r, hot, cold = self._router(w)
            cutoff = self._cutoff(w)
            r.query("s", "i", cutoff - 200, cutoff - 0.001, 100)
            cold.query.assert_called_once()
            hot.query.assert_not_called()
        self._with_now(go)

    # --- backend name returned ---

    def test_hot_backend_label(self):
        def go(w):
            r, hot, cold = self._router(w)
            cutoff = self._cutoff(w)
            _, _, name = r.query("s", "i", cutoff + 10, cutoff + 20, 10)
            self.assertEqual(name, "hot")
        self._with_now(go)

    def test_cold_backend_label(self):
        def go(w):
            r, hot, cold = self._router(w)
            cutoff = self._cutoff(w)
            _, _, name = r.query("s", "i", cutoff - 20, cutoff - 10, 10)
            self.assertEqual(name, "cold")
        self._with_now(go)

    def test_union_backend_label(self):
        def go(w):
            r, hot, cold = self._router(w)
            cutoff = self._cutoff(w)
            _, _, name = r.query("s", "i", cutoff - 10, cutoff + 10, 10)
            self.assertEqual(name, "union")
        self._with_now(go)

    # --- limit handling ---

    def test_union_limit_respected(self):
        """Total rows from union <= requested limit."""
        def go(w):
            r, hot, cold = self._router(w)
            cold.query.return_value = (["ts"], [(float(i),) for i in range(8)])
            hot.query.return_value  = (["ts"], [(float(i+100),) for i in range(8)])
            cutoff = self._cutoff(w)
            _, rows, _ = r.query("s", "i", cutoff - 10, cutoff + 10, 10)
            self.assertLessEqual(len(rows), 10)
        self._with_now(go)

    def test_union_hot_limit_reduced_by_cold_rows(self):
        """Hot query is called with limit = total_limit - cold_rows."""
        def go(w):
            r, hot, cold = self._router(w)
            cold.query.return_value = (["ts"], [(float(i),) for i in range(3)])
            hot.query.return_value  = (["ts"], [])
            cutoff = self._cutoff(w)
            r.query("s", "i", cutoff - 10, cutoff + 10, 10)
            # hot should be called with limit = 10 - 3 = 7
            _, _, hot_limit = hot.query.call_args[0][:3]
            # hot.query(site, instance, from_ts, to_ts, limit)
            hot_call_limit = hot.query.call_args[0][4]
            self.assertEqual(hot_call_limit, 7)
        self._with_now(go)

    def test_site_and_instance_forwarded(self):
        def go(w):
            r, hot, cold = self._router(w)
            cutoff = self._cutoff(w)
            r.query("evelyn", "unit-3", cutoff + 10, cutoff + 20, 100)
            call_site, call_inst = hot.query.call_args[0][:2]
            self.assertEqual(call_site, "evelyn")
            self.assertEqual(call_inst, "unit-3")
        self._with_now(go)

    # --- hot_window_seconds ---

    def test_window_seconds_routes_hot(self):
        """hot_window_seconds=600 — query within last 600 s goes to hot."""
        with patch("router.time.time", return_value=self.NOW):
            r, hot, cold = self._router(window_seconds=600)
            r.query("s", "i", self.NOW - 300, self.NOW, 10)
            hot.query.assert_called_once()
            cold.query.assert_not_called()

    def test_window_seconds_routes_cold(self):
        """hot_window_seconds=600 — query older than 600 s goes to cold."""
        with patch("router.time.time", return_value=self.NOW):
            r, hot, cold = self._router(window_seconds=600)
            r.query("s", "i", self.NOW - 7200, self.NOW - 1200, 10)
            cold.query.assert_called_once()
            hot.query.assert_not_called()

    def test_window_seconds_takes_precedence_over_days(self):
        """When both supplied, hot_window_seconds wins."""
        with patch("router.time.time", return_value=self.NOW):
            hot  = _mock_backend("hot")
            cold = _mock_backend("cold")
            # 1 day window via days, but override with 60 s
            r = QueryRouter(hot, cold, hot_window_days=1, hot_window_seconds=60)
            # Query 120 s ago — outside 60 s window → cold
            r.query("s", "i", self.NOW - 120, self.NOW - 90, 10)
            cold.query.assert_called_once()
            hot.query.assert_not_called()


# ---------------------------------------------------------------------------
# InfluxDB2Backend — Flux query construction
# ---------------------------------------------------------------------------

CFG_INFLUX = {
    "influxdb2": {
        "url": "http://localhost:8086",
        "token": "test-token",
        "org": "test-org",
        "bucket": "test-bucket",
        "measurement": "battery_cell",
        "site_tag": "site_id",
    }
}


class TestInfluxDB2Flux(unittest.TestCase):

    def _backend(self, extra=None):
        cfg = dict(CFG_INFLUX)
        if extra:
            cfg["influxdb2"] = {**cfg["influxdb2"], **extra}
        return InfluxDB2Backend(cfg)

    def test_flux_contains_bucket(self):
        b = self._backend()
        q = b._flux("0", "unit-3", 1000.0, 2000.0, 100, None)
        self.assertIn('"test-bucket"', q)

    def test_flux_contains_measurement_filter(self):
        b = self._backend()
        q = b._flux("0", "unit-3", 1000.0, 2000.0, 100, None)
        self.assertIn("battery_cell", q)

    def test_flux_contains_site_filter(self):
        b = self._backend()
        q = b._flux("site-42", "unit-3", 1000.0, 2000.0, 100, None)
        self.assertIn('"site_id"', q)
        self.assertIn('"site-42"', q)

    def test_flux_contains_limit(self):
        b = self._backend()
        q = b._flux("0", "u", 1000.0, 2000.0, 250, None)
        self.assertIn("limit(n: 250)", q)

    def test_flux_contains_pivot(self):
        b = self._backend()
        q = b._flux("0", "u", 1000.0, 2000.0, 100, None)
        self.assertIn("pivot", q)

    def test_flux_instance_filter_when_tag_configured(self):
        b = self._backend({"instance_tag": "rack_id"})
        q = b._flux("0", "rack-2", 1000.0, 2000.0, 100, None)
        self.assertIn('"rack_id"', q)
        self.assertIn('"rack-2"', q)

    def test_flux_no_instance_filter_when_tag_not_configured(self):
        b = self._backend()  # no instance_tag
        q = b._flux("0", "rack-2", 1000.0, 2000.0, 100, None)
        self.assertNotIn("rack-2", q)

    def test_flux_field_filter_when_fields_specified(self):
        b = self._backend()
        q = b._flux("0", "u", 1000.0, 2000.0, 100, ["kw", "soc"])
        self.assertIn('"kw"', q)
        self.assertIn('"soc"', q)

    def test_flux_no_field_filter_when_none(self):
        b = self._backend()
        q = b._flux("0", "u", 1000.0, 2000.0, 100, None)
        # Should not have a field-specific filter line
        self.assertNotIn('r["_field"] ==', q)

    def test_flux_site_tag_prefix_prepended(self):
        b = self._backend({"site_tag_prefix": "site="})
        q = b._flux("0", "u", 1000.0, 2000.0, 100, None)
        self.assertIn('"site=0"', q)

    def test_flux_instance_tag_prefix_prepended(self):
        b = self._backend({"instance_tag": "rack_id", "instance_tag_prefix": "rack="})
        q = b._flux("0", "rack-2", 1000.0, 2000.0, 100, None)
        self.assertIn('"rack=rack-2"', q)

    def test_flux_no_prefix_by_default(self):
        b = self._backend()
        q = b._flux("site-42", "u", 1000.0, 2000.0, 100, None)
        self.assertIn('"site-42"', q)
        self.assertNotIn('="site-42"', q)

    def test_flux_extra_filters_added(self):
        b = self._backend({"extra_filters": {"rack_id": "rack=0", "module_id": "module=0"}})
        q = b._flux("0", "u", 1000.0, 2000.0, 100, None)
        self.assertIn('"rack_id"', q)
        self.assertIn('"rack=0"', q)
        self.assertIn('"module_id"', q)
        self.assertIn('"module=0"', q)

    def test_flux_no_extra_filters_by_default(self):
        b = self._backend()
        q = b._flux("0", "u", 1000.0, 2000.0, 100, None)
        self.assertNotIn("rack_id", q)
        self.assertNotIn("module_id", q)


# ---------------------------------------------------------------------------
# InfluxDB2Backend — CSV parsing
# ---------------------------------------------------------------------------

class TestInfluxDB2ParseCSV(unittest.TestCase):

    SAMPLE = """\
#group,false,false,true,true,false,false,true,true,true,true,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string,string
#default,_result,,,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cell_id,field_name,host,module_id,project_id,rack_id,site_id
,_result,0,2026-03-26T00:00:00Z,2026-03-27T00:00:00Z,2026-03-26T10:00:00Z,1.5,value,battery_cell,cell=0,capacity,host1,module=0,project=0,rack=0,site=0
,_result,0,2026-03-26T00:00:00Z,2026-03-27T00:00:00Z,2026-03-26T10:00:01Z,2.5,value,battery_cell,cell=0,capacity,host1,module=0,project=0,rack=0,site=0

"""

    def test_annotation_rows_skipped(self):
        cols, rows = InfluxDB2Backend._parse_csv(self.SAMPLE)
        self.assertNotIn("#group", str(cols))

    def test_metadata_cols_removed(self):
        cols, rows = InfluxDB2Backend._parse_csv(self.SAMPLE)
        for skip in ("result", "table", "_start", "_stop"):
            self.assertNotIn(skip, cols)

    def test_returns_data_rows(self):
        cols, rows = InfluxDB2Backend._parse_csv(self.SAMPLE)
        self.assertEqual(len(rows), 2)

    def test_numeric_coercion(self):
        cols, rows = InfluxDB2Backend._parse_csv(self.SAMPLE)
        val_idx = cols.index("_value")
        self.assertIsInstance(rows[0][val_idx], float)

    def test_empty_csv_returns_empty(self):
        cols, rows = InfluxDB2Backend._parse_csv("")
        self.assertEqual(cols, [])
        self.assertEqual(rows, [])

    def test_blank_line_between_tables(self):
        """Two tables separated by a blank line are both parsed."""
        two_tables = self.SAMPLE + """\
,result,table,_start,_stop,_time,_value,_field,_measurement,cell_id,field_name,host,module_id,project_id,rack_id,site_id
,_result,1,2026-03-26T00:00:00Z,2026-03-27T00:00:00Z,2026-03-26T11:00:00Z,3.5,value,battery_cell,cell=1,capacity,host1,module=0,project=0,rack=0,site=0

"""
        cols, rows = InfluxDB2Backend._parse_csv(two_tables)
        self.assertEqual(len(rows), 3)


# ---------------------------------------------------------------------------
# DuckDBBackend — satisfies QueryBackend protocol
# ---------------------------------------------------------------------------

class TestDuckDBBackendProtocol(unittest.TestCase):

    def _backend(self):
        return DuckDBBackend({"s3": {"local_path": "/tmp", "region": "us-east-1"}})

    def test_satisfies_protocol(self):
        b = self._backend()
        self.assertIsInstance(b, QueryBackend)

    def test_health_returns_dict_with_ok(self):
        b = self._backend()
        h = b.health()
        self.assertIn("ok", h)
        self.assertIn("latency_ms", h)
        self.assertIn("detail", h)
        self.assertTrue(h["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
