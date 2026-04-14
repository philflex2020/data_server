"""
InfluxDB2Backend — hot query path via Flux over HTTP.

Config (under influxdb2:):
  url     — e.g. http://localhost:8086
  token   — InfluxDB2 auth token
  org     — InfluxDB2 org name
  bucket  — bucket name
  measurement  — measurement to query (default: battery_cell)
  site_tag     — tag name that maps to the 'site' API param (default: site_id)
  instance_tag — tag name that maps to 'instance'; omit to skip filter (optional)
  timeout  — HTTP timeout seconds (default: 10)

The raw InfluxDB2 schema stores each field as a separate row
(_field = "timestamp" | "value"). We pivot to get one row per
measurement point with all fields as columns.
"""

import csv
import io
import logging
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError

log = logging.getLogger(__name__)


def _unix_to_rfc3339(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class InfluxDB2Backend:

    name = "influxdb2"

    def __init__(self, cfg: dict):
        c = cfg["influxdb2"]
        self._url         = c["url"].rstrip("/")
        self._token       = c["token"]
        self._org         = c["org"]
        self._bucket      = c["bucket"]
        self._measurement      = c.get("measurement", "battery_cell")
        self._site_tag         = c.get("site_tag", "site_id")
        self._site_tag_prefix  = c.get("site_tag_prefix", "")  # e.g. "site=" when tag stores "site=0"
        self._inst_tag         = c.get("instance_tag")   # None = no instance filter
        self._inst_tag_prefix  = c.get("instance_tag_prefix", "")
        # extra_filters: {tag_name: exact_value} always added to every query.
        # Use to scope to a subsystem (e.g. rack_id, module_id) when the
        # measurement has very high series cardinality.
        self._extra_filters    = c.get("extra_filters", {})
        self._timeout          = int(c.get("timeout", 10))

    # ------------------------------------------------------------------

    def _flux(self, site: str, instance: str,
              from_ts: float, to_ts: float, limit: int,
              fields: list | None) -> str:

        start = _unix_to_rfc3339(from_ts)
        stop  = _unix_to_rfc3339(to_ts)

        lines = [
            f'from(bucket: "{self._bucket}")',
            f'  |> range(start: {start}, stop: {stop})',
            f'  |> filter(fn: (r) => r["_measurement"] == "{self._measurement}")',
            f'  |> filter(fn: (r) => r["{self._site_tag}"] == "{self._site_tag_prefix}{site}")',
        ]

        if self._inst_tag and instance and instance != "unknown":
            lines.append(
                f'  |> filter(fn: (r) => r["{self._inst_tag}"] == "{self._inst_tag_prefix}{instance}")'
            )

        for tag, value in self._extra_filters.items():
            lines.append(f'  |> filter(fn: (r) => r["{tag}"] == "{value}")')

        if fields:
            field_list = " or ".join(f'r["_field"] == "{f}"' for f in fields)
            lines.append(f'  |> filter(fn: (r) => {field_list})')

        lines += [
            '  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")',
            f'  |> limit(n: {limit})',
        ]

        return "\n".join(lines)

    def _execute_flux(self, flux: str) -> tuple[list, list]:
        """POST a Flux query and parse the annotated CSV response."""
        endpoint = f"{self._url}/api/v2/query?org={self._org}"
        body = flux.encode()
        req = Request(
            endpoint,
            data=body,
            headers={
                "Authorization": f"Token {self._token}",
                "Content-Type":  "application/vnd.flux",
                "Accept":        "application/csv",
            },
            method="POST",
        )
        with urlopen(req, timeout=self._timeout) as resp:
            raw = resp.read().decode()

        return self._parse_csv(raw)

    @staticmethod
    def _parse_csv(raw: str) -> tuple[list, list]:
        """
        Parse InfluxDB2 annotated CSV (possibly multi-table).
        Skip annotation rows (start with #), blank lines, and metadata columns
        (result, table, _start, _stop).
        Blank lines reset the current-table header but accumulated rows are kept.
        Return (column_names, rows).
        """
        skip = {"result", "table", "_start", "_stop", ""}
        reader = csv.reader(io.StringIO(raw))

        all_cols = None      # column names from the first header encountered
        all_rows = []
        keep_idx = []
        cur_header = None

        for row in reader:
            if not row:
                cur_header = None    # blank line = table separator
                keep_idx = []
                continue
            if row[0].startswith("#"):
                continue             # annotation row
            if cur_header is None:
                cur_header = row
                new_idx  = [i for i, c in enumerate(row) if c.strip() not in skip]
                new_cols = [row[i].strip() for i in new_idx]
                if all_cols is None:
                    all_cols = new_cols
                keep_idx = new_idx
                continue
            # Guard: skip rows that are shorter than our widest keep_idx
            if keep_idx and max(keep_idx) >= len(row):
                continue
            all_rows.append(tuple(row[i] for i in keep_idx))

        if all_cols is None:
            return [], []

        def _coerce(v: str):
            try:
                return int(v)
            except ValueError:
                pass
            try:
                return float(v)
            except ValueError:
                return v

        all_rows = [tuple(_coerce(v) for v in r) for r in all_rows]
        return all_cols, all_rows

    # ------------------------------------------------------------------

    def query(self, site: str, instance: str,
              from_ts: float, to_ts: float,
              limit: int, fields: list | None = None) -> tuple[list, list]:
        flux = self._flux(site, instance, from_ts, to_ts, limit, fields)
        log.debug("InfluxDB2 Flux:\n%s", flux)
        try:
            return self._execute_flux(flux)
        except Exception as exc:
            raise RuntimeError(f"InfluxDB2 query failed: {exc}") from exc

    def health(self) -> dict:
        t0 = time.monotonic()
        try:
            req = Request(
                f"{self._url}/health",
                headers={"Authorization": f"Token {self._token}"},
            )
            with urlopen(req, timeout=5) as resp:
                pass
            return {
                "ok": True,
                "latency_ms": int((time.monotonic() - t0) * 1000),
                "detail": "influxdb2 /health ok",
            }
        except Exception as exc:
            return {"ok": False, "latency_ms": -1, "detail": str(exc)}
