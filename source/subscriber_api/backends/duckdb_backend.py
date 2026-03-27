"""
DuckDBBackend — cold query path over local or S3 parquet files.

Path layout:  {local_path|s3_base}/{site}/{instance}/{yyyy}/{mm}/{dd}/*.parquet
Config keys (under s3:):
  local_path   — if set, use local filesystem (no httpfs); overrides bucket
  bucket       — S3 bucket name
  prefix       — optional S3 key prefix (stripped of leading/trailing /)
  endpoint_url — MinIO or custom S3 endpoint
  region       — AWS region (default us-east-1)
  access_key / secret_key
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import duckdb

log = logging.getLogger(__name__)


def _day_partitions(from_ts: float, to_ts: float):
    dt = datetime.fromtimestamp(from_ts, tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = datetime.fromtimestamp(to_ts, tz=timezone.utc)
    while dt <= end:
        yield dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
        dt += timedelta(days=1)


class DuckDBBackend:
    """Reads date-partitioned parquet files — local or S3."""

    name = "duckdb"

    def __init__(self, cfg: dict):
        self._cfg = cfg   # full config dict; reads cfg["s3"]

    # ------------------------------------------------------------------

    def _make_conn(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        s3 = self._cfg["s3"]
        if s3.get("local_path"):
            return conn
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        if s3.get("endpoint_url"):
            netloc = urlparse(s3["endpoint_url"]).netloc
            conn.execute(f"SET s3_endpoint='{netloc}';")
            conn.execute("SET s3_use_ssl=false;")
            conn.execute("SET s3_url_style='path';")
        conn.execute(f"SET s3_region='{s3.get('region', 'us-east-1')}';")
        if s3.get("access_key"):
            conn.execute(f"SET s3_access_key_id='{s3['access_key']}';")
            conn.execute(f"SET s3_secret_access_key='{s3['secret_key']}';")
        return conn

    def _build_globs(self, site: str, instance: str,
                     from_ts: float, to_ts: float) -> list:
        s3 = self._cfg["s3"]
        local = s3.get("local_path")
        if local:
            base = os.path.join(local.rstrip("/"), site, instance)
            return [
                os.path.join(base, yyyy, mm, dd, "*.parquet")
                for yyyy, mm, dd in _day_partitions(from_ts, to_ts)
            ]
        bucket = s3["bucket"]
        prefix = s3.get("prefix", "").strip("/")
        base = "s3://{}/{}{}".format(
            bucket,
            prefix + "/" if prefix else "",
            f"{site}/{instance}",
        )
        return [
            f"{base}/{yyyy}/{mm}/{dd}/*.parquet"
            for yyyy, mm, dd in _day_partitions(from_ts, to_ts)
        ]

    def _fallback_base(self, site: str, instance: str) -> str:
        s3 = self._cfg["s3"]
        local = s3.get("local_path")
        if local:
            return os.path.join(local.rstrip("/"), site, instance)
        bucket = s3["bucket"]
        prefix = s3.get("prefix", "").strip("/")
        return "s3://{}/{}{}".format(
            bucket,
            prefix + "/" if prefix else "",
            f"{site}/{instance}",
        )

    # ------------------------------------------------------------------

    def query(self, site: str, instance: str,
              from_ts: float, to_ts: float,
              limit: int, fields: list | None = None) -> tuple[list, list]:
        conn = self._make_conn()
        globs = self._build_globs(site, instance, from_ts, to_ts)
        sources = ", ".join(f"'{g}'" for g in globs)

        field_sql = "*" if not fields else ", ".join(f'"{f}"' for f in fields)

        try:
            result = conn.execute(f"""
                SELECT {field_sql}
                FROM read_parquet([{sources}], union_by_name=true)
                WHERE timestamp >= {from_ts} AND timestamp <= {to_ts}
                ORDER BY timestamp
                LIMIT {limit}
            """)
            cols = [d[0] for d in result.description]
            rows = result.fetchall()
        except Exception as exc:
            if "No files found" in str(exc):
                conn.close()
                return [], []
            log.warning("DuckDB partition query failed (%s) — trying instance glob", exc)
            base = self._fallback_base(site, instance)
            try:
                result = conn.execute(f"""
                    SELECT {field_sql}
                    FROM read_parquet('{base}/*/*/*/*.parquet', union_by_name=true)
                    WHERE timestamp >= {from_ts} AND timestamp <= {to_ts}
                    ORDER BY timestamp
                    LIMIT {limit}
                """)
                cols = [d[0] for d in result.description]
                rows = result.fetchall()
            except Exception as fallback_exc:
                conn.close()
                # "No files found" means no archived data for this range — return empty
                if "No files found" in str(fallback_exc):
                    return [], []
                raise RuntimeError(str(fallback_exc)) from fallback_exc

        conn.close()
        return cols, rows

    def health(self) -> dict:
        t0 = time.monotonic()
        try:
            conn = self._make_conn()
            conn.execute("SELECT 1").fetchone()
            conn.close()
            return {
                "ok": True,
                "latency_ms": int((time.monotonic() - t0) * 1000),
                "detail": "duckdb connect ok",
            }
        except Exception as exc:
            return {"ok": False, "latency_ms": -1, "detail": str(exc)}
