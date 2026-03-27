"""
QueryRouter — dispatches history queries to hot or cold backend
based on where the requested time range falls relative to the
hot_window boundary.

  from_ts >= cutoff          → hot only   (InfluxDB2/3)
  to_ts   <  cutoff          → cold only  (DuckDB / Athena)
  spans boundary             → cold up to cutoff + hot from cutoff, merged

Both queries in the union case run sequentially (cold first, then hot).
Results are concatenated and sorted by timestamp column if present.

Hot window sizing:
  hot_window_seconds — preferred; set to parquet flush interval (e.g. 600 s).
                       InfluxDB2 only serves data not yet written to parquet.
  hot_window_days    — legacy; converts to seconds internally.
"""

import logging
import time

log = logging.getLogger(__name__)

_TIMESTAMP_COLS = {"timestamp", "_time", "ts", "time"}


def _merge(cold: tuple, hot: tuple) -> tuple[list, list]:
    """Merge two (cols, rows) results. Cold comes first chronologically."""
    cold_cols, cold_rows = cold
    hot_cols,  hot_rows  = hot

    if not cold_rows:
        return hot_cols, hot_rows
    if not hot_rows:
        return cold_cols, cold_rows

    # Use cold column order as authoritative; hot rows reindexed to match
    cols = cold_cols
    if hot_cols == cold_cols:
        rows = list(cold_rows) + list(hot_rows)
    else:
        # Different column sets — align hot rows to cold column order
        hot_idx = {c: i for i, c in enumerate(hot_cols)}
        rows = list(cold_rows)
        for row in hot_rows:
            aligned = tuple(
                row[hot_idx[c]] if c in hot_idx else None
                for c in cols
            )
            rows.append(aligned)

    # Sort by timestamp column if present
    ts_col = next((c for c in cols if c in _TIMESTAMP_COLS), None)
    if ts_col:
        ts_idx = cols.index(ts_col)
        rows.sort(key=lambda r: (r[ts_idx] is None, r[ts_idx]))

    return cols, rows


class QueryRouter:

    def __init__(self, hot_backend, cold_backend,
                 hot_window_days: float = 90,
                 hot_window_seconds: float | None = None):
        self.hot  = hot_backend
        self.cold = cold_backend
        # hot_window_seconds takes precedence; hot_window_days kept for compat
        if hot_window_seconds is not None:
            self._window_s = hot_window_seconds
        else:
            self._window_s = hot_window_days * 86400

    @property
    def _cutoff(self) -> float:
        return time.time() - self._window_s

    def query(self, site: str, instance: str,
              from_ts: float, to_ts: float,
              limit: int, fields: list | None = None) -> tuple[list, list, str]:
        """
        Route the query and return (cols, rows, backend_name).
        backend_name is "hot", "cold", or "union".
        """
        cutoff = self._cutoff

        if from_ts >= cutoff:
            log.debug("Router: hot  site=%s inst=%s", site, instance)
            cols, rows = self.hot.query(site, instance, from_ts, to_ts, limit, fields)
            return cols, rows, "hot"

        if to_ts < cutoff:
            log.debug("Router: cold  site=%s inst=%s", site, instance)
            cols, rows = self.cold.query(site, instance, from_ts, to_ts, limit, fields)
            return cols, rows, "cold"

        # Spans boundary — split at cutoff
        log.debug("Router: union  site=%s inst=%s  cutoff=%s", site, instance, cutoff)
        cold_result = self.cold.query(site, instance, from_ts, cutoff, limit, fields)
        remaining   = max(0, limit - len(cold_result[1]))
        if remaining > 0:
            hot_result = self.hot.query(site, instance, cutoff, to_ts, remaining, fields)
        else:
            hot_result = (cold_result[0], [])
        cols, rows = _merge(cold_result, hot_result)
        return cols, rows[:limit], "union"
