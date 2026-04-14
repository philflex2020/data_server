"""
QueryBackend — protocol every backend must implement.

query() returns (column_names, rows) so the router and callers
are backend-agnostic. All backends raise on error; callers catch.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class QueryBackend(Protocol):

    def query(
        self,
        site:     str,
        instance: str,           # unit/device within site; backends may ignore if not applicable
        from_ts:  float,         # unix seconds UTC
        to_ts:    float,
        limit:    int,
        fields:   list | None = None,   # None = all fields
    ) -> tuple[list, list]:
        """Return (column_names, rows). Raises RuntimeError on failure."""
        ...

    def health(self) -> dict:
        """Return {"ok": bool, "latency_ms": int, "detail": str}."""
        ...
