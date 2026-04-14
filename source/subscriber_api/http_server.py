"""
HTTP server for subscriber_api — plain asyncio, no extra dependencies.

Endpoints (port 8768 by default):
  GET /query?site=X&instance=Y&from_ts=F&to_ts=T&limit=N[&fields=a,b]
      → {"columns":[...],"rows":[...],"total":N,"elapsed_ms":N,"backend":"..."}

  GET /health
      → {"ok":true,"hot_backend":{...},"cold_backend":{...},"hot_window_seconds":N}

  GET /live/status
      → {"connected":bool,"broker":"tcp://...","topics":["unit/#"]}
"""

import asyncio
import json
import logging
import time
from urllib.parse import parse_qs, urlparse

log = logging.getLogger(__name__)

_STATUS_TEXT = {200: "OK", 400: "Bad Request", 404: "Not Found",
                500: "Internal Server Error"}

# Live feed state — updated by subscriber.py via set_live_status()
_live: dict = {"connected": False, "broker": "", "topics": []}


def set_live_status(connected: bool, broker: str, topics: list) -> None:
    _live.update({"connected": connected, "broker": broker, "topics": topics})


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

async def _send(writer: asyncio.StreamWriter, status: int, data: dict) -> None:
    body = json.dumps(data, default=str).encode()
    head = (
        f"HTTP/1.1 {status} {_STATUS_TEXT.get(status, 'OK')}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()
    writer.write(head + body)
    await writer.drain()


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

async def _query(writer, params: dict, router, executor) -> None:
    def _p(key, default=None):
        vals = params.get(key)
        return vals[0] if vals else default

    now      = time.time()
    site     = _p("site", "unknown")
    instance = _p("instance", "unknown")
    try:
        from_ts = float(_p("from_ts", now - 300))
        to_ts   = float(_p("to_ts",   now))
        limit   = int(_p("limit",    500))
    except (TypeError, ValueError) as exc:
        await _send(writer, 400, {"error": f"Bad parameter: {exc}"})
        return

    fields_raw = _p("fields")
    fields = [f.strip() for f in fields_raw.split(",")] if fields_raw else None

    t0   = time.monotonic()
    loop = asyncio.get_running_loop()
    try:
        if router:
            cols, rows, backend = await loop.run_in_executor(
                executor, router.query, site, instance, from_ts, to_ts, limit, fields
            )
        else:
            cols, rows, backend = [], [], "none"
    except Exception as exc:
        await _send(writer, 500, {"error": str(exc)})
        return

    elapsed = int((time.monotonic() - t0) * 1000)
    await _send(writer, 200, {
        "columns":    cols,
        "rows":       [list(r) for r in rows],
        "total":      len(rows),
        "elapsed_ms": elapsed,
        "backend":    backend,
    })


async def _health(writer, router, executor) -> None:
    loop = asyncio.get_running_loop()
    if router:
        hot_h  = await loop.run_in_executor(executor, router.hot.health)
        cold_h = await loop.run_in_executor(executor, router.cold.health)
        ok = hot_h.get("ok", False) and cold_h.get("ok", False)
        await _send(writer, 200, {
            "ok":                 ok,
            "hot_backend":        {"name": router.hot.name,  **hot_h},
            "cold_backend":       {"name": router.cold.name, **cold_h},
            "hot_window_seconds": router._window_s,
        })
    else:
        await _send(writer, 200, {"ok": False, "detail": "router not configured"})


async def _live_status(writer) -> None:
    await _send(writer, 200, dict(_live))


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------

async def _dispatch(reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter,
                    router, executor) -> None:
    try:
        line = await asyncio.wait_for(reader.readline(), timeout=5.0)
        if not line:
            return
        parts = line.decode(errors="replace").split()
        if len(parts) < 2:
            return
        method, raw_path = parts[0], parts[1]

        # Drain remaining headers
        while True:
            hline = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if hline in (b"\r\n", b"\n", b""):
                break

        parsed = urlparse(raw_path)
        path   = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query, keep_blank_values=False)

        if method != "GET":
            await _send(writer, 400, {"error": "Only GET supported"})
            return

        if path == "/query":
            await _query(writer, params, router, executor)
        elif path == "/health":
            await _health(writer, router, executor)
        elif path in ("/live/status", "/live"):
            await _live_status(writer)
        else:
            await _send(writer, 404, {"error": f"Not found: {path}"})

    except asyncio.TimeoutError:
        pass
    except Exception as exc:
        log.debug("HTTP dispatch error: %s", exc)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------------------

async def start(host: str, port: int, router, executor=None):
    """Start the HTTP server and return the asyncio.Server object."""
    async def _handler(reader, writer):
        await _dispatch(reader, writer, router, executor)

    server = await asyncio.start_server(_handler, host, port)
    log.info("HTTP API  http://%s:%d", host, port)
    return server
