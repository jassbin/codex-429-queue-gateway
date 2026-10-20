# -*- coding: utf-8 -*-
"""
Codex local queue gateway — fixes upstream `429 Too Many Requests` for third-party
OpenAI-compatible gateways.

Listens on 127.0.0.1:<port> (default 15731) and forwards everything to the upstream:
  1. Concurrency queue: at most MAX_INFLIGHT requests hit the upstream at once,
     the rest wait locally (instead of being rejected with 429).
  2. Auto-retry: on 429/503 the gateway retries with exponential backoff,
     honouring upstream `Retry-After` when present. Codex never sees a 429.
  3. Byte-level streaming passthrough: SSE event streams are relayed untouched.

Usage:
  pip install aiohttp
  pythonw queue_proxy.py --upstream https://your-gateway.example.com
  # or via env:  set CODEX_GW_UPSTREAM=https://...  &&  pythonw queue_proxy.py
  # then point Codex at it:
  #   base_url = "http://127.0.0.1:15731/openai/v1"

Endpoints:
  GET  /__stats   local queue statistics (JSON)
  ANY  /*         transparently forwarded to the upstream
Log: ~/.codex/queue_proxy.log
"""
import argparse
import asyncio
import os
import sys
import time

import aiohttp
from aiohttp import web, ClientSession, ClientTimeout

DEFAULT_PORT = 15731
MAX_QUEUE_WAIT = 1800     # max seconds a request may wait in the local queue
RETRY_429_MAX = 40        # max retries on upstream 429/503
RETRY_429_BASE = 2.0      # backoff base, seconds
RETRY_429_CAP = 30.0      # backoff cap, seconds

LOG_PATH = os.path.expanduser("~/.codex/queue_proxy.log")

HOP_HEADERS = {"content-length", "transfer-encoding", "connection", "keep-alive",
               "proxy-authenticate", "proxy-authorization", "te", "trailers", "upgrade"}

LOG = open(LOG_PATH, "a", encoding="utf-8")
STATS = {"inflight": 0, "waiting": 0, "done": 0, "retried": 0, "failed": 0}


def log(msg: str) -> None:
    line = f"[{time.strftime('%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    LOG.write(line + "\n")
    LOG.flush()


def retry_delay(up, backoff: float) -> float:
    """Honour upstream Retry-After (seconds or HTTP-date), else use own backoff."""
    ra = up.headers.get("Retry-After") if up is not None else None
    if ra:
        try:
            return min(max(float(ra), 1.0), RETRY_429_CAP)
        except (TypeError, ValueError):
            pass  # HTTP-date form: fall through to backoff
    return backoff


async def forward(request: web.Request) -> web.StreamResponse:
    cfg = request.app["cfg"]
    rel = request.match_info.get("path", "")
    url = f"{cfg['upstream']}/{rel}"
    body = await request.read()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in HOP_HEADERS | {"host"}}

    t0 = time.time()
    backoff = RETRY_429_BASE
    attempt = 0
    STATS["waiting"] += 1
    try:
        while True:
            # 1) local queue: wait for a slot instead of letting upstream reject us
            try:
                await asyncio.wait_for(cfg["sem"].acquire(), timeout=MAX_QUEUE_WAIT)
            except asyncio.TimeoutError:
                STATS["failed"] += 1
                return web.json_response(
                    {"error": {"message": "queue wait timeout", "code": 429}},
                    status=429)
            STATS["waiting"] -= 1
            STATS["inflight"] += 1
            try:
                timeout = ClientTimeout(total=None, connect=30, sock_read=3600)
                async with cfg["session"].request(
                        request.method, url, data=body, headers=headers) as up:
                    # 2) transparent retry on 429/503 (a rejected request never started)
                    if up.status in (429, 503) and attempt < RETRY_429_MAX:
                        wait_s = retry_delay(up, backoff)
                        up.release()
                        attempt += 1
                        STATS["retried"] += 1
                        backoff = min(backoff * 1.5, RETRY_429_CAP)
                        log(f"!!! upstream {up.status}, retry #{attempt} in "
                            f"{wait_s:.0f}s  /{rel}")
                        await asyncio.sleep(wait_s)
                        STATS["waiting"] += 1   # re-queue for the next slot
                        continue
                    # 3) streaming passthrough
                    resp_headers = {k: v for k, v in up.headers.items()
                                    if k.lower() not in HOP_HEADERS}
                    resp = web.StreamResponse(status=up.status, headers=resp_headers)
                    await resp.prepare(request)
                    async for chunk in up.content.iter_any():
                        await resp.write(chunk)
                    await resp.write_eof()
                    note = f" (succeeded after {attempt} retries)" if attempt else ""
                    log(f"<<< {up.status}  /{rel}  body={len(body)}B  "
                        f"{time.time() - t0:.1f}s{note}")
                    STATS["done"] += 1
                    return resp
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                if attempt < RETRY_429_MAX:
                    attempt += 1
                    STATS["retried"] += 1
                    wait_s = retry_delay(None, backoff)
                    backoff = min(backoff * 1.5, RETRY_429_CAP)
                    log(f"!!! upstream error {type(e).__name__}: {str(e)[:80]}, "
                        f"retry #{attempt} in {wait_s:.0f}s")
                    await asyncio.sleep(wait_s)
                    STATS["waiting"] += 1
                    continue
                STATS["failed"] += 1
                log(f"!!! giving up after {attempt} attempts: "
                    f"{type(e).__name__}: {str(e)[:120]}")
                return web.json_response(
                    {"error": {"message": f"upstream error: {type(e).__name__}",
                               "code": 502}}, status=502)
            finally:
                STATS["inflight"] -= 1
                cfg["sem"].release()
    finally:
        pass


async def stats_view(request: web.Request) -> web.Response:
    cfg = request.app["cfg"]
    return web.json_response({
        **STATS,
        "max_inflight": cfg["max_inflight"],
        "upstream": cfg["upstream"],
        "uptime_s": round(time.time() - cfg["started"], 1),
    })


async def main() -> None:
    ap = argparse.ArgumentParser(
        description="Local concurrency queue + 429 auto-retry gateway for "
                    "OpenAI-compatible upstreams.")
    ap.add_argument("--upstream",
                    default=os.environ.get("CODEX_GW_UPSTREAM", ""),
                    help="upstream base host, e.g. https://api.example.com "
                         "(no path). Env: CODEX_GW_UPSTREAM")
    ap.add_argument("--port", type=int,
                    default=int(os.environ.get("CODEX_GW_PORT", DEFAULT_PORT)),
                    help=f"local listen port (default {DEFAULT_PORT}). "
                         "Env: CODEX_GW_PORT")
    ap.add_argument("--max-inflight", type=int,
                    default=int(os.environ.get("CODEX_GW_MAX_INFLIGHT", "4")),
                    help="max concurrent upstream requests (default 4). "
                         "Env: CODEX_GW_MAX_INFLIGHT")
    args = ap.parse_args()

    if not args.upstream:
        print("error: --upstream is required.\n"
              "  example: python queue_proxy.py --upstream https://api.example.com\n"
              "  or set CODEX_GW_UPSTREAM in the environment.", file=sys.stderr)
        sys.exit(2)
    if not args.upstream.startswith(("http://", "https://")):
        print(f"error: --upstream must start with http:// or https:// "
              f"(got {args.upstream!r})", file=sys.stderr)
        sys.exit(2)

    upstream = args.upstream.rstrip("/")
    session = ClientSession()
    app = web.Application(client_max_size=1024 ** 3)  # huge request bodies allowed
    app["cfg"] = {
        "upstream": upstream,
        "max_inflight": args.max_inflight,
        "sem": asyncio.Semaphore(args.max_inflight),
        "session": session,
        "started": time.time(),
    }
    app.router.add_get("/__stats", stats_view)
    app.router.add_route("*", "/{path:.*}", forward)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", args.port)
    try:
        await site.start()
    except OSError as e:
        log(f"!!! cannot bind 127.0.0.1:{args.port} — {e}")
        print(f"error: port {args.port} is already in use. "
              f"Another instance of this gateway is probably running "
              f"(check /__stats), or pass --port <other>.",
              file=sys.stderr)
        await session.close()
        sys.exit(1)

    log(f"queue gateway up: 127.0.0.1:{args.port} -> {upstream}  "
        f"max_inflight={args.max_inflight}  (stats: /__stats)")
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("gateway exiting")
