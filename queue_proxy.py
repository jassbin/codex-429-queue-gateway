# -*- coding: utf-8 -*-
"""
Codex local queue gateway — fixes upstream `429 Too Many Requests` for third-party
OpenAI-compatible gateways.

Listens on 127.0.0.1:15731 and forwards everything to the upstream host:
  1. Concurrency queue: at most MAX_INFLIGHT requests hit the upstream at once,
     the rest wait locally (instead of being rejected with 429).
  2. Auto-retry: on 429/503 the gateway retries with exponential backoff —
     a rejected request never started, so retrying costs nothing. Codex never sees a 429.
  3. Byte-level streaming passthrough: SSE event streams are relayed untouched.

Usage:
  pip install aiohttp
  pythonw queue_proxy.py          # detached, no console
  # then point Codex at it:
  #   base_url = "http://127.0.0.1:15731/openai/v1"

Log: ~/.codex/queue_proxy.log
"""
import asyncio
import os
import time

import aiohttp
from aiohttp import web, ClientSession, ClientTimeout

UPSTREAM = "https://chatapi.weixin.qq.com"   # <-- your upstream base host (no path)
LISTEN_PORT = 15731
MAX_INFLIGHT = 4          # upstream concurrency cap (provider limit is 6; keep headroom)
MAX_QUEUE_WAIT = 1800     # max seconds a request may wait in the local queue
RETRY_429_MAX = 40        # max retries on upstream 429/503
RETRY_429_BASE = 2.0      # backoff base, seconds
RETRY_429_CAP = 30.0      # backoff cap, seconds

LOG_PATH = os.path.expanduser("~/.codex/queue_proxy.log")

sem = asyncio.Semaphore(MAX_INFLIGHT)
session: ClientSession = None
stats = {"inflight": 0, "queued": 0, "done": 0}

LOG = open(LOG_PATH, "a", encoding="utf-8")


def log(msg: str) -> None:
    line = f"[{time.strftime('%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    LOG.write(line + "\n")
    LOG.flush()


HOP_HEADERS = {"content-length", "transfer-encoding", "connection", "keep-alive",
               "proxy-authenticate", "proxy-authorization", "te", "trailers", "upgrade"}


async def forward(request: web.Request) -> web.StreamResponse:
    rel = request.match_info.get("path", "")
    url = f"{UPSTREAM}/{rel}"
    body = await request.read()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in HOP_HEADERS | {"host"}}

    t0 = time.time()
    stats["queued"] += 1
    backoff = RETRY_429_BASE
    attempt = 0
    while True:
        # 1) local queue: wait for a slot instead of letting upstream reject us
        try:
            await asyncio.wait_for(sem.acquire(), timeout=MAX_QUEUE_WAIT)
        except asyncio.TimeoutError:
            return web.json_response(
                {"error": {"message": "queue wait timeout", "code": 429}}, status=429)
        stats["inflight"] += 1
        try:
            timeout = ClientTimeout(total=None, connect=30, sock_read=3600)
            async with session.request(request.method, url, data=body,
                                       headers=headers) as up:
                # 2) transparent retry on 429/503 (rejected request never started)
                if up.status in (429, 503) and attempt < RETRY_429_MAX:
                    up.release()
                    attempt += 1
                    wait_s = min(backoff, RETRY_429_CAP)
                    log(f"!!! upstream {up.status}, retry #{attempt} in {wait_s:.0f}s  /{rel}")
                    backoff = min(backoff * 1.5, RETRY_429_CAP)
                    await asyncio.sleep(wait_s)
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
                return resp
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            if attempt < RETRY_429_MAX:
                attempt += 1
                wait_s = min(backoff, RETRY_429_CAP)
                log(f"!!! upstream error {type(e).__name__}: {str(e)[:80]}, "
                    f"retry #{attempt} in {wait_s:.0f}s")
                backoff = min(backoff * 1.5, RETRY_429_CAP)
                await asyncio.sleep(wait_s)
                continue
            log(f"!!! giving up after {attempt} attempts: {type(e).__name__}: {str(e)[:120]}")
            return web.json_response(
                {"error": {"message": f"upstream error: {type(e).__name__}", "code": 502}},
                status=502)
        finally:
            stats["inflight"] -= 1
            sem.release()


async def main() -> None:
    global session
    session = ClientSession()
    app = web.Application(client_max_size=1024 ** 3)  # huge request bodies allowed
    app.router.add_route("*", "/{path:.*}", forward)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", LISTEN_PORT)
    await site.start()
    log(f"queue gateway up: 127.0.0.1:{LISTEN_PORT} -> {UPSTREAM}  max_inflight={MAX_INFLIGHT}")
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("gateway exiting")
