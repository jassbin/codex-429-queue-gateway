# Codex Desktop + Third-Party Model: Fixing `429 Too Many Requests` with a Local Queue Gateway

> A field-tested deep-dive into why OpenAI Codex Desktop (paired with a third-party OpenAI-compatible gateway) throws
> `exceeded retry limit, last status: 429 Too Many Requests`, why proxy clients (CC Switch / Clash Verge) seem to "magically"
> prevent it, and how a ~200-line local queue gateway fixes it permanently — without changing how you work.
>
> Every conclusion below was verified with HTTP probes, stress tests, and app log forensics. No speculation.

中文版：[README.zh-CN.md](./README.zh-CN.md)

---

## TL;DR

- The `429` is **not** a network problem. A `429` means your request **reached the server** and was rejected for
  **too many concurrent requests** (`concurrent limit exceeded: running=6 max=6`).
  Network-level blocks look like TCP RST (`os error 10054`) or timeouts — they never carry an HTTP status code.
- The upstream gateway enforces a **hard concurrency cap of 6 per API key**, and **releases slots very slowly**
  (even successfully finished requests keep their slot for minutes — "phantom occupancy").
- Proxy tools like CC Switch never triggered it because they run a **local forwarding proxy that smooths out request
  bursts**. Going "direct" removes that buffer, and Codex's own retry loop (`request_max_retries = 10`) then
  **DDoS-es the slot pool with itself**.
- Fix: put the buffering layer back yourself — a tiny local gateway that (1) caps concurrency at 4 with local queuing,
  (2) transparently retries on 429/503 with exponential backoff, (3) passes SSE streaming through untouched.
  Codex never sees a 429 again.

---

## 1. The four operating modes, compared

| Mode | Model protocol | 429 risk | Notes |
|---|---|---|---|
| ① Proxy client takeover (e.g. CC Switch) | Chat Completions (converted by Codex internally) | Almost never fires | Local proxy smooths bursts; also rewrites your whole `config.toml` |
| ② Direct, no ChatGPT login (API key only) | **Responses API, native, zero conversion** | **High** (no buffer) | `requires_openai_auth = false` |
| ③ Direct + ChatGPT login (dual-auth, recommended) | Same as ② — plugins & login state fully preserved | Solved by the gateway below | `requires_openai_auth = true` + `experimental_bearer_token` |
| ④ Phone → PC remote control | Same as ③ | Same as ③ | Additionally **requires TUN mode** (see §4) |

All modes hit the same upstream: an OpenAI-compatible gateway serving a DeepSeek-class model, exposing both
`/v1/chat/completions` and `/v1/responses`.

## 2. Anatomy of the 429 (all claims experiment-verified)

### 2.1 What the server actually says

The retry-exhausted error in the UI wraps this 77-byte body:

```json
{"error":{"message":"concurrent limit exceeded: running=6 max=6","code":429}}
```

### 2.2 Stress-test results (same key, same endpoint, only payload size / concurrency varied)

| Test | Result |
|---|---|
| 12 concurrent **small** requests → `/responses` | 12/12 OK (small requests barely register on the counter) |
| 12 concurrent **small** requests → `/chat/completions` | 5 OK, 7× `429 running=6 max=6` (strict even for small) |
| 8 concurrent ~50K-char requests → `/responses` | 8/8 rejected, `running=7 max=6` |
| request body ≥ ~2.5 MB | **Connection RST** — not even an error response |
| slot release timing | **Very slow**: finished requests keep occupying slots for minutes; one burst (even a fully successful one) poisons the pool for minutes |

### 2.3 The self-inflicted death spiral

```
huge session (~200K+ tokens) → one request occupies a slot for minutes
  → Codex retries (request_max_retries = 10) → each retry adds concurrency
    → all 6 slots occupied by its own retries → everything 429s
      → it retries again → …
```

Aggravating factors: multiple desktop windows **and** the phone remote-control session all share the same key's
6 slots; concurrent-limit quota is bound to the **API key** (a rotated key = a fresh pool).

### 2.4 Why the proxy client "never had this problem"

Log forensics over ~4 months: **zero 429s** while the proxy client's local forwarder was in the path; the first 429s
appeared within hours of switching to a direct connection. The forwarder sits at `127.0.0.1:<port>` and forwards
sequentially — the upstream never sees bursts. Same gateway, same provider — the buffer was the difference.

## 3. The fix: a ~200-line local queue gateway

[`queue_proxy.py`](./queue_proxy.py) — aiohttp, listens on `127.0.0.1:15731`, forwards to the upstream gateway:

| Layer | What it does | Verified result |
|---|---|---|
| ① Concurrency queue | Max 4 in-flight to upstream; extras wait locally | 8 concurrent mid-size requests: direct = 8/8 failed; via gateway = **8/8 OK** |
| ② 429 auto-retry | Exponential backoff (2s → 30s, up to 40 attempts) on 429/503; rejected requests cost nothing to retry | Penetrated a 12-rejections-in-a-row storm and got a 200 |
| ③ Streaming passthrough | SSE event streams relayed untouched (reasoning summaries, typewriter output) | `codex exec` streaming verified |

### Wiring it up

`~/.codex/config.toml`:

```toml
model_provider = "custom"

[model_providers.custom]
name = "third-party via local queue gateway"
wire_api = "responses"                       # native Responses API — zero protocol conversion
requires_openai_auth = true                  # keep ChatGPT login state (plugins, remote control)
base_url = "http://127.0.0.1:15731/openai/v1"
experimental_bearer_token = "<your-upstream-api-key>"   # forwarded as-is by the gateway
request_max_retries = 10
stream_max_retries = 10
```

Run it (**no source edits needed** — the upstream comes from a flag):

```bash
python queue_proxy.py --upstream https://your-gateway.example.com
# or via env:  set CODEX_GW_UPSTREAM=https://...  &&  python queue_proxy.py
# tunables:    --port 15731   --max-inflight 4
```

Autostart: put a one-liner in your Startup folder
(`pythonw.exe path\to\queue_proxy.py --upstream https://your-gateway.example.com`).
Single-instance by design (a second copy reports "port already in use" and exits cleanly).

Live stats: `curl http://127.0.0.1:15731/__stats` — current queue depth plus cumulative
done / retried / failed counters. Logs to `~/.codex/queue_proxy.log`.

Rollback: point `base_url` back at the upstream and kill the process.

### Gotchas learned the hard way

| Pitfall | Lesson |
|---|---|
| Config edits "do nothing" | **Codex Desktop reads `config.toml` only at startup** — fully quit and relaunch after every edit (force-kill is safe; sessions are persisted) |
| `Connection refused` | Gateway process is down — restart it |
| `Model provider custom not found` after quitting the proxy client | It writes its backup snapshot back into `config.toml` on exit — keep manual backups; don't let it manage Codex once you've gone manual |
| Spinner with no error after closing the proxy/VPN | Leftover system proxy pointing at a dead local port — clear `ProxyEnable` before exiting, then restart Codex |
| Missing reasoning summaries | Model-catalog `default_reasoning_summary: "none"` overrides — set top-level `model_reasoning_summary = "detailed"` |

## 4. Bonus: phone → PC remote control requires TUN

The desktop app's remote-control WebSocket (`wss://…/backend-api/wham/remote/control/server`) **does not read the
system proxy** — unlike its HTTPS traffic (which does, and worked fine). Direct WS connection attempts get RST'd by
the GFW (verified: same URL through the proxy completes a normal handshake; direct gets `os error 10054` — identical
to the app's logs, 7,959 consecutive failures over a week).

**TUN mode** (virtual NIC capturing all traffic regardless of per-app proxy awareness) fixes it: first-ever
`remote control websocket: Connected` immediately after enabling TUN, phone-to-PC control verified working.
It coexists fine with the direct third-party-model traffic.

## 5. Quick troubleshooting table

| Symptom | Cause | Action |
|---|---|---|
| 429 even via gateway | Phantom slots draining | Wait 3–5 min; the gateway retries automatically. Don't spam the send button |
| 429, gateway log empty | Desktop app not restarted after config edit, or proxy client rewrote config | Restart the app; check `base_url` |
| Connection refused | Gateway down | Start it; check its log |
| Infinite spinner, no error | Leftover dead system proxy | Clear `ProxyEnable`, restart Codex |
| Phone can't reach PC | TUN off | Enable TUN, restart the proxy core |
| No reasoning summaries | Catalog override | `model_reasoning_summary = "detailed"` at top level |

## 6. Protocol note: no conversion anywhere

- Codex with `wire_api = "responses"` speaks **OpenAI Responses API** natively (`POST /responses`, SSE events
  `response.output_text.delta`, `response.completed`, …).
- The upstream gateway implements Responses natively as well (responses carry `object: "response"`, `resp_*` ids).
- The queue gateway is a byte-level transparent pipe — it never rewrites payloads. With the older
  `wire_api = "chat"` mode (used under proxy-client takeover), Codex itself converts to Chat Completions internally.

---

*Everything in this document was reproduced experimentally on 2026-10-02. Your upstream's exact quota rules may
differ — the diagnosis method (capture the 429 body, bisect by payload size, then stress concurrency) transfers
to any OpenAI-compatible gateway.*
