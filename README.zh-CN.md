# Codex 桌面版 + 第三方模型：用本地排队网关根治 429 Too Many Requests

> 深度复盘：为什么 Codex 桌面版配第三方 OpenAI 兼容网关会报
> `exceeded retry limit, last status: 429 Too Many Requests`，为什么代理客户端（CC Switch / Clash Verge）
> 看起来能"神奇地"避免它，以及如何用一个约 100 行的本地排队网关永久根治——不改变任何使用习惯。
>
> 所有结论均有 HTTP 探测、压测和应用日志取证支撑，无推测项。

English version: [README.md](./README.md)

---

## TL;DR

- 429 **不是网络问题**。429 意味着请求**已经到达服务器**，被以"并发数超限"拒绝
  （`concurrent limit exceeded: running=6 max=6`）。网络被墙的表现是 TCP RST（`os error 10054`）或超时——不会有任何 HTTP 状态码。
- 上游网关对每把 API key 有 **6 个并发硬上限**，且**槽位释放极慢**（成功跑完的请求也占槽数分钟——幽灵占用）。
- CC Switch 这类代理客户端从不触发，是因为它在本机跑了一个**转发代理，把请求洪峰削平了**。改直连后这层缓冲消失，
  Codex 自己的重试循环（`request_max_retries = 10`）开始**用自己的重试轰炸自己的并发池**。
- 解法：把缓冲层自己装回来——一个微型本地网关：① 并发上限 4、多余请求本地排队；② 429/503 指数退避自动重试；
  ③ SSE 流式透传一字不改。Codex 从此看不到 429。

---

## 一、四种运行模式对照

| 模式 | 模型协议 | 429 风险 | 备注 |
|---|---|---|---|
| ① 代理客户端接管（如 CC Switch） | Chat Completions（Codex 内部转换） | 几乎不触发 | 本地代理削峰；但会改写整个 config.toml |
| ② 直连、无 ChatGPT 登录（纯 API key） | **Responses API 原生，零转换** | **高**（无缓冲） | `requires_openai_auth = false` |
| ③ 直连 + ChatGPT 登录（双轨，推荐） | 同②，插件与登录态全保留 | 已被下文网关解决 | `requires_openai_auth = true` + `experimental_bearer_token` |
| ④ 手机 → 电脑远程控制 | 同③ | 同③ | 另需 **TUN 模式**（见第四节） |

所有模式打同一个上游：OpenAI 兼容网关（DeepSeek 级模型），同时提供 `/v1/chat/completions` 与 `/v1/responses`。

## 二、429 病理（全部实验验证）

### 2.1 服务端到底说了什么

UI 上"重试超限"报错包裹的是这个 77 字节响应体：

```json
{"error":{"message":"concurrent limit exceeded: running=6 max=6","code":429}}
```

### 2.2 压测结果（同 key 同端点，只变请求体大小与并发数）

| 实验 | 结果 |
|---|---|
| 12 并发**小**请求 → `/responses` | 12/12 全过（小请求几乎不占计数器）|
| 12 并发**小**请求 → `/chat/completions` | 5 过 7 拒（该接口对小请求也严格）|
| 8 并发约 5 万字符请求 → `/responses` | 8/8 全拒，`running=7 max=6` |
| 请求体 ≥ 约 2.5MB | **连接直接被 RST**——连错误响应都没有 |
| 槽位释放时机 | **极慢**：成功跑完的请求也占槽数分钟；一波并发（哪怕全成功）把池子污染数分钟 |

### 2.3 自我轰炸的死循环

```
大会话（20 万+ tokens）→ 单请求占槽数分钟
  → Codex 重试（request_max_retries = 10）→ 每次重试新增并发
    → 6 槽全被自己的重试占满 → 全部 429
      → 继续重试 → ……
```

放大因素：桌面多窗口 + 手机远程控制会话**共用同一把 key 的 6 个槽**；并发额度绑定在 **API key** 上（换 key = 换池子）。

### 2.4 为什么代理客户端"从来不报"

日志取证约 4 个月：代理客户端本地转发器在链路上时 **0 次 429**；切换直连后几小时内出现首批 429。
转发器位于 `127.0.0.1:<端口>` 顺序转发——上游永远看不到洪峰。同一个网关、同一家服务商，差别只在缓冲层。

## 三、解法：约 100 行的本地排队网关

[`queue_proxy.py`](./queue_proxy.py) —— aiohttp 实现，监听 `127.0.0.1:15731`，转发到上游网关：

| 层 | 功能 | 实测结果 |
|---|---|---|
| ① 并发排队 | 上游同时最多 4 个，其余本地等待 | 8 并发中等请求：直连 8/8 全灭；走网关 **8/8 全成** |
| ② 429 自动重试 | 429/503 指数退避（2s→30s，上限 40 次）；被拒请求重试零成本 | 实测穿透连续 12 次拒绝的 429 风暴拿到 200 |
| ③ 流式透传 | SSE 事件流原样转发（思考摘要、打字机输出不受影响） | `codex exec` 流式输出验证通过 |

### 接线

`~/.codex/config.toml`：

```toml
model_provider = "custom"

[model_providers.custom]
name = "third-party via local queue gateway"
wire_api = "responses"                       # 原生 Responses API——零协议转换
requires_openai_auth = true                  # 保留 ChatGPT 登录态（插件、远程控制可用）
base_url = "http://127.0.0.1:15731/openai/v1"
experimental_bearer_token = "<你的上游 API key>"   # 网关原样透传
request_max_retries = 10
stream_max_retries = 10
```

自启：启动文件夹放一行 `pythonw.exe <路径>\queue_proxy.py`，或用任何你喜欢的方式。单实例设计
（重复启动因端口占用自动退出）。日志写 `~/.codex/queue_proxy.log`。

回滚：`base_url` 改回上游地址，杀掉进程即可。

### 踩坑清单（每个都真实付出过时间）

| 坑 | 教训 |
|---|---|
| 改了配置"没生效" | **Codex 桌面版只在启动时读 config.toml**——每次改完必须完全退出重启（强杀无害，会话已持久化）|
| `Connection refused` | 网关进程没在跑——重启它 |
| 退出代理客户端后报 `Model provider custom not found` | 它退出时把备份快照写回 config.toml——手工配置要自己留备份链；改手工后别再让它管 Codex |
| 关代理/梯子后无限转圈无报错 | 系统代理残留指向死端口——先清 `ProxyEnable` 再退进程，然后重启 Codex |
| 思考过程不显示 | 模型目录 `default_reasoning_summary: "none"` 覆盖——顶层设 `model_reasoning_summary = "detailed"` |

## 四、附：手机遥控电脑必须开 TUN

桌面版的远程控制 WebSocket（`wss://…/backend-api/wham/remote/control/server`）**不读系统代理**——它的 HTTPS 流量读
（所以正常），WS 不读（所以直连被墙 RST）。实测：同一 URL 走代理能完成正常握手；直连立刻 `os error 10054`
——与 App 日志逐字一致（一周内连续 7959 次失败）。

**TUN 模式**（虚拟网卡从网络层接管所有流量，不管程序是否感知代理）能修：开启后立即出现首次
`remote control websocket: Connected`，手机实测遥控成功。且与第三方模型的直连流量互不干扰。

## 五、速查表

| 症状 | 原因 | 动作 |
|---|---|---|
| 走网关仍 429 | 幽灵槽位排空中 | 等 3–5 分钟，网关自动重试穿透；别狂点发送 |
| 429 且网关日志无记录 | 桌面版没重启 / 代理客户端覆写了配置 | 重启桌面版；核对 base_url |
| Connection refused | 网关没跑 | 启动网关；看日志 |
| 无限转圈无报错 | 系统代理残留死端口 | 清 `ProxyEnable`，重启 Codex |
| 手机连不上电脑 | TUN 没开 | 开 TUN，重启代理内核 |
| 思考过程不显示 | catalog 覆盖 | 顶层 `model_reasoning_summary = "detailed"` |

## 六、协议说明：全程零转换

- Codex `wire_api = "responses"` 原生说 **OpenAI Responses API**（`POST /responses`，SSE 事件
  `response.output_text.delta`、`response.completed` 等）。
- 上游网关同样原生实现 Responses（响应带 `object: "response"`、`resp_*` ID）。
- 排队网关是字节级透明管道，从不改写载荷。旧的 `wire_api = "chat"` 模式（代理客户端接管时期）才是由
  Codex 内部转换成 Chat Completions。

---

*本文所有结论于 2026-10-02 实验复现。你的上游配额规则可能不同，但诊断方法通用：抓 429 响应体 → 按请求体大小二分 → 再压并发。*
