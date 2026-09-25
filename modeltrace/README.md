# ModelTrace 检测服务（sub2api 定制版，0.1.15）

判断“上游号实际跑的是不是它声称的模型”。以 sidecar 容器 `sub2api-modeltrace` 跑在 sub2api 旁边，只通过内部网络与 sub2api 通信。

- 指纹算法与题库来自 [xqy2006/ModelTrace](https://github.com/xqy2006/ModelTrace)（MIT），commit `55a2e4a`，原样复制，见 `PROVENANCE.json`。
- 其余代码（调度、传输、API、测试）为 sub2api 集成而写。

## 怎么测一次

1. 用专用 API Key 调 sub2api 的 `POST /v1/responses`，带请求头 `X-ModelTrace-Probe: <MODELTRACE_SECRET>` 和 `X-ModelTrace-Account: <账号 id>`，把请求钉在这个号上（不换号、不故障转移；号不可用返回 503 `probe_account_unavailable`）。
2. 一次检测在同一个任务里连续出题，每题等回答完整输出后比对指纹：
   - 这题一致 → 判 match，结束；
   - 这题“明显像别的模型”（另一模型概率 ≥ 70%、目标 ≤ 15%、差距 ≥ 65%）→ 记 1 次不对，攒够 3 次 → 判 suspect（降智，触发自动暂停）；
   - “说不准”或没测成（上游报错、回答不完整）→ 不计数，换一题；
   - 最多发 5 次请求；到上限还没结论：有测成的题 → uncertain，一题没测成 → error。
3. 每次检测只记一条结果，`diagnostics.attempts` 里有每次请求的明细。
4. 没有总时长上限；只有“连续 600 秒没有任何输出”才断开（`idle_timeout_seconds`）。不限制输出长度。

## 调度（按“检测目标”）

- 目标 = 一个上游（同一 `upstream_cluster_id` 的多个 OpenAI Key，只测一次）或一个单独账号。只检测 OpenAI 平台（OAuth 与 API Key）。
- 每 60 秒拉一次 sub2api 内部接口 `GET /api/v1/internal/modeltrace/accounts`，得到账号、上游归属、最近 10 分钟真实请求（排除 sub2api 环境变量 `MODELTRACE_ACTIVITY_EXCLUDE_KEY_IDS` 里的 Key）。
- 活跃（10 分钟内有真实请求）：每 300 秒一次，进入活跃时立即一次，只测最近真正在用的模型；空闲：每 3600 秒一次，按目标哈希在一小时内错开，模型轮流。
- 上游：模型 = 各 Key 支持模型的并集；每次轮换 Key；Key 自身失败（401/402/403/429/5xx、账号不可用）当场换下一个 Key，超时/样本不足不换。
- 单 worker 串行，不并发。版本切换打断的检测记为 `interrupted`（不算失败），60 秒后重排。

## 降智自动暂停（`auto_pause_enabled`，默认开）

- 最终结论“可疑”→ 调 sub2api `POST /api/v1/internal/modeltrace/accounts/<id>/model-pause`，给该号**该模型**加模型级限流（reason `modeltrace_suspect`）：OAuth 号 `pause_minutes_oauth`（1440），API Key 号 `pause_minutes_apikey`（60）；上游多 Key 全部成员一起停。
- 暂停中照常检测（检测请求不受模型限流影响）；API Key 目标不晚于 `paused_recheck_seconds_apikey`（1800 秒）复测；结论“一致”→ `model-resume` 解除。只解除检测器自己加的暂停。
- 人工：`POST /accounts/<id>/run {"model"}` 立即复测；`POST /accounts/<id>/reset {"model"}` 解除暂停并记一条中性的 `reset` 结果（不算失败/可疑）。

## HTTP 接口（全部需要 `Authorization: Bearer <MODELTRACE_SECRET>`）

| 路由 | 用途 |
|---|---|
| `GET /health` | 版本、worker 状态 |
| `GET /accounts`、`GET /accounts/<id>` | 每个目标的状态：`models`、`next_model`、`per_model`（每模型最新与最近 12 次）、`cluster_*`、`member_account_ids`；上游任一成员 id 都返回上游数据 |
| `POST /accounts/<id>/run` | 立即检测，可带 `{"model": "..."}`；400 `model_not_supported` / 409 `already_queued` / 404 |
| `GET /v1/monitors/<id>`、`POST /v1/monitors/<id>/run` | 渠道状态页用的旧接口；快照含 `accounts_summary`（只有计数）；手动检测会让该模型的每个目标各测一次 |

sub2api 管理端代理：`/api/v1/admin/modeltrace/accounts[/<id>][/run]`。

## 配置

`config.example.json` 是完整示例（部署时放到挂载目录里的 `config.json`，只读挂载进容器 `/run/modeltrace`）。密钥 `MODELTRACE_SECRET` 来自环境变量，sub2api 与 ModelTrace 共用。

## 测试

```bash
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -p no:cacheprovider -q -o addopts=""
```

228 个 skip 是已停用的旧规则测试，保留作记录。

## 构建镜像

```bash
docker build -t modeltrace-service:local .
```

基础镜像按摘要锁定为 linux/amd64；ARM 服务器请把 `Dockerfile` 第一行换成 `python:3.12-slim`（或对应架构的摘要）。
