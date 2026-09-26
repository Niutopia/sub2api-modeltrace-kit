# ModelTrace 检测服务（sub2api 集成版，0.1.19）

判断“上游号实际跑的是不是它声称的模型”。以独立容器跑在 sub2api 旁边，只通过 Docker 内网与 sub2api 通信。部署步骤见上一级 [README](../README.md)。

- 指纹算法与题库来自 [xqy2006/ModelTrace](https://github.com/xqy2006/ModelTrace)（MIT），commit `55a2e4a`，原样复制，见 `PROVENANCE.json`。
- 其余代码（调度、传输、API、测试）为 sub2api 集成而写。

## 怎么测一次

1. 用专用 API Key 调 sub2api 的 `POST /v1/responses`，带请求头 `X-ModelTrace-Probe: <MODELTRACE_SECRET>` 和 `X-ModelTrace-Account: <账号 id>`，把请求钉在这个号上（不换号、不故障转移；号不可用返回 503 `probe_account_unavailable`）。这两个请求头在 sub2api 入口处统一剥掉，不会转发给上游。
2. 一次检测在同一个任务里连续出题，每题等回答完整输出后比对指纹：
   - 这题一致 → 判 `match`，结束；
   - 这题“明显像别的模型”（另一模型概率 ≥ 70%、目标 ≤ 15%、差距 ≥ 65%）→ 记 1 次不对，攒够 3 次 → 判 `suspect`；
   - “说不准”或没测成（上游报错、回答不完整）→ 不计数，换一题；
   - 最多发 5 次请求；到上限还没结论：有测成的题 → `uncertain`，一题没测成 → `error`。
3. 每次检测只记一条结果，`diagnostics.attempts` 里有每次请求的明细。
4. 没有总时长上限；只有连续 `idle_timeout_seconds`（默认 600）秒没有任何输出才断开。不限制输出长度。
5. 推理强度：`gpt-6-astra` 默认 `low`（astra 不接受 `none`），其他模型默认 `none`；可用 `reasoning_effort_overrides` 覆盖。

## 调度

- 每 60 秒拉一次 sub2api 内部接口 `GET /api/v1/internal/modeltrace/accounts?include_inactive=1`，得到 OpenAI 账号、模型白名单、最近 10 分钟真实请求（排除 `MODELTRACE_ACTIVITY_EXCLUDE_KEY_IDS` 里的 Key）。只检测 OpenAI 平台（OAuth 与 API Key）。
- 打开账号名单/详情、手动检测入队前，会先同步 sub2api 最新保存的白名单（最多等 4 秒，失败就用上一次完整快照）。新增的模型显示“未检测”；没有指纹数据也没有别名的模型不会伪装成可测；还没保存的编辑草稿不参与检测。
- **活跃**（10 分钟内有真实请求）：每 300 秒一次，只测最近真正在用的模型。刚进入活跃时，没测过或距上次已满 300 秒就立即排队，否则最迟在上次检测后 300 秒排队，不沿用闲置时的小时排期。
- **空闲**：每 3600 秒一次，按账号哈希在一小时内错开，模型轮流。
- **不参与调度的号**（手动关闭调度、限流、临时不可调度、已停用）：sub2api 照样返回并注明原因（`inactive_reason`）。这些号不自动检测、不能手动检测、不计入渠道汇总，但保留并显示最后一次结果。号从 sub2api 删除后才会“退役”并隐藏。
- 发送前再检查一次账号是否仍在调度、模型是否仍在白名单；已失效的任务直接取消，不留检测记录。
- 单 worker 串行，不并发。服务重启打断的检测记为 `interrupted`（不算失败），60 秒后重排。
- 如果 sub2api 提供了“上游集群”（同一 `upstream_cluster_id` 的多个 Key），集群只测一次、每次轮换 Key。原版 sub2api v0.2.8 没有这个概念，每个号就是一个检测目标。

## 自动暂停（需要校准声明才生效）

- 只对 `auto_pause_calibrations` 里声明过的“账号 + 路由模型 + 推理强度”生效。列表默认为空，**所以默认只检测、不停号**。集群要求所有相关成员都有声明，缺一个就只观察。
- 声明格式：`{"account_id": 12, "model": "gpt-6-astra", "reasoning_effort": "low", "reference": "<校准记录在哪>"}`。这是人工声明，检测器不会替你验证；换号、换线路、改推理强度后应删掉重做。
- 有声明且结论为 `suspect` → 调 sub2api `POST /api/v1/internal/modeltrace/accounts/<id>/model-pause`，给该号**该模型**加模型级限流（reason `modeltrace_suspect`）：OAuth 号 `pause_minutes_oauth`（1440 分钟），API Key 号 `pause_minutes_apikey`（60 分钟）。该模型上已有的其他原因限流会被保留，解除时原样恢复。
- 暂停中照常检测（检测请求不受模型限流影响）；API Key 号不晚于 `paused_recheck_seconds_apikey`（1800 秒）复测；结论恢复 `match` → 调 `model-resume` 解除。只解除检测器自己加的暂停。
- 人工：`POST /accounts/<id>/run` 立即复测；`POST /accounts/<id>/reset` 解除暂停并记一条中性的 `reset` 结果（不算失败或可疑）。
- 解除（自动或人工）只在 sub2api 确认成功后才清本地状态；部分失败时人工重置返回 `reset:false` 和失败的账号。
- `enabled:false`：停止自动调度、不再领取排队中的自动任务、不做自动暂停/解除；人工检测仍可用。

## HTTP 接口（全部需要 `Authorization: Bearer <MODELTRACE_SECRET>`）

| 路由 | 用途 |
|---|---|
| `GET /health` | 版本、worker 状态 |
| `GET /accounts`、`GET /accounts/<id>` | 每个目标的状态：`models`、`next_model`、`per_model`（每模型最新结果与最近 12 次）、`participating`、`inactive_reason`、`members` |
| `POST /accounts/<id>/run` | 立即检测，可带 `{"model": "..."}`；400 `model_not_supported` / 409 `already_queued` / 404 |
| `POST /accounts/<id>/reset` | 解除检测暂停，可带 `{"model": "..."}` 指定模型 |
| `GET /v1/monitors/<id>`、`POST /v1/monitors/<id>/run` | 渠道状态页用；快照含 `accounts_summary`（只有计数）；手动检测会让该模型的每个目标各测一次 |

sub2api 管理端代理：`/api/v1/admin/modeltrace/accounts[/<id>][/run|/reset]`。

## 配置

`config.example.json` 是完整示例，部署时放到挂载目录里的 `config.json`（只读挂载进容器 `/run/modeltrace`）。密钥 `MODELTRACE_SECRET` 来自环境变量，sub2api 与检测服务共用。字段说明见上一级 README 的“配置参考”。

- `model_aliases`：路由模型名 → 题库模型名，例如 `{"gpt-6-astra-backup": "gpt-6-astra"}`。只做显式映射，不自动去后缀；上游返回的 model 只接受请求名或其映射目标。
- 会注入额外提示词或改写推理强度的中转线路，指纹与官方线路不完全相同，这也是自动暂停要求校准声明的原因。

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

基础镜像锁定为 linux/amd64；ARM 服务器请把 `Dockerfile` 第一行改成 `FROM python:3.12-slim`。
