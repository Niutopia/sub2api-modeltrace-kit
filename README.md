# sub2api-modeltrace-kit

给 [sub2api](https://github.com/Wei-Shaw/sub2api) **v0.2.8** 加上“模型一致性检测”：定期检查每个 OpenAI 上游号实际跑的是不是它声称的模型，发现降智（例如声称 gpt-5.6-terra，实际回答像别的模型）时自动暂停这个号的这个模型。

检测算法与题库来自 [xqy2006/ModelTrace](https://github.com/xqy2006/ModelTrace)。

> 检测结果是统计判断，仅供参考；不能作为上游违约的证明。

## 包含什么

| 目录 | 内容 | 许可证 |
|---|---|---|
| `patches/sub2api-v0.2.8-modeltrace.patch` | 给 sub2api v0.2.8 打的补丁（60 个文件，后端 + 前端） | LGPL-3.0（同 sub2api，见 `patches/LICENSE`） |
| `modeltrace/` | 检测服务（Python，独立容器） | MIT（见 `modeltrace/LICENSE`） |
| `deploy/` | docker compose、密钥、nginx 示例 | — |

## 装好以后能看到

- **账号管理页**：每个 OpenAI 号的“状态”列下面多一个检测标签（一致 / 可疑 / 不确定 / 检测失败）。点开是弹窗：每个模型一行，显示最新结论、最相似的模型、判定说明、最近 12 次记录；可以选模型“立即检测”；被暂停的模型有“立即复测”“重置状态”。
- **渠道状态页**：监控卡片右上角多一个“一致性”标签（需要在检测服务配置里把监控项和模型对应起来，见下文）。

## 检测规则

- 每个 OpenAI 号（OAuth 与 API Key）都会被检测。最近 10 分钟有真实请求的号每 5 分钟测一次，其余每小时一次、在一小时内错开。
- 一次检测在同一个任务里连续出题：
  - 这题一致 → 判“一致”，结束；
  - 这题**明显像别的模型**（另一模型概率 ≥ 70%、目标模型 ≤ 15%）→ 记 1 次不对，攒够 **3 次** → 判“可疑”（降智）；
  - “说不准”或没测成（上游报错、回答太短）→ 不计数，换一题；
  - 一次最多发 5 次请求；到上限还没结论：有测成的题 → “不确定”，一题没测成 → “检测失败”。
- **自动暂停**：判“可疑”后，只暂停**这个号的这个模型**（复用 sub2api 原有的“模型级限流”，账号列表会显示“模型 X 限流至 …”），其他模型照常调度。OAuth 号停 24 小时，API Key 号停 60 分钟；暂停期间照常检测，恢复一致立即解除。人工也可以点“重置状态”解除。只会解除检测器自己加的暂停，不影响真实的 429 限流。
- 检测请求通过 sub2api 自己的网关发出，并用内部密钥把请求钉在指定的号上（不换号、不故障转移），所以测的就是用户真实走的路径。

## 安装

前提：你能自己从源码构建 sub2api 镜像（补丁需要重新编译）。

### 1. 给 sub2api 打补丁并构建

```bash
git clone https://github.com/Wei-Shaw/sub2api.git
cd sub2api
git checkout v0.2.8
git apply /path/to/sub2api-modeltrace-kit/patches/sub2api-v0.2.8-modeltrace.patch
# 然后按 sub2api 官方文档构建你的镜像，例如：
docker build -t sub2api:v0.2.8-modeltrace .
```

补丁只针对 v0.2.8；其他版本请先在测试环境试 `git apply --check`。

### 2. 构建检测服务镜像

```bash
docker build -t modeltrace-service:local ./modeltrace
```

`modeltrace/Dockerfile` 的基础镜像按摘要锁定为 linux/amd64；ARM 服务器请把第一行改成 `FROM python:3.12-slim`。

### 3. 准备配置

在 sub2api 的部署目录下：

```text
modeltrace/
├── shared.env          # 从 deploy/shared.env.example 复制，填一个长随机密钥
├── config/config.json  # 从 modeltrace/config.example.json 复制并修改
└── data/               # 空目录，检测服务的 SQLite 数据（容器内用户 uid 10001 需要可写）
```

`config.json` 里要改的：

- `api_key`：在 sub2api 后台为检测**单独建一个 API Key**（绑定到能用到这些 OpenAI 号的分组）。
- `base_url`：保持 `http://sub2api:8080/v1`（检测请求只走 Docker 内网）。
- `host_api_base`：保持 `http://sub2api:8080`。
- `monitors`：渠道状态页的标签用。键是 sub2api 里渠道监控项的 ID，值写这个监控项的模型，例如 `"2": {"model": "gpt-6-astra", "enabled": false, "supported": true}`。不需要渠道页标签可以留空对象。

### 4. 加到 docker compose

参考 `deploy/docker-compose.modeltrace.yml`：

- sub2api 加环境变量 `MODELTRACE_INTERNAL_URL=http://modeltrace:8081`、`MODELTRACE_SECRET`（来自 shared.env），以及 `MODELTRACE_ACTIVITY_EXCLUDE_KEY_IDS=<第 3 步那个 Key 的 ID>`（检测自己的流量不算“有人在用”；如果渠道监控也用了某个 Key，一并写上，逗号分隔）。
- 检测服务的 compose 服务名必须是 `modeltrace`（或 `sub2api-modeltrace`），端口 8081——sub2api 只允许连这两个名字，防止密钥被发到别处。

### 5. 反向代理屏蔽内部接口

`/api/v1/internal/*` 只给 Docker 内网用，公网必须挡掉，见 `deploy/nginx-snippet.conf`。

### 6. 启动并确认

```bash
docker compose up -d
docker compose exec modeltrace python -c "import os,httpx;print(httpx.get('http://127.0.0.1:8081/health',headers={'Authorization':'Bearer '+os.environ['MODELTRACE_SECRET']}).json())"
```

看到 `"worker_running": true` 即可。几分钟后账号管理页的 OpenAI 号会出现检测标签。

## 可调配置（`config.json`，都可省略）

| 字段 | 默认 | 说明 |
|---|---|---|
| `per_account_enabled` | `true` | 按账号检测 |
| `active_interval_seconds` / `idle_interval_seconds` / `active_window_seconds` | 300 / 3600 / 600 | 活跃/空闲检测间隔、判定活跃的时间窗 |
| `auto_pause_enabled` | `true` | 判“可疑”后自动暂停 |
| `pause_minutes_oauth` / `pause_minutes_apikey` | 1440 / 60 | 暂停时长 |
| `paused_recheck_seconds_apikey` | 1800 | API Key 号暂停期间多久复查一次 |
| `idle_timeout_seconds` | 600 | 连续多少秒没有任何输出才判超时（没有总时长上限） |

## 致谢

- **sub2api** — 作者 [Wei-Shaw](https://github.com/Wei-Shaw)，本补丁基于其 v0.2.8 源码，遵循 LGPL-3.0。
- **ModelTrace** — 作者 [xqy2006](https://github.com/xqy2006)，指纹算法与题库原样取自 commit `55a2e4a`（MIT），见 `modeltrace/PROVENANCE.json`。

详细的许可证说明见 `NOTICE.md`。
