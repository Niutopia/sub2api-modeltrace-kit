# sub2api-modeltrace-kit

**给 [sub2api](https://github.com/Wei-Shaw/sub2api) v0.2.8 加上“模型一致性检测”：观察上游模型指纹一致性，显式完成线路校准后才允许自动暂停。**

上游号声称跑的是 `gpt-5.6-terra`，实际回答却像别的模型？这个补丁包会定期用指纹题目检测每个 OpenAI 上游号，结论直接显示在 sub2api 后台；只有配置了该账号/路由/推理参数的校准记录且命中暂停规则时，才自动把这个号的这个模型停掉，别的模型照常工作，恢复正常后自动解除。

检测算法与题库来自 [xqy2006/ModelTrace](https://github.com/xqy2006/ModelTrace)。

---

## 功能

- **按账号检测**：每个 OpenAI 号（OAuth 与 API Key）都会被检测，新号自动加入。
- **动态频率**：最近 10 分钟有人用的号每 5 分钟测一次；没人用的号每小时一次，并在一小时内错开，不会同时打满上游。
- **真实路径**：检测请求经过 sub2api 自己的网关，并被钉在指定的号上（不换号、不故障转移），测的就是用户实际走的那条路。
- **自动暂停**：确认降智后只暂停**这个号的这个模型**，其他模型照常调度；恢复一致后自动解除，也可以手动解除。
- **后台可见**：
  - 账号管理页：每个 OpenAI 号的“状态”列下方显示检测标签，点开能看每个模型的结论、最像哪个模型、判定说明和最近 12 次记录，还能选模型立即检测、立即复测、重置状态。
  - 渠道状态页：监控卡片右上角显示“一致性”标签。

## 怎么判定

一次检测在同一个任务里连续出题，直到得出结论：

| 这一题的结果 | 怎么处理 |
|---|---|
| 与声称的模型一致 | 判 **一致**，结束 |
| 明显像别的模型（另一模型概率 ≥ 70%，声称的模型 ≤ 15%） | 记 1 次“不对”；攒够 **3 次** → 判 **可疑（降智）** |
| 说不准 | 不计数，换一题 |
| 没测成（上游报错、回答不完整） | 不计数，换一题 |

一次检测最多发 5 次请求。到上限还没结论：有测成的题判 **不确定**，一题都没测成判 **检测失败**。正常的号第 1 题就会判一致，只花 1 次请求。

**自动暂停**必须同时启用全局开关、auto_pause_enabled，并为所有目标成员配置匹配的 auto_pause_calibrations；仅判“可疑”不足以触发：OAuth 号暂停 24 小时，API Key 号暂停 60 分钟。暂停期间照常检测，一旦恢复一致立即解除。它复用 sub2api 原有的“模型级限流”，账号列表会显示“模型 X 限流至 …”；只会解除检测器自己加的暂停，不会碰上游真实的 429 限流。

> 检测是统计判断，结果仅供参考，不能作为上游违约的证明。

## 快速开始

**前提**：能从源码构建 sub2api 镜像（补丁需要重新编译 sub2api），用 Docker Compose 部署。

### 1. 打补丁并构建 sub2api

```bash
git clone https://github.com/Wei-Shaw/sub2api.git
cd sub2api
git checkout v0.2.8
git apply /path/to/sub2api-modeltrace-kit/patches/sub2api-v0.2.8-modeltrace.patch
docker build -t sub2api:v0.2.8-modeltrace .
```

补丁针对 v0.2.8 制作并验证过。其他版本请先运行 `git apply --check` 看能否打上。

### 2. 构建检测服务

```bash
docker build -t modeltrace-service:local /path/to/sub2api-modeltrace-kit/modeltrace
```

`Dockerfile` 的基础镜像锁定为 linux/amd64。ARM 服务器请把第一行改成 `FROM python:3.12-slim`。

### 3. 准备配置

在 sub2api 的部署目录下新建：

```text
modeltrace/
├── shared.env          # 复制 deploy/shared.env.example，填一个长随机密钥
├── config/config.json  # 复制 modeltrace/config.example.json 后修改
└── data/               # 空目录，存检测记录（容器内 uid 10001 需要可写）
```

`config.json` 需要改的只有两处：

- `api_key`：在 sub2api 后台为检测**单独新建一个 API Key**，所在分组要能用到你想检测的 OpenAI 号。
- `monitors`：想在渠道状态页显示“一致性”标签时填写。键是渠道监控项的 ID，值写它对应的模型，例如：
  ```json
  "monitors": { "2": { "model": "gpt-6-astra", "enabled": false, "supported": true } }
  ```
  不需要可以写 `{}`。

默认 auto_pause_calibrations 为空，因此先观测、不自动停号。其余保持默认即可（`base_url` 与 `host_api_base` 默认走 Docker 内网 `http://sub2api:8080`）。

### 4. 加入 Docker Compose

参考 [`deploy/docker-compose.modeltrace.yml`](deploy/docker-compose.modeltrace.yml)，要点：

- sub2api 使用第 1 步构建的镜像，并加上环境变量：
  - `MODELTRACE_INTERNAL_URL=http://modeltrace:8081`
  - `MODELTRACE_SECRET`（从 `shared.env` 读取）
  - `MODELTRACE_ACTIVITY_EXCLUDE_KEY_IDS=<第 3 步那个 Key 的 ID>`：检测自己的请求不算“有人在用”。渠道监控用的 Key 也写上，多个用逗号分隔。
- 检测服务的服务名必须是 `modeltrace`（或 `sub2api-modeltrace`），端口 8081。sub2api 只会连这两个名字，防止密钥被发到别处。

### 5. 挡住内部接口

`/api/v1/internal/*` 只给 Docker 内网使用。在反向代理里对公网屏蔽，Nginx 写法见 [`deploy/nginx-snippet.conf`](deploy/nginx-snippet.conf)。

### 6. 启动并确认

```bash
docker compose up -d
docker compose exec modeltrace python -c "import os,httpx;print(httpx.get('http://127.0.0.1:8081/health',headers={'Authorization':'Bearer '+os.environ['MODELTRACE_SECRET']}).json())"
```

看到 `"worker_running": true` 就成功了。几分钟后，账号管理页的 OpenAI 号下方会出现检测标签。

## 配置参考

`config.json` 里的可选项：

| 字段 | 默认值 | 作用 |
|---|---|---|
| `per_account_enabled` | `true` | 按账号检测 |
| `active_interval_seconds` | `300` | 有人在用的号多久测一次（秒） |
| `idle_interval_seconds` | `3600` | 没人用的号多久测一次（秒） |
| `active_window_seconds` | `600` | 多久内有真实请求算“有人在用”（秒） |
| `auto_pause_enabled` | `true` | 判“可疑”后自动暂停 |
| `pause_minutes_oauth` | `1440` | OAuth 号暂停多久（分钟） |
| `pause_minutes_apikey` | `60` | API Key 号暂停多久（分钟） |
| `paused_recheck_seconds_apikey` | `1800` | API Key 号暂停期间多久复查一次（秒） |
| `idle_timeout_seconds` | `600` | 连续多少秒没有任何输出才算超时（没有总时长上限） |

## 常见问题

**会消耗很多额度吗？**
正常的号每次检测只发 1 个请求。只有结果可疑的号才会多测几题，一次最多 5 个请求。

**会把整个号停掉吗？**
不会。只暂停被判降智的那一个模型，号本身和其他模型不受影响。

**误判了怎么办？**
在账号管理页点开检测标签，对被暂停的模型点“重置状态”，立即解除。想完全关掉自动暂停，把 `auto_pause_enabled` 设为 `false`，只看结果不停号。

**支持 OpenAI 以外的平台吗？**
不支持。检测题库和判定目前只覆盖 OpenAI 模型。

## 目录结构

```text
patches/sub2api-v0.2.8-modeltrace.patch   sub2api v0.2.8 补丁（后端 + 前端）
modeltrace/                               检测服务源码、Dockerfile、示例配置、测试
deploy/                                   Compose、密钥、Nginx 示例
```

## 说明与致谢

这是个人二次开发的项目，基于以下两个开源项目：

- **[sub2api](https://github.com/Wei-Shaw/sub2api)**，作者 [Wei-Shaw](https://github.com/Wei-Shaw)。`patches/` 中的补丁修改自 sub2api v0.2.8。
- **[ModelTrace](https://github.com/xqy2006/ModelTrace)**，作者 [xqy2006](https://github.com/xqy2006)。`modeltrace/` 中的指纹算法（`modeltrace/modeltrace/fingerprint.py`）与题库（`modeltrace/modeltrace/data/unified_bank.json`）原样取自上游 commit `55a2e4a`，文件哈希见 [`modeltrace/PROVENANCE.json`](modeltrace/PROVENANCE.json)。

两个原项目的许可证文件按其要求保留在对应目录：[`patches/LICENSE`](patches/LICENSE)、[`modeltrace/LICENSE`](modeltrace/LICENSE)。


## 0.1.16 安全与线路兼容说明

- `enabled:false` 关闭自动调度，并阻止领取已排队的自动任务及自动暂停/恢复；明确的手动检测仍可执行。已开始的请求不强行中断，返回后不触发自动动作。
- `auto_pause_calibrations` 默认 `[]`。每项必须包含正整数 `account_id`、精确路由 `model`、`reasoning_effort`、已完成线路校准的文档 `reference`。它是运维声明，不是检测器自动验证；不要填占位引用冒充验收。账号、路由、参数或注入提示词/插件版本发生变化时应撤销对应声明并重新校准。
- 缺少匹配校准的单号或集群仅观测，不自动停用，也不会因一次未经校准的 match 自动恢复。集群全部支持该模型的成员都需要校准声明。
- BPS 会注入提示词，并可能归一化 reasoning effort。请求参数 none 不证明上游实际 none；仅 HTTP200 或工具成功不是能力校准证据。
- `model_aliases` 显式配置路由名到题库模型的直接映射，例如 `gpt-6-astra-basispoints` → `gpt-6-astra`；不自动裁剪后缀、不修改题库、不把别名支持当作校准通过。发现与请求保留路由名，指纹比较使用配置的题库模型。返回 model 仅接受请求名或它声明的目标，任意其他模型仍被拒绝。
- 自动解除和手动重置仅在宿主确认成功后清理本地暂停状态。手动部分失败返回 `reset:false` 及失败账号，不再虚报完全恢复。
