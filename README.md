# sub2api-modeltrace-kit

**给 [sub2api](https://github.com/Wei-Shaw/sub2api) v0.2.8 加一个“模型真假检测器”：定期检查每个 OpenAI 上游号，看它实际跑的是不是它声称的模型。重点支持 `gpt-6-astra`。**

你从上游买了 `gpt-6-astra`，但它真的是 astra 吗？会不会被悄悄换成便宜模型，或者在高峰期降智？这个补丁包会：

- 用指纹题目检测每个 OpenAI 上游号（OAuth 和 API Key 都行），新加的号自动纳入；
- 检测请求走 sub2api 自己的网关，并**钉死在被测的那个号上**，测的就是你的用户实际走的那条线路；
- 把结论直接显示在 sub2api 后台的账号列表和渠道状态页上；
- 可选：确认某个号“可疑”后，**只暂停这个号的这个模型**，其他模型和其他号照常工作，恢复正常后自动解除。

当前版本：**检测服务 ModelTrace 0.1.19**，sub2api 补丁基于 **v0.2.8**。检测算法和题库来自 [xqy2006/ModelTrace](https://github.com/xqy2006/ModelTrace)。

> 检测是统计判断，只能作为参考，不能当作上游违约的证据。

---

## 目录

- [能测哪些模型](#能测哪些模型)
- [工作原理](#工作原理)
- [怎么判定](#怎么判定)
- [用 gpt-6-astra 要注意的事](#用-gpt-6-astra-要注意的事)
- [快速开始](#快速开始)
- [后台怎么看](#后台怎么看)
- [自动暂停与校准](#自动暂停与校准)
- [配置参考](#配置参考)
- [从旧版本升级](#从旧版本升级)
- [常见问题](#常见问题)
- [版本记录](#版本记录)
- [目录结构](#目录结构)
- [说明与致谢](#说明与致谢)

## 能测哪些模型

只检测 **OpenAI 平台**的账号。题库里有指纹数据的 GPT 模型：

| 模型 | 说明 |
|---|---|
| **`gpt-6-astra`** | 主力推荐。推理强度默认 `low`，见[下文](#用-gpt-6-astra-要注意的事) |
| `gpt-6-sol`、`gpt-6-luna` | |
| `gpt-5.6-sol`、`gpt-5.6-terra`、`gpt-5.6-luna` | |
| `gpt-5.5`、`gpt-5.4` | |

题库里还有 Claude 系列的指纹，只在比对时用于判断“像不像别的模型”，不会主动去测 Claude 账号。

账号的模型名如果和题库不一样（例如你把 astra 路由起名叫 `gpt-6-astra-backup`），在 `model_aliases` 里写一条映射就能测，见[配置参考](#配置参考)。没有指纹数据、也没有映射的模型，后台会显示为“未检测”，不会假装测过。

## 工作原理

```text
            ┌──────────── 每 60 秒拉一次账号名单和最近用量 ────────────┐
            │                                                            ▼
┌───────────┴───────────┐   检测请求（带密钥，钉在指定账号）   ┌─────────────────────┐
│ ModelTrace 检测服务    │ ───────────────────────────────▶ │  sub2api（打过补丁）  │ ──▶ 上游号
│ （独立容器，只连内网） │ ◀─────────── 回答 ────────────── │                     │
└───────────┬───────────┘                                   └──────────┬──────────┘
            │  结论、暂停/解除                                           │
            └─────────────────────────────────────────────────────────▶ 后台账号页 / 渠道状态页
```

- **调度节奏**：最近 10 分钟有真实用户请求的号算“活跃”，每 5 分钟测一次，只测最近真正在用的模型；没人用的号每小时测一次，按号错开时间，不会同时打满上游。
- **真实线路**：检测请求带内部密钥和账号 ID，sub2api 会把它钉在这个号上，不换号、不故障转移。号不可用时直接返回“本轮未检测”，不会跑去别的号测出一个假结果。
- **不算用量**：检测用的 API Key 可以排除在“活跃”统计之外，免得检测自己把号判成活跃。
- **一次只测一个**：单个 worker 串行执行，不会并发压上游。

## 怎么判定

一次检测在同一个任务里连续出题，直到有结论：

| 这一题的结果 | 处理方式 |
|---|---|
| 与声称的模型一致 | 判 **一致**，结束 |
| 明显像别的模型（另一模型概率 ≥ 70%，声称的模型 ≤ 15%） | 记 1 次“不对”，攒够 **3 次** → 判 **可疑** |
| 说不准 | 不计数，换一题 |
| 没测成（上游报错、回答不完整） | 不计数，换一题 |

一次检测最多发 **5 个请求**。到上限还没结论时：有测成的题判 **不确定**，一题都没测成判 **检测失败**。

正常的号第一题就会判一致，**只花 1 个请求**。每题都等回答完整输出后再比对；没有总时长限制，只有连续 10 分钟没有任何输出才算超时。

## 用 gpt-6-astra 要注意的事

1. **推理强度默认 `low`。** astra 不支持 `none`，所以检测请求默认带 `reasoning_effort: low`。想改成别的强度，用 `reasoning_effort_overrides`（例如 `{"gpt-6-astra": "medium"}`）。填 `none` 会在启动时报配置错误。
2. **校准声明必须和推理强度对得上。** 自动暂停要求为“账号 + 模型 + 推理强度”写校准声明（见[自动暂停与校准](#自动暂停与校准)）。改了推理强度，旧声明就不再生效，这是故意的：不同强度下 astra 的回答风格不同，不能拿旧结论去停号。
3. **路由名和题库名不一样时用别名。** 账号里配的是 `gpt-6-astra-backup` 这类名字，就写 `"model_aliases": {"gpt-6-astra-backup": "gpt-6-astra"}`。只做你显式写的映射，不会自动去后缀；上游返回的模型名只接受请求名或它映射到的目标，别的一律判不符。
4. **经过中转或改写的线路先观察，别急着自动停号。** 有些线路会给请求加额外提示词，或者改写推理强度。这类线路的指纹和官方线路不完全一样，可能出现“不确定”甚至误判。先只看结果，确认正常号在这条线路上稳定判“一致”后，再给它写校准声明。
5. **成本很低。** 正常的 astra 号每次检测只发 1 个 `low` 强度请求；只有结果可疑时才多测几题，最多 5 个。

## 快速开始

**前提**：能从源码构建 sub2api 镜像（补丁要重新编译 sub2api），并且用 Docker Compose 部署。

### 1. 打补丁并构建 sub2api

```bash
git clone https://github.com/Wei-Shaw/sub2api.git
cd sub2api
git checkout v0.2.8
git apply /path/to/sub2api-modeltrace-kit/patches/sub2api-v0.2.8-modeltrace.patch
docker build -t sub2api:v0.2.8-modeltrace .
```

补丁针对 v0.2.8 制作并测试过。其他版本先运行 `git apply --check` 看能不能打上。

### 2. 构建检测服务

```bash
docker build -t modeltrace-service:local /path/to/sub2api-modeltrace-kit/modeltrace
```

`Dockerfile` 的基础镜像锁定为 linux/amd64。ARM 服务器请把第一行改成 `FROM python:3.12-slim`。

### 3. 新建检测专用的 API Key

在 sub2api 后台新建一个 API Key，**只给检测用**：

- 它所在的分组要能用到你想检测的 OpenAI 号（比如 astra 号所在的分组）；
- 记下它的 **ID**（后台 Key 列表里能看到），第 5 步要用。

### 4. 准备配置目录

在 sub2api 的部署目录下新建：

```text
modeltrace/
├── shared.env          # 复制 deploy/shared.env.example，填一个长随机密钥
├── config/config.json  # 复制 modeltrace/config.example.json 后修改
└── data/               # 空目录，存检测记录（容器内 uid 10001 要能写）
```

```bash
mkdir -p modeltrace/config modeltrace/data
cp /path/to/sub2api-modeltrace-kit/deploy/shared.env.example modeltrace/shared.env
cp /path/to/sub2api-modeltrace-kit/modeltrace/config.example.json modeltrace/config/config.json
sed -i "s/REPLACE_WITH_A_LONG_RANDOM_SECRET/$(openssl rand -hex 32)/" modeltrace/shared.env
sudo chown 10001 modeltrace/data
```

`config.json` 最少只改一处：`api_key` 换成第 3 步的 Key。一个只测 astra 的最小配置：

```json
{
  "base_url": "http://sub2api:8080/v1",
  "api_key": "sk-你的检测专用Key",
  "host_api_base": "http://sub2api:8080",
  "per_account_enabled": true,
  "monitors": {},
  "reasoning_effort_overrides": { "gpt-6-astra": "low" },
  "auto_pause_calibrations": []
}
```

- `monitors`：想在“渠道状态页”的监控卡片上显示一致性标签时才填。键是渠道监控项的 ID，值写它监控的模型，例如 `{"1": {"model": "gpt-6-astra", "enabled": false, "supported": true}}`。
- `auto_pause_calibrations` 留空 = **只检测、不自动停号**。建议先这样跑几天。

### 5. 加入 Docker Compose

参考 [`deploy/docker-compose.modeltrace.yml`](deploy/docker-compose.modeltrace.yml)，要点：

- sub2api 改用第 1 步构建的镜像，加上：
  - `env_file: ./modeltrace/shared.env`（提供 `MODELTRACE_SECRET`）
  - `MODELTRACE_INTERNAL_URL=http://modeltrace:8081`
  - `MODELTRACE_ACTIVITY_EXCLUDE_KEY_IDS=<第 3 步 Key 的 ID>`：检测自己的请求不算“有人在用”。渠道监控用的 Key 也建议写上，多个用逗号隔开。
- 检测服务的服务名必须是 `modeltrace`（或 `sub2api-modeltrace`），端口 8081。sub2api 只会连这两个名字，防止密钥被发到别处。
- 检测服务只读运行、丢掉所有权限，只需要挂载 `config/`（只读）和 `data/`。

### 6. 挡住内部接口

`/api/v1/internal/*` 只给 Docker 内网使用，必须在反向代理里对公网屏蔽。Nginx 写法见 [`deploy/nginx-snippet.conf`](deploy/nginx-snippet.conf)：

```nginx
location ^~ /api/v1/internal/ { return 404; }
```

### 7. 启动并确认

```bash
docker compose up -d
docker compose exec modeltrace python -c "import os,httpx;print(httpx.get('http://127.0.0.1:8081/health',headers={'Authorization':'Bearer '+os.environ['MODELTRACE_SECRET']}).json())"
```

看到 `"worker_running": true` 就成功了。一两分钟后，账号管理页里 OpenAI 号的状态列下面会出现检测标签；想马上看结果，点开标签选 `gpt-6-astra`，点“立即检测”。

## 后台怎么看

### 账号管理页

每个 OpenAI 号的“状态”列下面有一个检测标签：

| 标签 | 含义 |
|---|---|
| **一致** | 最近一次检测，回答符合声称的模型 |
| **⚠ 可疑** | 3 道题都明显像别的模型 |
| **⚠ 可疑 · 已暂停** | 可疑，并且已经自动暂停这个号的这个模型 |
| **不确定** | 换了几道题，没有一题判一致，但也不够 3 题明显像别的模型 |
| **检测失败** | 一题都没测成（上游报错、号不可用等） |
| **未检测** | 还没测过，或题库不支持这个模型 |
| **排队中 / 检测中** | 正在排队或正在测 |

点开标签可以看到：每个模型的结论、“最像哪个模型”、判定说明、最近 12 次记录；还能选模型**立即检测**，对已暂停的模型**重置状态**（立即解除暂停并清掉可疑结论）。

**不参与调度的号**（手动关闭调度、限流中、临时不可调度、已停用）不会自动检测，也不能手动检测，但会**保留并显示最后一次结果**，同时注明原因，比如“限流中”。号从后台删除后才会消失。

保存账号设置（比如改了模型白名单）后，检测名单会立刻刷新，不用等下一轮。

### 渠道状态页

在 `monitors` 里配置过的监控卡片，右上角会显示“一致性”标签。按账号检测模式下，卡片上的结论、历史和上次/下次时间，都由仍在调度的账号的结果汇总得出，和账号页保持一致。

## 自动暂停与校准

自动暂停**默认不生效**。只有同时满足下面三条，才会自动停号：

1. `auto_pause_enabled` 为 `true`（默认就是）；
2. 在 `auto_pause_calibrations` 里为这个号写了校准声明；
3. 检测结论是“可疑”。

校准声明的意思是：“我已经确认过，这个号在这个模型、这个推理强度下，正常时能稳定判一致。”写法：

```json
"auto_pause_calibrations": [
  {
    "account_id": 12,
    "model": "gpt-6-astra",
    "reasoning_effort": "low",
    "reference": "2026-09 连续 3 天检测均一致，记录见运维文档第 4 节"
  }
]
```

- `account_id`：后台账号 ID；`model`：账号里的路由模型名（有别名时写路由名）；`reasoning_effort`：必须和实际检测用的强度一致（astra 默认 `low`）；`reference`：你的校准记录在哪。
- 这是**人工声明**，检测器不会替你验证。换号、换线路、改推理强度之后，请删掉旧声明重新观察。

触发后会发生什么：

- 只给**这个号的这个模型**加模型级限流，账号列表会显示“模型 X 限流至 …”。OAuth 号暂停 24 小时，API Key 号暂停 60 分钟（可配置）。
- 暂停期间照常检测；一旦恢复“一致”就自动解除。API Key 号暂停期间最晚 30 分钟复查一次。
- 如果这个模型上已经有别的原因的限流（比如上游 429），暂停**不会覆盖它**；检测器解除自己的暂停时，还没到期的原有限流会原样恢复。
- 检测器只解除自己加的暂停，不会碰上游真实的限流。
- 想马上解除：在账号页点开标签，对该模型点“重置状态”。

想彻底关掉自动暂停，把 `auto_pause_enabled` 设为 `false`，就只看结果不停号。想临时停掉整个检测服务的自动行为，把 `enabled` 设为 `false`：不再自动排队，也不做自动暂停/解除，手动检测照常能用。

## 配置参考

`config.json` 的全部字段：

| 字段 | 默认值 | 作用 |
|---|---|---|
| `base_url` | `http://sub2api:8080/v1` | 检测请求发往哪里（sub2api 在 Docker 内网的地址） |
| `api_key` | 必填 | 检测专用 API Key |
| `host_api_base` | `http://sub2api:8080` | sub2api 内部接口地址 |
| `enabled` | `true` | 总开关。`false` 时停止自动检测和自动暂停/解除，手动检测仍可用 |
| `per_account_enabled` | `true` | 按账号检测 |
| `active_interval_seconds` | `300` | 活跃的号多久测一次（秒） |
| `idle_interval_seconds` | `3600` | 没人用的号多久测一次（秒） |
| `active_window_seconds` | `600` | 多久内有真实请求算“活跃”（秒） |
| `monitors` | `{}` | 渠道状态页监控项 → 模型的对应关系 |
| `reasoning_effort_overrides` | `{}` | 每个模型的检测推理强度。astra 默认 `low`，其他模型默认 `none` |
| `model_aliases` | `{}` | 路由模型名 → 题库模型名，例如 `{"gpt-6-astra-backup": "gpt-6-astra"}` |
| `auto_pause_enabled` | `true` | 允许自动暂停（还需要校准声明） |
| `auto_pause_calibrations` | `[]` | 校准声明列表，见上文 |
| `pause_minutes_oauth` | `1440` | OAuth 号暂停多久（分钟） |
| `pause_minutes_apikey` | `60` | API Key 号暂停多久（分钟） |
| `paused_recheck_seconds_apikey` | `1800` | API Key 号暂停期间多久复查一次（秒） |
| `idle_timeout_seconds` | `600` | 连续多少秒没有任何输出算超时（没有总时长上限） |
| `scope_label` | — | 渠道状态页上显示的检测范围说明 |

sub2api 这边的环境变量：

| 变量 | 作用 |
|---|---|
| `MODELTRACE_SECRET` | 与检测服务共用的密钥（两边都从 `shared.env` 读） |
| `MODELTRACE_INTERNAL_URL` | 检测服务地址，固定写 `http://modeltrace:8081` |
| `MODELTRACE_ACTIVITY_EXCLUDE_KEY_IDS` | 不计入“活跃”统计的 Key ID，逗号分隔 |

检测服务的 HTTP 接口说明见 [`modeltrace/README.md`](modeltrace/README.md)。

## 从旧版本升级

1. 拉取本仓库最新代码。
2. 用新补丁重新构建 sub2api：在干净的 v0.2.8 源码上 `git apply` 新的 `patches/sub2api-v0.2.8-modeltrace.patch`，再 `docker build`。补丁是完整的，不需要先打旧补丁。
3. 重新构建检测服务镜像。
4. `docker compose up -d`。检测记录保存在 `data/` 的 SQLite 里，启动时自动补齐新字段，历史记录会保留。

建议 sub2api 和检测服务一起升级：新版检测服务会向 sub2api 要“不参与调度的号”，旧版 sub2api 会忽略这个参数，这些号就不会显示最后一次结果。

## 常见问题

**会消耗很多额度吗？**
不会。正常的号每次检测只发 1 个请求（astra 用 `low` 强度）。只有结果可疑时才多测几题，一次最多 5 个请求。活跃的号 5 分钟一次，闲置的号 1 小时一次。

**会把整个号停掉吗？**
不会。只暂停被判可疑的那一个模型，号本身和其他模型照常工作。而且默认没有校准声明，根本不会自动停号。

**号限流了、额度用完了会怎样？**
检测会显示“本轮未检测”，不算可疑，也不会反复重试。限流中的号不参与检测，只保留最后一次结果。

**误判了怎么办？**
在账号页点开检测标签，对该模型点“重置状态”，立即解除并清掉可疑结论。经常误判的线路，先删掉它的校准声明，只观察不停号。

**astra 总是显示“不确定”？**
多半是线路改写了请求（加了提示词、改了推理强度），或者路由名没配 `model_aliases`。先确认账号的模型名能对应到题库里的 `gpt-6-astra`，再对比一个直连正常号的结果。

**能测 Claude 或其他平台的号吗？**
不能。目前只检测 OpenAI 平台账号。

**检测请求会被转发给上游吗？密钥会泄露吗？**
检测用的内部请求头在进入 sub2api 时就会被统一剥掉，不会转发给上游。sub2api 只会把共享密钥发给 `modeltrace` / `sub2api-modeltrace` 这两个内网服务名，配成别的地址会被拒绝。

## 版本记录

| 版本 | 变化 |
|---|---|
| **0.1.19** | 保存账号设置后立即同步检测名单和模型白名单；活跃的号不再沿用闲置时的小时排期，最迟 5 分钟内开始检测 |
| **0.1.18** | 限流、关闭调度的号保留并显示最后一次结果，注明原因；渠道状态页与账号页结论一致；排队、检测中状态实时显示 |
| **0.1.17** | 检测请求不再带 `service_tier`，兼容拒绝该参数的线路 |
| **0.1.16** | 自动暂停改为必须有校准声明；新增 `enabled` 总开关、`model_aliases` 别名；解除暂停只在 sub2api 确认成功后才算数 |
| 0.1.15 | “明显像别的模型”阈值由 80% 调到 70% |
| 0.1.14 | 三题判定规则与自动暂停 |

sub2api 补丁同步更新：暂停时保留并恢复其他原因的限流；检测请求头改为全局剥离；账号页显示不参与调度的号；保存账号后立即刷新检测名单。

## 目录结构

```text
patches/sub2api-v0.2.8-modeltrace.patch   sub2api v0.2.8 补丁（后端 + 前端 + 测试）
modeltrace/                               检测服务：源码、Dockerfile、示例配置、测试
deploy/                                   Compose、密钥、Nginx 示例
```

自己跑检测服务的测试：

```bash
cd modeltrace
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -p no:cacheprovider -q -o addopts=""
```

## 说明与致谢

这是个人二次开发的项目，基于以下两个开源项目：

- **[sub2api](https://github.com/Wei-Shaw/sub2api)**，作者 [Wei-Shaw](https://github.com/Wei-Shaw)。`patches/` 中的补丁修改自 sub2api v0.2.8。
- **[ModelTrace](https://github.com/xqy2006/ModelTrace)**，作者 [xqy2006](https://github.com/xqy2006)。指纹算法（`modeltrace/modeltrace/fingerprint.py`）与题库（`modeltrace/modeltrace/data/unified_bank.json`）原样取自上游 commit `55a2e4a`，文件哈希见 [`modeltrace/PROVENANCE.json`](modeltrace/PROVENANCE.json)。

两个原项目的许可证文件按要求保留在对应目录：[`patches/LICENSE`](patches/LICENSE)、[`modeltrace/LICENSE`](modeltrace/LICENSE)。
