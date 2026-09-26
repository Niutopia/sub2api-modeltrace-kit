"""Tests for Task M1: ModelTrace按检测目标（上游/单号）调度、选模型、换Key、中断状态、契约格式与迁移。"""
import hashlib
import json
import pytest

from modeltrace import __version__
from modeltrace.db import ModelTraceDB
from modeltrace.service import (
    ModelTraceService,
    ModelNotSupported,
    DuplicateQueue,
    UnknownAccount,
    utc_iso,
)
from modeltrace.transport import ProbeResult
from app import create_app
from tests.test_per_account_d2 import MockHostClient, MockProbeTransport, make_service


def test_version_remains_0113():
    assert __version__ == "0.1.19"


def test_cluster_three_members_scheduled_once_per_hour_with_cluster_hash(tmp_path, fake_clock):
    """三个成员的上游只调度 1 次/小时；偏移按 cluster 哈希。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 101,
            "name": "Cluster-Key-1",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_alpha",
            "cluster_name": "Alpha Upstream",
            "real_models_10m": [],
            "last_real_request_at": None,
        },
        {
            "account_id": 102,
            "name": "Cluster-Key-2",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_alpha",
            "cluster_name": "Alpha Upstream",
            "real_models_10m": [],
            "last_real_request_at": None,
        },
        {
            "account_id": 103,
            "name": "Cluster-Key-3",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_alpha",
            "cluster_name": "Alpha Upstream",
            "real_models_10m": [],
            "last_real_request_at": None,
        },
    ])

    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    # 验证偏移用 sha256("cluster:" + cluster_id)
    offset = int(hashlib.sha256("cluster:cluster_alpha".encode("utf-8")).hexdigest(), 16) % 3600
    hour_start = (int(t0) // 3600) * 3600
    expected_next = hour_start + offset
    if expected_next <= t0:
        expected_next += 3600

    rep_acc = svc.db.get_account(101)
    acc102 = svc.db.get_account(102)
    acc103 = svc.db.get_account(103)

    assert rep_acc["next_run_at"] == pytest.approx(expected_next)
    # 其他成员的 next_run_at 为 NULL，只有一份排期
    assert acc102["next_run_at"] is None
    assert acc103["next_run_at"] is None

    # Advance clock to expected_next and run schedule
    fake_clock.value = expected_next
    svc._schedule_due_accounts(expected_next)

    # 上游只排队了 1 次（由代表 101 排队）
    assert svc.db.has_pending_account(101) is True
    assert svc.db.has_pending_account(102) is False
    assert svc.db.has_pending_account(103) is False

    # 排队之后下一个调度在 1 小时后
    rep_after = svc.db.get_account(101)
    assert rep_after["next_run_at"] == pytest.approx(expected_next + 3600)


def test_models_union_and_single_member_model_routing(tmp_path, fake_clock, monkeypatch):
    """模型并集；某模型只在一个成员上 → 只用那个成员。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 201,
            "name": "Key-Sol-Only",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_beta",
            "cluster_name": "Beta Cluster",
            "real_models_10m": [],
        },
        {
            "account_id": 202,
            "name": "Key-Astra-Only",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-6-astra"],
            "cluster_id": "cluster_beta",
            "cluster_name": "Beta Cluster",
            "real_models_10m": [],
        },
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-6-astra",
        "results": [{"model": "gpt-6-astra", "probability": 0.95}],
    })
    svc._refresh_accounts(t0)

    # 查任一成员，models 是并集
    snap201 = svc.account_snapshot(201)
    snap202 = svc.account_snapshot(202)
    assert snap201["models"] == ["gpt-5.6-sol", "gpt-6-astra"]
    assert snap202["models"] == ["gpt-5.6-sol", "gpt-6-astra"]
    assert snap201["member_account_ids"] == [201, 202]

    # 手动指定测 gpt-6-astra -> 必须落到 202 上运行
    res = svc.enqueue_manual_account(201, model="gpt-6-astra")
    assert res.queued is True
    assert res.model == "gpt-6-astra"

    job = svc.db.claim_next_account(now=t0)
    assert job is not None
    assert job.model == "gpt-6-astra"

    svc._run_account_job(job)

    # transport 接收到的请求 account_id 必须是 202（支持该模型的成员）
    assert len(transport.calls) == 1
    assert transport.calls[0]["account_id"] == 202
    assert transport.calls[0]["model"] == "gpt-6-astra"

    # snapshot latest 带 account_id=202
    snap = svc.account_snapshot(201)
    assert snap["latest"]["account_id"] == 202


def test_first_key_fails_503_or_403_rotates_to_second_and_succeeds(tmp_path, fake_clock, monkeypatch):
    """第一个 Key 返回 503/403 → 同轮换到第二个 Key 并成功，只记一条结果且 account_id 为第二个；超时不换 Key。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 301,
            "name": "Key-Fail",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_gamma",
            "cluster_name": "Gamma Cluster",
            "real_models_10m": [],
        },
        {
            "account_id": 302,
            "name": "Key-Success",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_gamma",
            "cluster_name": "Gamma Cluster",
            "real_models_10m": [],
        },
    ])

    class FailingFirstKeyTransport:
        def __init__(self):
            self.calls = []

        def run(self, *, model, challenge, user_agent, session_affinity=None, account_id=None):
            self.calls.append(account_id)
            if account_id == 301:
                # 宿主返回 503 probe_account_unavailable
                return ProbeResult(
                    text=None,
                    status_code=503,
                    error_code="account_unavailable",
                    transport_detail={},
                )
            else:
                return ProbeResult(
                    text="1 " * 400,
                    status_code=200,
                    error_code=None,
                    transport_detail={"output_tokens": 100, "termination_event_received": True},
                )

    transport = FailingFirstKeyTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-sol",
        "results": [{"model": "gpt-5.6-sol", "probability": 0.95}],
    })
    svc._refresh_accounts(t0)

    svc.enqueue_manual_account(301, model="gpt-5.6-sol")
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    # 两次 transport 调用：先 301 后 302
    assert transport.calls == [301, 302]

    # 只记录一条 account_rounds 记录，且 account_id 为 302
    rounds = svc.db.get_account_rounds(302, limit=10)
    assert len(rounds) == 1
    assert rounds[0]["status"] in {"match", "uncertain"}

    rounds301 = svc.db.get_account_rounds(301, limit=10)
    assert len(rounds301) == 0  # 301 自身未记录失败，因为轮换到 302 成功

    # snapshot latest 显示 account_id == 302
    snap = svc.account_snapshot(301)
    assert snap["latest"]["account_id"] == 302
    # Account results carry the top candidates like monitor snapshots do.
    assert isinstance(snap["latest"]["ranking"], list) and len(snap["latest"]["ranking"]) <= 3
    assert all(set(c) == {"model", "probability"} for c in snap["latest"]["ranking"])
    assert snap["latest"]["status"] in {"match", "uncertain"}

    # The schedule stays on the upstream's representative (301), not on the key used.
    assert svc.db.get_account(301)["next_run_at"] is not None
    assert svc.db.get_account(302)["next_run_at"] is None

    # --- 测试超时（upstream_timeout）不换 Key ---
    class TimeoutTransport:
        def __init__(self):
            self.calls = []

        def run(self, *, model, challenge, user_agent, session_affinity=None, account_id=None):
            self.calls.append(account_id)
            return ProbeResult(
                text=None,
                status_code=504,
                error_code="upstream_timeout",
                transport_detail={},
            )

    timeout_transport = TimeoutTransport()
    svc.transport = timeout_transport
    # Advance clock past retest or clear retest
    fake_clock.advance(100)
    t1 = fake_clock()
    svc.enqueue_manual_account(302, model="gpt-5.6-sol")
    job2 = svc.db.claim_next_account(now=t1)
    svc._run_account_job(job2)

    # 超时不换 Key，每题只调用一次（不因超时换Key重试），循环直到5次上限
    assert len(timeout_transport.calls) == 5


def test_active_mode_cycles_within_real_models_and_next_model_is_readonly(tmp_path, fake_clock):
    """活跃时只在 real_models 中轮流；next_model 只读不推进。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 401,
            "name": "Cluster-Key",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol", "gpt-6-astra"],
            "cluster_id": "cluster_active",
            "cluster_name": "Active Cluster",
            "real_models_10m": ["gpt-6-astra", "gpt-5.6-sol"],  # astra 与 sol 活跃
            "last_real_request_at": utc_iso(t0 - 30),  # 30s ago -> active
            "real_requests_10m": 10,
        }
    ])

    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    target, _ = svc._get_target_and_members(401)
    assert target["mode"] == "active"
    assert target["real_models"] == ["gpt-6-astra", "gpt-5.6-sol"]

    # next_model 只读计算
    model1 = svc._compute_target_next_model(target)
    assert model1 == "gpt-6-astra"
    model2 = svc._compute_target_next_model(target)
    assert model2 == "gpt-6-astra"
    # index 未推进
    acc_row = svc.db.get_account(401)
    assert acc_row["last_model_index"] == 0

    # 真正选模型时会推进
    chosen = svc._select_model_for_target(target)
    assert chosen == "gpt-6-astra"
    acc_row_after = svc.db.get_account(401)
    assert acc_row_after["last_model_index"] == 1


def test_post_accounts_run_api_status_codes_and_responses(tmp_path, fake_clock, monkeypatch):
    """run 带 model：合法 202、非法 400、重复 409；成员 id 查询返回上游数据。"""
    monkeypatch.setenv("MODELTRACE_SECRET", "test-secret-456")
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 501,
            "name": "Cluster-Member-1",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol", "gpt-6-astra"],
            "cluster_id": "cluster_api",
            "cluster_name": "API Cluster",
            "real_models_10m": [],
        },
        {
            "account_id": 502,
            "name": "Cluster-Member-2",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_api",
            "cluster_name": "API Cluster",
            "real_models_10m": [],
        },
    ])

    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    app = create_app(svc, secret="test-secret-456", start_worker=False)
    client = app.test_client()
    headers = {"Authorization": "Bearer test-secret-456", "Content-Type": "application/json"}

    # 1. 查询 502（非代表）返回完整的上游目标数据，models 为并集
    res_get = client.get("/accounts/502", headers=headers)
    assert res_get.status_code == 200
    detail = res_get.get_json()
    assert detail["cluster_id"] == "cluster_api"
    assert detail["cluster_name"] == "API Cluster"
    assert detail["member_account_ids"] == [501, 502]
    assert detail["models"] == ["gpt-5.6-sol", "gpt-6-astra"]
    assert "next_model" in detail
    assert "per_model" in detail
    assert len(detail["per_model"]) == 2

    # 2. POST /accounts/502/run 带非法 model -> 400 model_not_supported
    res_bad = client.post("/accounts/502/run", json={"model": "claude-3-sonnet"}, headers=headers)
    assert res_bad.status_code == 400
    assert res_bad.get_json()["error"] == "model_not_supported"

    # 3. POST /accounts/502/run 带合法 model -> 202 {"queued": True, "model": ...}
    res_ok = client.post("/accounts/502/run", json={"model": "gpt-6-astra"}, headers=headers)
    assert res_ok.status_code == 202
    assert res_ok.get_json() == {"queued": True, "model": "gpt-6-astra"}

    # 4. 再次 POST /accounts/501/run（同集群排队中）-> 409 already_queued
    res_dup = client.post("/accounts/501/run", json={"model": "gpt-5.6-sol"}, headers=headers)
    assert res_dup.status_code == 409
    assert res_dup.get_json()["error"] == "already_queued"

    # 5. 未知账号 -> 404
    res_404 = client.post("/accounts/9999/run", json={}, headers=headers)
    assert res_404.status_code == 404


def test_interrupted_excluded_from_summary_and_requeued_within_60s(tmp_path, fake_clock):
    """interrupted 不计入 summary 且 60 秒内重排。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    svc = make_service(tmp_path, fake_clock)
    svc.db.upsert_account_sync(
        account_id=601,
        name="Key-Interrupted",
        platform="openai",
        type_="oauth",
        schedulable=True,
        models=["gpt-5.6-sol"],
        last_real_request_at=None,
        last_real_model=None,
        real_requests_10m=0,
        mode="idle",
        interval_seconds=3600,
        next_run_at=None,
        now=t0,
    )

    # 模拟正在运行的 job 被重启打断
    svc.db.enqueue_account(601, "gpt-5.6-sol", now=t0)
    job = svc.db.claim_next_account(now=t0)
    assert job is not None

    # 重启恢复
    interrupted_jobs = svc.db.recover_running_account_jobs(now=t0)
    assert len(interrupted_jobs) == 1
    assert interrupted_jobs[0] == (601, "gpt-5.6-sol")

    # 检查状态是否为 interrupted
    rounds = svc.db.get_account_rounds(601)
    assert len(rounds) == 1
    assert rounds[0]["status"] == "interrupted"
    assert rounds[0]["message_code"] == "worker_restarted"

    # 检查 summary：total=1，其余 match/suspect/uncertain/error 均为 0
    summary = svc.accounts_summary_for_model("gpt-5.6-sol")
    assert summary["total"] == 1
    assert summary["match"] == 0
    assert summary["suspect"] == 0
    assert summary["uncertain"] == 0
    assert summary["error"] == 0

    # 60 秒内重新排队
    for rec_acct_id, rec_model in interrupted_jobs:
        svc.db.enqueue_account(
            rec_acct_id,
            rec_model,
            now=t0,
            available_at=t0 + 60,
            trigger="scheduled",
            retest_index=0,
        )

    # 在 t0 + 30s 还不可 claim
    job_early = svc.db.claim_next_account(now=t0 + 30)
    assert job_early is None

    # 在 t0 + 60s 重新被 claim
    job_ready = svc.db.claim_next_account(now=t0 + 60)
    assert job_ready is not None
    assert job_ready.account_id == 601
    assert job_ready.model == "gpt-5.6-sol"


def test_legacy_database_migration_keeps_existing_data(tmp_path):
    """已有数据库升级（旧 accounts 表无新列）启动不报错且数据保留。"""
    db_path = tmp_path / "legacy.sqlite3"
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    # 模拟老版本 v0.1.13 accounts 表
    conn.execute("""
        CREATE TABLE accounts (
            account_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            platform TEXT,
            type TEXT,
            schedulable INTEGER NOT NULL DEFAULT 1,
            models_json TEXT NOT NULL DEFAULT '[]',
            last_real_request_at REAL,
            last_real_model TEXT,
            real_requests_10m INTEGER NOT NULL DEFAULT 0,
            mode TEXT NOT NULL DEFAULT 'idle',
            interval_seconds INTEGER NOT NULL DEFAULT 3600,
            next_run_at REAL,
            last_model_index INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL
        )
    """)
    conn.execute("""
        INSERT INTO accounts (account_id, name, platform, models_json, updated_at)
        VALUES (701, 'Legacy Account', 'openai', '["gpt-5.6-sol"]', 1700000000.0)
    """)
    conn.commit()
    conn.close()

    # 初始化 DB（会执行 schema & ALTER TABLE 迁移）
    db = ModelTraceDB(db_path)
    acc = db.get_account(701)
    assert acc is not None
    assert acc["name"] == "Legacy Account"
    assert acc["cluster_id"] is None
    assert acc["cluster_name"] is None
    assert acc["real_models_json"] == "[]"
    db.close()


def test_summary_and_fanout_counts_cluster_as_one(tmp_path, fake_clock):
    """summary 一个上游只算 1；扇出也只排 1 次。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 801,
            "name": "Cluster-Member-A",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_delta",
            "cluster_name": "Delta Cluster",
            "real_models_10m": [],
        },
        {
            "account_id": 802,
            "name": "Cluster-Member-B",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_delta",
            "cluster_name": "Delta Cluster",
            "real_models_10m": [],
        },
        {
            "account_id": 803,
            "name": "Cluster-Member-C",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_delta",
            "cluster_name": "Delta Cluster",
            "real_models_10m": [],
        },
    ])

    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    # 1. accounts_summary_for_model 只有一个上游目标，total 必须为 1
    summary = svc.accounts_summary_for_model("gpt-5.6-sol")
    assert summary["total"] == 1

    # 2. 扇出检测 (_enqueue_model_on_all_accounts) 对该模型也只排 1 次
    queued_cnt = svc._enqueue_model_on_all_accounts("gpt-5.6-sol", now=t0)
    assert queued_cnt == 1

    assert svc.db.has_pending_account(801) is True
    assert svc.db.has_pending_account(802) is False
    assert svc.db.has_pending_account(803) is False
