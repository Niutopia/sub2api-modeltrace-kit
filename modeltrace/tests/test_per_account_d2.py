"""Tests for ModelTrace per-account dynamic detection (Task D2 / Contract B1-B3)."""
from dataclasses import replace
import hashlib
import json
import pytest
import httpx

from modeltrace import __version__
from modeltrace.config import ServiceConfig, MonitorConfig, load_config
from modeltrace.service import ModelTraceService, HostClient, SAFE_MESSAGE_CODES
from modeltrace.transport import ProbeTransport, ProbeResult
from modeltrace.db import ModelTraceDB
from tests.conftest import build_service


class MockHostClient:
    def __init__(self, accounts_data=None):
        self.accounts_data = accounts_data or []
        self.fetch_calls = []
        self.pause_calls = []
        self.resume_calls = []

    def fetch_accounts(self, models: list[str]) -> list[dict]:
        self.fetch_calls.append(list(models))
        return self.accounts_data

    def pause_model(self, account_id: int, model: str, minutes: int, evidence: str) -> bool:
        self.pause_calls.append({
            "account_id": account_id,
            "model": model,
            "minutes": minutes,
            "evidence": evidence,
        })
        return True

    def resume_model(self, account_id: int, model: str) -> bool:
        self.resume_calls.append({
            "account_id": account_id,
            "model": model,
        })
        return True


class MockProbeTransport:
    def __init__(self, text="1 " * 400, error_code=None, status_code=200):
        self.text = text
        self.error_code = error_code
        self.status_code = status_code
        self.calls = []

    def run(self, *, model: str, challenge: dict, user_agent: str, session_affinity: str = None, account_id: int = None) -> ProbeResult:
        self.calls.append({
            "model": model,
            "challenge": challenge,
            "user_agent": user_agent,
            "session_affinity": session_affinity,
            "account_id": account_id,
        })
        return ProbeResult(
            text=self.text if self.error_code is None else None,
            status_code=self.status_code,
            error_code=self.error_code,
            transport_detail={"output_tokens": 100, "termination_event_received": True},
        )


def make_service(tmp_path, fake_clock, host_client=None, transport=None, config_overrides=None):
    overrides = {
        "host_api_base": "https://api.example.com",
        "active_interval_seconds": 300,
        "idle_interval_seconds": 3600,
        "active_window_seconds": 600,
        "per_account_enabled": True,
    }
    if config_overrides:
        overrides.update(config_overrides)

    config_path = tmp_path / "config.json"
    config_dict = {
        "base_url": "https://api.example.com/v1",
        "api_key": "test-key-not-used-by-fake",
        "enabled": True,
        "interval_seconds": 3600,
        "daily_budget_usd": 5,
        "max_output_tokens": 2048,
        "timeout_seconds": 120,
        "auto_retests": 0,
        "scope_label": "test",
        "monitors": {"1": {"model": "gpt-5.6-sol", "enabled": True}},
        "pricing_upper_bound": {
            "gpt-5.6-sol": {
                "input_per_1m_usd": 2.5,
                "cache_read_per_1m_usd": 1.25,
                "output_per_1m_usd": 15.0,
            }
        },
    }
    config_dict.update(overrides)
    config_path.write_text(json.dumps(config_dict), encoding="utf-8")
    cfg = load_config(config_path)

    svc = ModelTraceService(
        cfg,
        database_path=tmp_path / "modeltrace_account.sqlite3",
        transport=transport or MockProbeTransport(),
        clock=fake_clock,
        monotonic=fake_clock,
        sleeper=lambda _: None,
    )
    if host_client:
        svc.host_client = host_client
    return svc


def test_version_is_0113():
    assert __version__ == "0.1.17"


def test_account_unavailable_in_safe_message_codes():
    assert "account_unavailable" in SAFE_MESSAGE_CODES


def test_active_judgement_and_ten_minute_fallback(tmp_path, fake_clock):
    """Active when last_real_request_at <= 600s, fallback to idle when > 600s."""
    t0 = 1700000000.0
    fake_clock.current = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 1,
            "name": "acct-1",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "last_real_request_at": "2023-11-14T22:13:00Z", # t0 - 20s (active)
            "last_real_model": "gpt-5.6-sol",
            "real_requests_10m": 5,
        }
    ])
    # Let's adjust last_real_request_at to t0 - 200s
    last_req_str = "2023-11-14T22:09:40Z" # 1700000000 - 200 = 1699999800
    host.accounts_data[0]["last_real_request_at"] = "2023-11-14T22:10:00Z"
    # Actually let's format from timestamp
    from modeltrace.service import utc_iso
    host.accounts_data[0]["last_real_request_at"] = utc_iso(t0 - 200.0)

    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    snap = svc.account_snapshot(1)
    assert snap["mode"] == "active"
    assert snap["interval_seconds"] == 300

    # Advance clock by 401s -> now last_real_request is 601s ago (> 600s) -> fallback to idle
    fake_clock.advance(401)
    t1 = fake_clock()
    svc._refresh_accounts(t1)

    snap1 = svc.account_snapshot(1)
    assert snap1["mode"] == "idle"
    assert snap1["interval_seconds"] == 3600


def test_enter_active_enqueues_immediately_if_over_300s(tmp_path, fake_clock):
    """When an account transitions from idle to active, if >300s since last check, enqueue immediately."""
    from modeltrace.service import utc_iso
    t0 = 1700000000.0
    fake_clock.current = t0

    # Initially idle (no recent real request)
    host = MockHostClient(accounts_data=[
        {
            "account_id": 1,
            "name": "acct-1",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "last_real_request_at": None,
            "last_real_model": None,
            "real_requests_10m": 0,
        }
    ])
    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    svc._refresh_accounts(t0)
    snap0 = svc.account_snapshot(1)
    assert snap0["mode"] == "idle"
    assert snap0["next_run_at"] is not None

    # Suppose it was checked 400 seconds ago (or never checked)
    # Now user sends a real request -> account becomes active!
    fake_clock.advance(400)
    t1 = fake_clock()
    host.accounts_data[0]["last_real_request_at"] = utc_iso(t1 - 10.0)
    host.accounts_data[0]["last_real_model"] = "gpt-5.6-sol"
    svc._refresh_accounts(t1)

    snap1 = svc.account_snapshot(1)
    assert snap1["mode"] == "active"
    # next_run_at should now be <= t1 (queued / due immediately!)
    assert snap1["next_run_at"] == utc_iso(t1)


def test_idle_anchors_spread_across_ten_accounts(tmp_path, fake_clock):
    """10 accounts have distinct idle anchors staggered across the hour."""
    t0 = 1700000000.0
    fake_clock.current = t0

    accounts_data = [
        {
            "account_id": i,
            "name": f"acct-{i}",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "last_real_request_at": None,
            "last_real_model": None,
            "real_requests_10m": 0,
        }
        for i in range(1, 11)
    ]
    host = MockHostClient(accounts_data=accounts_data)
    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    snaps = [svc.account_snapshot(i) for i in range(1, 11)]
    next_runs = [s["next_run_at"] for s in snaps]

    # Verify all 10 are idle and have distinct next_run_at
    assert all(s["mode"] == "idle" for s in snaps)
    assert len(set(next_runs)) == 10, f"Expected 10 distinct anchors, got: {next_runs}"


def test_no_backlog_replay_and_no_duplicate_queue(tmp_path, fake_clock):
    """Overdue time slots do not accumulate backlogs; duplicate runs cannot be queued."""
    t0 = 1700000000.0
    fake_clock.current = t0
    from modeltrace.service import utc_iso

    host = MockHostClient(accounts_data=[
        {
            "account_id": 1,
            "name": "acct-1",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "last_real_request_at": utc_iso(t0),
            "last_real_model": "gpt-5.6-sol",
            "real_requests_10m": 1,
        }
    ])
    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    svc._refresh_accounts(t0)

    # First schedule
    svc._schedule_due_accounts(t0)
    assert svc.db.has_pending_account(1) is True

    # Attempt duplicate enqueue
    res2, _ = svc.db.enqueue_account(1, "gpt-5.6-sol", now=t0)
    assert res2 is False

    # Skip 10000 seconds into the future without running worker
    fake_clock.advance(10000)
    t_future = fake_clock()
    svc._schedule_due_accounts(t_future)

    # There should still only be 1 pending item in queue, not dozens!
    pending = svc.db.get_all_accounts()
    assert len(pending) == 1


def test_retest_targets_same_account_and_model(tmp_path, fake_clock, monkeypatch):
    """新流程：单个任务内连续出题，不再排跨任务 auto_retest。"""
    from modeltrace.service import utc_iso
    t0 = 1700000000.0
    fake_clock.current = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 17,
            "name": "acct-17",
            "schedulable": True,
            "models": ["gpt-5.6-sol", "gpt-6-astra"],
            "last_real_request_at": utc_iso(t0),
            "last_real_model": "gpt-5.6-sol",
            "real_requests_10m": 1,
        }
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    svc._refresh_accounts(t0)

    # Enqueue manual or scheduled
    svc._schedule_due_accounts(t0)
    job = svc.db.claim_next_account(now=t0)
    assert job is not None
    assert job.account_id == 17
    assert job.model == "gpt-5.6-sol"

    # 执行 job
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-sol",
        "results": [{"model": "gpt-5.6-sol", "probability": 0.95}],
    })
    svc._run_account_job(job)

    # 验证新流程在一个任务内完成，不会再 enqueue auto_retest 任务
    assert svc.db.claim_next_account(now=t0) is None
    snap = svc.account_snapshot(17)
    assert snap["retest_progress"] is None
    assert snap["latest"]["status"] == "match"


def test_transport_injects_a2_headers(monkeypatch):
    """ProbeTransport injects X-ModelTrace-Probe and X-ModelTrace-Account when account_id and probe_secret are provided."""
    captured_requests = []

    def fake_handler(request: httpx.Request):
        captured_requests.append(request)
        return httpx.Response(200, json={"status": "completed", "output_text": "1 " * 400})

    transport = ProbeTransport(
        endpoint="https://api.example.com/v1/responses",
        api_key="test-key",
        probe_secret="test-secret-xyz",
        client_factory=lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(fake_handler)),
    )

    res = transport.run(
        model="gpt-5.6-sol",
        challenge={"prompt": "test", "expected_count": 100},
        user_agent="ModelTraceProbe/test",
        account_id=17,
    )
    assert res.error_code is None
    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert req.headers.get("X-ModelTrace-Probe") == "test-secret-xyz"
    assert req.headers.get("X-ModelTrace-Account") == "17"


def test_account_unavailable_classification():
    """503 response with probe_account_unavailable is classified as account_unavailable."""
    def error_503_handler(request: httpx.Request):
        return httpx.Response(
            503,
            headers={"content-type": "application/json"},
            json={"error": {"code": "probe_account_unavailable", "message": "Account offline"}},
        )

    transport = ProbeTransport(
        endpoint="https://api.example.com/v1/responses",
        api_key="test-key",
        probe_secret="test-secret-xyz",
        client_factory=lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(error_503_handler)),
    )

    res = transport.run(
        model="gpt-5.6-sol",
        challenge={"prompt": "test"},
        user_agent="ModelTraceProbe/test",
        account_id=17,
    )
    assert res.status_code == 503
    assert res.error_code == "account_unavailable"


def test_b2_and_b3_output_structure(tmp_path, fake_clock):
    """Test GET /accounts and snapshot accounts_summary (B2 & B3)."""
    t0 = 1700000000.0
    fake_clock.current = t0
    from modeltrace.service import utc_iso

    host = MockHostClient(accounts_data=[
        {
            "account_id": 17,
            "name": "acc-17@example.com",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol", "gpt-6-astra"],
            "last_real_request_at": utc_iso(t0),
            "last_real_model": "gpt-5.6-sol",
            "real_requests_10m": 3,
        }
    ])
    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    # Insert a fake completed round for account 17
    svc.db.enqueue_account(17, "gpt-5.6-sol", now=t0)
    job = svc.db.claim_next_account(now=t0)
    svc.db.record_account_round_and_finish(
        job,
        status="match",
        target_probability=0.85,
        best_model="gpt-5.6-sol",
        checked_at=t0,
        message_code="compatible",
        reasoning_effort="low",
        ranking=[{"model": "gpt-5.6-sol", "probability": 0.85}],
        diagnostics={},
        next_run_at=t0 + 300,
    )

    # Check B2: GET /accounts/:account_id
    detail = svc.account_snapshot(17)
    assert detail["account_id"] == 17
    assert detail["name"] == "acc-17@example.com"
    assert detail["models"] == ["gpt-5.6-sol", "gpt-6-astra"]
    assert detail["mode"] == "active"
    assert detail["interval_seconds"] == 300
    assert detail["running"] is False
    assert detail["queued"] is False
    assert detail["latest"]["model"] == "gpt-5.6-sol"
    assert detail["latest"]["status"] == "match"
    assert detail["latest"]["target_probability"] == 0.85
    assert detail["latest"]["best_model"] == "gpt-5.6-sol"
    assert detail["latest"]["reasoning_effort"] == "low"
    assert isinstance(detail["history"], list)

    # Check B2: GET /accounts
    all_accounts = svc.all_accounts_snapshot()
    assert "accounts" in all_accounts
    assert len(all_accounts["accounts"]) == 1
    assert all_accounts["accounts"][0]["account_id"] == 17

    # Check B3: accounts_summary on monitor snapshot
    monitor_snap = svc.snapshot(1)
    summary = monitor_snap["accounts_summary"]
    assert summary["model"] == "gpt-5.6-sol"
    assert summary["total"] == 1
    assert summary["match"] == 1
    assert summary["suspect"] == 0
    assert summary["uncertain"] == 0
    assert summary["error"] == 0
    assert summary["active"] == 1
    # Check that privacy is maintained (no account name or ID in accounts_summary)
    assert "account_id" not in summary
    assert "name" not in summary
    # A sol result says nothing about astra on the same account.
    astra = svc.accounts_summary_for_model("gpt-6-astra")
    assert astra["total"] == 1
    assert astra["match"] == astra["suspect"] == astra["uncertain"] == astra["error"] == 0


def test_old_monitor_endpoints_and_api_remain_available(tmp_path, fake_clock, monkeypatch):
    """Old monitor endpoints GET /v1/monitors/:id and POST /v1/monitors/:id/run remain available."""
    from app import create_app
    monkeypatch.setenv("MODELTRACE_SECRET", "test-secret-123")

    host = MockHostClient()
    svc = make_service(tmp_path, fake_clock, host_client=host)
    app = create_app(svc, secret="test-secret-123", start_worker=False)
    client = app.test_client()

    headers = {"Authorization": "Bearer test-secret-123"}

    # GET /v1/monitors/1
    res = client.get("/v1/monitors/1", headers=headers)
    assert res.status_code == 200
    data = res.get_json()
    assert data["monitor_id"] == 1
    assert "accounts_summary" in data

    # POST /accounts/17/run
    # First seed account 17
    svc.db.upsert_account_sync(
        account_id=17,
        name="test",
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
        now=fake_clock(),
    )
    res_run = client.post("/accounts/17/run", headers=headers)
    assert res_run.status_code == 202
    assert res_run.get_json()["queued"] is True

    # GET /accounts
    res_accs = client.get("/accounts", headers=headers)
    assert res_accs.status_code == 200
    assert len(res_accs.get_json()["accounts"]) == 1

    # GET /accounts/17
    res_acc = client.get("/accounts/17", headers=headers)
    assert res_acc.status_code == 200
    assert res_acc.get_json()["account_id"] == 17


def test_retire_missing_accounts_on_successful_fetch(tmp_path, fake_clock):
    """When fetch_accounts succeeds:
    1. Accounts missing from the list are marked retired (schedulable=False, mode='retired', next_run_at=None).
    2. Pending queued jobs for retired accounts are deleted.
    3. Historical records are preserved.
    4. When fetch_accounts fails, missing accounts are NOT retired.
    5. Reappearing accounts are restored (active/idle recalculated).
    6. GET /accounts does not return retired accounts, GET /accounts/:id returns them with mode='retired'.
    7. accounts_summary does not count retired accounts.
    """
    from modeltrace.service import utc_iso
    from app import create_app

    t0 = 1700000000.0
    fake_clock.current = t0

    # Initially 2 accounts
    host = MockHostClient(accounts_data=[
        {
            "account_id": 10,
            "name": "Account 10",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "last_real_request_at": utc_iso(t0),
            "last_real_model": "gpt-5.6-sol",
            "real_requests_10m": 1,
        },
        {
            "account_id": 20,
            "name": "Account 20",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "last_real_request_at": None,
            "last_real_model": None,
            "real_requests_10m": 0,
        },
    ])

    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    # Both accounts should be present
    acc10 = svc.account_snapshot(10)
    acc20 = svc.account_snapshot(20)
    assert acc10["mode"] == "active"
    assert acc20["mode"] == "idle"

    # Queue a job for account 20 and record a historical round for it
    svc.db.enqueue_account(20, "gpt-5.6-sol", now=t0)
    assert svc.db.has_pending_account(20) is True

    job = svc.db.claim_next_account(now=t0)
    svc.db.record_account_round_and_finish(
        job,
        status="match",
        target_probability=0.9,
        best_model="gpt-5.6-sol",
        checked_at=t0,
        message_code="compatible",
        reasoning_effort="low",
        ranking=[],
        diagnostics={},
        next_run_at=t0 + 3600,
    )
    # Now re-queue account 20 so it has an unstarted queued job
    svc.db.enqueue_account(20, "gpt-5.6-sol", now=t0)
    assert svc.db.has_pending_account(20) is True

    # 4. Test fetch failure -> missing accounts are NOT retired
    class FailingHostClient:
        def fetch_accounts(self, models):
            return host.accounts_data, False

    svc.host_client = FailingHostClient()
    # Even if failing client returns empty list
    class ErrorHostClient:
        def fetch_accounts(self, models):
            return [], False

    svc.host_client = ErrorHostClient()
    svc._refresh_accounts(t0 + 10)
    acc20_unmodified = svc.account_snapshot(20)
    assert acc20_unmodified["mode"] == "idle"
    assert svc.db.has_pending_account(20) is True

    # 1 & 2 & 3. Successful fetch missing account 20 -> account 20 retired, queue removed, history kept
    class SuccessHostClientOnly10:
        def fetch_accounts(self, models):
            return [host.accounts_data[0]], True

    svc.host_client = SuccessHostClientOnly10()
    svc._refresh_accounts(t0 + 20)

    acc20_row = svc.db.get_account(20)
    assert acc20_row["schedulable"] == 0
    assert acc20_row["mode"] == "retired"
    assert acc20_row["next_run_at"] is None

    # Queued job deleted
    assert svc.db.has_pending_account(20) is False

    # 6. GET /accounts and GET /accounts/:id behavior
    all_snap = svc.all_accounts_snapshot()
    ids_in_all = [a["account_id"] for a in all_snap["accounts"]]
    assert 10 in ids_in_all
    assert 20 not in ids_in_all

    snap20 = svc.account_snapshot(20)
    assert snap20["account_id"] == 20
    assert snap20["mode"] == "retired"
    # History preserved
    assert len(snap20["history"]) == 1
    assert snap20["latest"]["status"] == "match"

    # 7. accounts_summary does not count retired accounts
    summary = svc.accounts_summary_for_model("gpt-5.6-sol")
    assert summary["total"] == 1  # only account 10, account 20 excluded
    assert summary["active"] == 1
    assert summary["match"] == 0  # account 10 has no rounds yet, account 20 match ignored

    # 5. Account 20 reappears -> automatically restored
    class ReappearingHostClient:
        def fetch_accounts(self, models):
            return [
                host.accounts_data[0],
                {
                    "account_id": 20,
                    "name": "Account 20",
                    "schedulable": True,
                    "models": ["gpt-5.6-sol"],
                    "last_real_request_at": None,
                    "last_real_model": None,
                    "real_requests_10m": 0,
                },
            ], True

    svc.host_client = ReappearingHostClient()
    svc._refresh_accounts(t0 + 30)

    restored20 = svc.account_snapshot(20)
    assert restored20["mode"] == "idle"
    assert restored20["next_run_at"] is not None
    assert svc.db.get_account(20)["schedulable"] == 1

    # summary now counts both
    summary_restored = svc.accounts_summary_for_model("gpt-5.6-sol")
    assert summary_restored["total"] == 2
    assert summary_restored["match"] == 1  # historical round is counted again!


def test_host_client_fetch_accounts_tuple_return(monkeypatch):
    """HostClient.fetch_accounts returns (accounts, True) on 200, (last_accounts, False) on failure."""
    hc = HostClient(host_api_base="https://api.example.com", secret="test-sec")

    # Mock 200 response
    class Mock200Resp:
        status_code = 200
        def json(self):
            return {"accounts": [{"account_id": 1, "name": "a1"}]}

    class MockClientOk:
        def get(self, url, headers=None, timeout=None):
            return Mock200Resp()

    hc.client = MockClientOk()
    accs, ok = hc.fetch_accounts(["gpt-5.6-sol"])
    assert ok is True
    assert len(accs) == 1
    assert accs[0]["account_id"] == 1

    # Mock 500 failure response
    class Mock500Resp:
        status_code = 500
        def json(self):
            return {"error": "server_error"}

    class MockClientErr:
        def get(self, url, headers=None, timeout=None):
            return Mock500Resp()

    hc.client = MockClientErr()
    accs_fail, ok_fail = hc.fetch_accounts(["gpt-5.6-sol"])
    assert ok_fail is False
    assert len(accs_fail) == 1  # holds last accounts


def test_host_client_default_transport_fetches_accounts(monkeypatch):
    # Production constructs HostClient without an injected client; this path
    # must build its own httpx client (regression: httpx was never imported).
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("Authorization")
        return httpx.Response(200, json={"generated_at": "2026-09-24T00:00:00Z", "accounts": [{"account_id": 7}]})

    real_client = httpx.Client
    monkeypatch.setattr(
        "modeltrace.service.httpx.Client",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    client = HostClient("http://sub2api:8080/", secret="s3cret")
    accounts, ok = client.fetch_accounts(["gpt-6-astra", "gpt-5.6-sol"])
    assert ok is True
    assert accounts == [{"account_id": 7}]
    assert seen["url"] == "http://sub2api:8080/api/v1/internal/modeltrace/accounts?models=gpt-6-astra,gpt-5.6-sol"
    assert seen["auth"] == "Bearer s3cret"


def test_manual_monitor_run_is_served_and_fans_out_in_per_account_mode(tmp_path, fake_clock):
    svc = make_service(tmp_path, fake_clock, host_client=MockHostClient())
    for account_id, models in ((17, ["gpt-5.6-sol"]), (18, ["gpt-6-astra"])):
        svc.db.upsert_account_sync(
            account_id=account_id, name="t", platform="openai", type_="oauth", schedulable=True,
            models=models, last_real_request_at=None, last_real_model=None, real_requests_10m=0,
            mode="idle", interval_seconds=3600, next_run_at=fake_clock() + 3000, now=fake_clock(),
        )

    svc.enqueue_manual(1, expected_model="gpt-5.6-sol")
    # The click also refreshes every account that serves this model, and only those.
    assert svc.db.has_pending_account(17) is True
    assert svc.db.has_pending_account(18) is False

    served = []

    def fake_run(job):
        served.append(job)
        svc._stop.set()

    svc._run_job_safely = fake_run
    svc._last_account_refresh_at = fake_clock()
    svc._worker_loop()
    # Regression: in per-account mode the monitor queue was never drained, so a
    # channel-page click stayed "queued" forever.
    assert len(served) == 1 and served[0].monitor_id == 1
