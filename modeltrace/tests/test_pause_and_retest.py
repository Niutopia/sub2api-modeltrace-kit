import json
import sqlite3
import pytest
from dataclasses import replace
from datetime import datetime, timezone, timedelta

from modeltrace.db import ModelTraceDB
from modeltrace.service import (
    ModelTraceService,
    utc_iso,
)
from modeltrace.transport import ProbeResult
from tests.test_per_account_d2 import MockHostClient, MockProbeTransport, make_service


def test_retest_upstream_error_retries_next_key_in_same_job_and_succeeds(tmp_path, fake_clock, monkeypatch):
    """某题 Key 自身 504 当场换 Key；换 Key 成功后继续并得出结论。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 101,
            "name": "Cluster-1",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "c1",
            "cluster_name": "Cluster One",
        },
        {
            "account_id": 102,
            "name": "Cluster-2",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "c1",
            "cluster_name": "Cluster One",
        },
    ])

    class RetestRetryTransport:
        def __init__(self):
            self.calls = []

        def run(self, *, model, challenge, user_agent, session_affinity=None, account_id=None):
            self.calls.append(account_id)
            if account_id == 101:
                # 101 fails with key error (e.g. 503)
                return ProbeResult(None, 503, "account_unavailable", transport_detail={})
            else:
                # 102 succeeds
                return ProbeResult(
                    text="1 " * 400,
                    status_code=200,
                    error_code=None,
                    transport_detail={"output_tokens": 100, "termination_event_received": True},
                )

    transport = RetestRetryTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-sol",
        "results": [{"model": "gpt-5.6-sol", "probability": 0.95}],
    })
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(101, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    # 101 换 Key 到 102，102 结果为 compatible 立即结束
    assert transport.calls == [101, 102]
    rounds = svc.db.get_account_rounds(102, limit=5)
    assert len(rounds) == 1
    assert rounds[0]["status"] == "match"
    assert rounds[0]["message_code"] == "compatible"


def test_first_question_compatible_finishes_immediately(tmp_path, fake_clock, monkeypatch):
    """第 1 题一致 → 1 次请求就 match。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 201,
            "name": "Account-A",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
        },
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-sol",
        "results": [{"model": "gpt-5.6-sol", "probability": 0.95}],
    })
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(201, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    assert len(transport.calls) == 1
    latest = svc.db.get_account_latest_round(201)
    assert latest["status"] == "match"
    assert latest["message_code"] == "compatible"
    assert latest["best_model"] == "gpt-5.6-sol"
    diag = svc.db.decode_diagnostics(latest)
    assert diag["request_count"] == 1
    assert diag["valid_count"] == 1
    assert diag["wrong_count"] == 0
    assert len(diag["attempts"]) == 1
    assert diag["attempts"][0]["outcome"] == "compatible"


def test_wrong_then_compatible_two_requests_match(tmp_path, fake_clock, monkeypatch):
    """不对、一致 → 2 次请求 match。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 201,
            "name": "Account-A",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
        },
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    
    # 模拟第1题 difference_signal，第2题 compatible
    call_idx = 0
    def mock_analyze(*a, **k):
        nonlocal call_idx
        call_idx += 1
        if call_idx == 1:
            return {
                "prediction": "gpt-5.6-luna",
                "results": [
                    {"model": "gpt-5.6-luna", "probability": 0.90},
                    {"model": "gpt-5.6-sol", "probability": 0.05},
                ],
            }
        else:
            return {
                "prediction": "gpt-5.6-sol",
                "results": [
                    {"model": "gpt-5.6-sol", "probability": 0.92},
                    {"model": "gpt-5.6-luna", "probability": 0.08},
                ],
            }

    monkeypatch.setattr("modeltrace.service.analyze_outputs", mock_analyze)
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(201, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    assert len(transport.calls) == 2
    latest = svc.db.get_account_latest_round(201)
    assert latest["status"] == "match"
    assert latest["message_code"] == "compatible"
    assert latest["best_model"] == "gpt-5.6-sol"
    diag = svc.db.decode_diagnostics(latest)
    assert diag["request_count"] == 2
    assert diag["valid_count"] == 2
    assert diag["wrong_count"] == 1
    assert len(diag["attempts"]) == 2
    assert diag["attempts"][0]["outcome"] == "difference_signal"
    assert diag["attempts"][1]["outcome"] == "compatible"


def test_three_difference_signals_suspect_repeated_other_model_and_pauses(tmp_path, fake_clock, monkeypatch):
    """3 次“明显像 luna”→ suspect/repeated_other_model，best_model=luna，并触发 pause。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 201,
            "name": "Cluster-A",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "c2",
            "cluster_name": "Cluster Two",
        },
        {
            "account_id": 202,
            "name": "Cluster-B",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "c2",
            "cluster_name": "Cluster Two",
        },
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport,
        config_overrides={"auto_pause_calibrations": [
            {"account_id": a["account_id"], "model": "gpt-5.6-sol",
             "reasoning_effort": "none", "reference": "mock-calibration-fixture"}
            for a in host.accounts_data]})
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-luna",
        "results": [
            {"model": "gpt-5.6-luna", "probability": 0.90},
            {"model": "gpt-5.6-sol", "probability": 0.05},
        ],
    })
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(201, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    assert len(transport.calls) == 3
    latest = svc.db.get_account_latest_round(201)
    assert latest["status"] == "suspect"
    assert latest["message_code"] == "repeated_other_model"
    assert latest["best_model"] == "gpt-5.6-luna"
    diag = svc.db.decode_diagnostics(latest)
    assert diag["request_count"] == 3
    assert diag["valid_count"] == 3
    assert diag["wrong_count"] == 3
    assert len(diag["attempts"]) == 3
    assert all(a["outcome"] in ("difference_signal", "repeated_difference") for a in diag["attempts"])
    assert all(a["prediction"] == "gpt-5.6-luna" for a in diag["attempts"])

    # 触发 pause
    assert len(host.pause_calls) == 2
    assert {c["account_id"] for c in host.pause_calls} == {201, 202}
    assert all(c["model"] == "gpt-5.6-sol" for c in host.pause_calls)
    assert all(c["evidence"] == "repeated_other_model gpt-5.6-luna" for c in host.pause_calls)


def test_inconclusive_twice_and_wrong_three_times_five_requests_suspect(tmp_path, fake_clock, monkeypatch):
    """说不准 ×2 + 不对 ×3 → 5 次请求 suspect。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 201,
            "name": "Account-A",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
        },
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)

    call_count = 0
    def mock_analyze(*a, **k):
        nonlocal call_count
        call_count += 1
        if call_count in (1, 2):
            # 说不准 (inconclusive): top model is gpt-5.6-sol but prob < 0.5, or not high diff
            return {
                "prediction": "gpt-5.6-sol",
                "results": [
                    {"model": "gpt-5.6-sol", "probability": 0.40},
                    {"model": "gpt-5.6-luna", "probability": 0.35},
                ],
            }
        else:
            # 不对 (difference_signal):
            return {
                "prediction": "gpt-5.6-luna",
                "results": [
                    {"model": "gpt-5.6-luna", "probability": 0.90},
                    {"model": "gpt-5.6-sol", "probability": 0.05},
                ],
            }

    monkeypatch.setattr("modeltrace.service.analyze_outputs", mock_analyze)
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(201, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    assert len(transport.calls) == 5
    latest = svc.db.get_account_latest_round(201)
    assert latest["status"] == "suspect"
    assert latest["message_code"] == "repeated_other_model"
    assert latest["best_model"] == "gpt-5.6-luna"
    diag = svc.db.decode_diagnostics(latest)
    assert diag["request_count"] == 5
    assert diag["questions_valid"] == 5
    assert diag["wrong_count"] == 3
    assert len(diag["attempts"]) == 5
    assert [a["outcome"] for a in diag["attempts"]] == [
        "inconclusive", "inconclusive", "difference_signal", "repeated_difference", "repeated_difference"
    ]


def test_inconclusive_five_times_uncertain_no_pause(tmp_path, fake_clock, monkeypatch):
    """说不准 ×5 → uncertain，不 pause。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 201,
            "name": "Account-A",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
        },
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-sol",
        "results": [
            {"model": "gpt-5.6-sol", "probability": 0.40},
            {"model": "gpt-5.6-luna", "probability": 0.35},
        ],
    })
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(201, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    assert len(transport.calls) == 5
    latest = svc.db.get_account_latest_round(201)
    assert latest["status"] == "uncertain"
    assert latest["message_code"] == "low_or_competing_probability"
    diag = svc.db.decode_diagnostics(latest)
    assert diag["request_count"] == 5
    assert diag["questions_valid"] == 5
    assert diag["wrong_count"] == 0
    assert len(diag["attempts"]) == 5
    assert all(a["outcome"] == "inconclusive" for a in diag["attempts"])
    assert len(host.pause_calls) == 0


def test_error_five_times_and_error_in_between_not_counted(tmp_path, fake_clock, monkeypatch):
    """错误 ×5 → error；错误夹在中间不计数。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 201,
            "name": "Account-A",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
        },
    ])

    # 1. 全部 5 次错误
    error_transport = MockProbeTransport(error_code="upstream_timeout", status_code=504)
    (tmp_path / "err1").mkdir(exist_ok=True)
    svc1 = make_service(tmp_path / "err1", fake_clock, host_client=host, transport=error_transport)
    svc1._refresh_accounts(t0)
    svc1.db.enqueue_account(201, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job1 = svc1.db.claim_next_account(now=t0)
    svc1._run_account_job(job1)

    assert len(error_transport.calls) == 5
    latest1 = svc1.db.get_account_latest_round(201)
    assert latest1["status"] == "error"
    assert latest1["message_code"] == "upstream_timeout"
    diag1 = svc1.db.decode_diagnostics(latest1)
    assert diag1["request_count"] == 5
    assert diag1["questions_valid"] == 0
    assert diag1["wrong_count"] == 0
    assert len(diag1["attempts"]) == 5
    assert all(a["error_code"] == "upstream_timeout" for a in diag1["attempts"])

    # 2. 错误夹在中间不计入不对或说不准：
    # 比如：不对、超时、不对、超时、不对 -> 5次请求，3个不对 -> suspect
    call_num = 0
    class IntermittentTransport:
        def __init__(self):
            self.calls = []
        def run(self, *, model, challenge, user_agent, session_affinity=None, account_id=None):
            nonlocal call_num
            call_num += 1
            self.calls.append(account_id)
            if call_num in (2, 4):
                return ProbeResult(None, 504, "upstream_timeout", transport_detail={})
            return ProbeResult(text="1 " * 400, status_code=200, error_code=None, transport_detail={"output_tokens": 100, "termination_event_received": True})

    intermittent = IntermittentTransport()
    (tmp_path / "err2").mkdir(exist_ok=True)
    svc2 = make_service(tmp_path / "err2", fake_clock, host_client=host, transport=intermittent)
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-luna",
        "results": [
            {"model": "gpt-5.6-luna", "probability": 0.90},
            {"model": "gpt-5.6-sol", "probability": 0.05},
        ],
    })
    svc2._refresh_accounts(t0)
    svc2.db.enqueue_account(201, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job2 = svc2.db.claim_next_account(now=t0)
    svc2._run_account_job(job2)

    assert len(intermittent.calls) == 5
    latest2 = svc2.db.get_account_latest_round(201)
    assert latest2["status"] == "suspect"
    assert latest2["message_code"] == "repeated_other_model"
    diag2 = svc2.db.decode_diagnostics(latest2)
    assert diag2["request_count"] == 5
    assert diag2["questions_valid"] == 3
    assert diag2["wrong_count"] == 3
    assert len(diag2["attempts"]) == 5
    assert [a["outcome"] for a in diag2["attempts"]] == [
        "difference_signal", None, "repeated_difference", None, "repeated_difference"
    ]


def test_upstream_multi_key_retry_counts_towards_five_request_cap(tmp_path, fake_clock, monkeypatch):
    """上游多 Key：某题 Key 自身 503 当场换 Key，且计入 5 次上限；结论只记一条 round，diagnostics.attempts 长度 = 实际请求数。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 501,
            "name": "Multi-1",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_multi",
            "cluster_name": "Multi Cluster",
        },
        {
            "account_id": 502,
            "name": "Multi-2",
            "platform": "openai",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "cluster_id": "cluster_multi",
            "cluster_name": "Multi Cluster",
        },
    ])

    # 501 总是 503 account_unavailable（换 Key），502 总是 inconclusive
    class MultiKeyTransport:
        def __init__(self):
            self.calls = []
        def run(self, *, model, challenge, user_agent, session_affinity=None, account_id=None):
            self.calls.append(account_id)
            if account_id == 501:
                return ProbeResult(None, 503, "account_unavailable", transport_detail={})
            return ProbeResult(text="1 " * 400, status_code=200, error_code=None, transport_detail={"output_tokens": 100, "termination_event_received": True})

    multi_trans = MultiKeyTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=multi_trans)
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-sol",
        "results": [
            {"model": "gpt-5.6-sol", "probability": 0.40},
            {"model": "gpt-5.6-luna", "probability": 0.35},
        ],
    })
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(501, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    # 第1题：501(503换Key) -> 502(inconclusive) (请求2次)
    # 第2题：501(503换Key) -> 502(inconclusive) (请求2次)
    # 第3题：501(503换Key) -> 达到5次上限！ (请求1次)
    assert len(multi_trans.calls) == 5
    assert multi_trans.calls == [501, 502, 502, 501, 502]

    # 只记一条 round
    rounds_501 = svc.db.get_account_rounds(501, limit=10)
    rounds_502 = svc.db.get_account_rounds(502, limit=10)
    assert len(rounds_501) + len(rounds_502) == 1

    # 最新 round 的 diagnostics.attempts 长度 = 5
    snap = svc.account_snapshot(501)
    latest = snap["latest"]
    assert latest["status"] == "uncertain"
    # target_probability / best_model / ranking 取最后一个测成的题 (502测成的题)
    assert latest["account_id"] == 502

    diag = svc.db.decode_diagnostics(svc.db.get_account_rounds(502)[0])
    assert diag["request_count"] == 5
    assert len(diag["attempts"]) == 5
    assert diag["questions_valid"] == 3
    assert diag["wrong_count"] == 0


def test_suspect_pauses_apikey_account_with_60_minutes(tmp_path, fake_clock, monkeypatch):
    """API Key 账号 suspect 暂停 60 分钟。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 301,
            "name": "APIKey-Account",
            "platform": "openai",
            "type": "apikey",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
        },
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport,
        config_overrides={"auto_pause_calibrations": [
            {"account_id": a["account_id"], "model": "gpt-5.6-sol",
             "reasoning_effort": "none", "reference": "mock-calibration-fixture"}
            for a in host.accounts_data]})
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-luna",
        "results": [
            {"model": "gpt-5.6-luna", "probability": 0.90},
            {"model": "gpt-5.6-sol", "probability": 0.05},
        ],
    })
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(301, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    # API Key account uses pause_minutes_apikey (default 60)
    assert len(host.pause_calls) == 1
    assert host.pause_calls[0]["account_id"] == 301
    assert host.pause_calls[0]["minutes"] == 60


def test_auto_pause_enabled_false_does_not_call_host(tmp_path, fake_clock, monkeypatch):
    """auto_pause_enabled=false 时不调用 pause/resume。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 401,
            "name": "Test-Acct",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
        },
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport, config_overrides={"auto_pause_enabled": False})
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-luna",
        "results": [
            {"model": "gpt-5.6-luna", "probability": 0.90},
            {"model": "gpt-5.6-sol", "probability": 0.05},
        ],
    })
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(401, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    # No pause calls made
    assert len(host.pause_calls) == 0


def test_host_client_exception_does_not_prevent_round_recording(tmp_path, fake_clock, monkeypatch):
    """调用抛异常不影响结果记录。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    class ExplodingHostClient(MockHostClient):
        def pause_model(self, account_id, model, minutes, evidence):
            raise RuntimeError("host connection broke")

    host = ExplodingHostClient(accounts_data=[
        {
            "account_id": 501,
            "name": "Test-Acct",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
        },
    ])

    transport = MockProbeTransport()
    svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport)
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *a, **k: {
        "prediction": "gpt-5.6-luna",
        "results": [
            {"model": "gpt-5.6-luna", "probability": 0.90},
            {"model": "gpt-5.6-sol", "probability": 0.05},
        ],
    })
    svc._refresh_accounts(t0)

    svc.db.enqueue_account(501, "gpt-5.6-sol", now=t0, available_at=t0, trigger="scheduled", retest_index=0)
    job = svc.db.claim_next_account(now=t0)
    svc._run_account_job(job)

    latest = svc.db.get_account_latest_round(501)
    assert latest is not None
    assert latest["status"] == "suspect"
    # The host never applied the pause, so it must not be shown as paused.
    assert all(pm["paused_until"] is None for pm in svc.account_snapshot(501)["per_model"])


def test_paused_apikey_target_next_run_bounded_to_1800_seconds(tmp_path, fake_clock, monkeypatch):
    """暂停中 API Key 目标下次检测 ≤ 1800 秒。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    host = MockHostClient(accounts_data=[
        {
            "account_id": 601,
            "name": "APIKey-Target",
            "platform": "openai",
            "type": "apikey",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "paused_models": [{"model": "gpt-5.6-sol", "until": utc_iso(t0 + 3600)}],
        },
        {
            "account_id": 602,
            "name": "OAuth-Target",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol"],
            "paused_models": [{"model": "gpt-5.6-sol", "until": utc_iso(t0 + 86400)}],
        },
    ])

    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    acc601 = svc.db.get_account(601)
    # API Key target's next_run_at must be <= t0 + 1800
    assert acc601["next_run_at"] is not None
    assert acc601["next_run_at"] - t0 <= 1800.0

    # In _schedule_due_accounts when scheduled
    fake_clock.value = acc601["next_run_at"]
    svc._schedule_due_accounts(fake_clock.value)

    acc601_after = svc.db.get_account(601)
    assert acc601_after["next_run_at"] is not None
    assert acc601_after["next_run_at"] - fake_clock.value <= 1800.0


def test_snapshot_per_model_paused_until(tmp_path, fake_clock, monkeypatch):
    """快照 per_model.paused_until 显示到期时间（上游取成员中最晚的）。"""
    t0 = 1700000000.0
    fake_clock.value = t0

    until1 = utc_iso(t0 + 1000)
    until2 = utc_iso(t0 + 2000)

    host = MockHostClient(accounts_data=[
        {
            "account_id": 701,
            "name": "Cluster-Member-1",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol", "gpt-6-astra"],
            "cluster_id": "c_snap",
            "cluster_name": "Cluster Snap",
            "paused_models": [{"model": "gpt-5.6-sol", "until": until1}],
        },
        {
            "account_id": 702,
            "name": "Cluster-Member-2",
            "platform": "openai",
            "type": "oauth",
            "schedulable": True,
            "models": ["gpt-5.6-sol", "gpt-6-astra"],
            "cluster_id": "c_snap",
            "cluster_name": "Cluster Snap",
            "paused_models": [{"model": "gpt-5.6-sol", "until": until2}],
        },
    ])

    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    snap = svc.account_snapshot(701)
    per_model = {item["model"]: item for item in snap["per_model"]}

    # sol is paused on both members; takes the latest (until2)
    assert per_model["gpt-5.6-sol"]["paused_until"] == until2

    # astra is not paused
    assert per_model["gpt-6-astra"]["paused_until"] is None

    # Advance clock past until2 -> should become None (expired)
    fake_clock.value = t0 + 2500
    snap_after = svc.account_snapshot(701)
    per_model_after = {item["model"]: item for item in snap_after["per_model"]}
    assert per_model_after["gpt-5.6-sol"]["paused_until"] is None


def test_db_migration_preserves_existing_data(tmp_path):
    """旧数据库升级不丢数据，自动补齐 paused_models_json 列。"""
    db_path = tmp_path / "old_modeltrace.sqlite3"
    # Create DB with previous schema (without paused_models_json column)
    conn = sqlite3.connect(db_path)
    conn.executescript("""
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
            cluster_id TEXT,
            cluster_name TEXT,
            real_models_json TEXT NOT NULL DEFAULT '[]',
            updated_at REAL NOT NULL
        );
        INSERT INTO accounts (account_id, name, platform, type, schedulable, models_json, updated_at)
        VALUES (888, 'Existing-Account', 'openai', 'oauth', 1, '["gpt-5.6-sol"]', 1700000000.0);
    """)
    conn.commit()
    conn.close()

    # Open with ModelTraceDB
    db = ModelTraceDB(db_path)
    row = db.get_account(888)
    assert row is not None
    assert row["name"] == "Existing-Account"
    assert row["platform"] == "openai"
    # Column exists and defaults to '[]'
    assert "paused_models_json" in row.keys()
    assert row["paused_models_json"] == "[]"


def test_manual_reset_resumes_members_and_records_neutral_round(tmp_path, fake_clock, monkeypatch):
    t0 = 1700000000.0
    fake_clock.value = t0
    until = utc_iso(t0 + 3600)
    host = MockHostClient(accounts_data=[
        {"account_id": 801, "name": "K1", "platform": "openai", "type": "apikey", "schedulable": True,
         "models": ["gpt-6-astra"], "cluster_id": "c_reset", "cluster_name": "Reset",
         "paused_models": [{"model": "gpt-6-astra", "until": until}]},
        {"account_id": 802, "name": "K2", "platform": "openai", "type": "apikey", "schedulable": True,
         "models": ["gpt-6-astra"], "cluster_id": "c_reset", "cluster_name": "Reset",
         "paused_models": [{"model": "gpt-6-astra", "until": until}]},
    ])
    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(t0)

    result = svc.reset_account_model(802, model="gpt-6-astra")
    assert result == {"reset": True, "resumed": 2}
    assert sorted(c["account_id"] for c in host.resume_calls) == [801, 802]

    snap = svc.account_snapshot(801)
    astra = snap["per_model"][0]
    assert astra["paused_until"] is None
    assert astra["latest"]["status"] == "reset" and astra["latest"]["message_code"] == "manual_reset"
    # A reset is neutral: it counts in total but in no verdict bucket.
    summary = svc.accounts_summary_for_model("gpt-6-astra")
    assert summary["total"] == 1
    assert summary["match"] == summary["suspect"] == summary["uncertain"] == summary["error"] == 0

    # HTTP contract
    from app import create_app
    monkeypatch.setenv("MODELTRACE_SECRET", "s")
    client = create_app(svc, secret="s", start_worker=False).test_client()
    h = {"Authorization": "Bearer s"}
    assert client.post("/accounts/801/reset", json={"model": "gpt-6-astra"}, headers=h).status_code == 200
    assert client.post("/accounts/801/reset", json={"model": "gpt-5.6-sol"}, headers=h).status_code == 400
    assert client.post("/accounts/999/reset", json={"model": "gpt-6-astra"}, headers=h).status_code == 404
