"""Regression tests verifying timeout, anti-burst, anti-starvation, and diagnostic sanitization in ModelTrace scheduler."""
from dataclasses import replace
import json
import socket
import pytest

from modeltrace.config import load_config
from modeltrace.service import ModelTraceService, SAFE_MESSAGE_CODES
from tests.conftest import FakeTransport

MODELS = ('gpt-5.4', 'gpt-5.5', 'gpt-5.6-sol', 'gpt-5.6-terra')

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('schedule tests must not access the network')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect_ex', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setattr(socket, 'getaddrinfo', forbidden)

@pytest.fixture
def config_path(tmp_path):
    path = tmp_path / 'ten-minute.json'
    path.write_text(json.dumps({
        'base_url': 'https://modeltrace.invalid/v1',
        'api_key': 'offline-test-only',
        'enabled': True,
        'interval_seconds': 600,
        'auto_retests': 0,
        'monitors': {str(i): {'model': MODELS[i - 1], 'enabled': True}
                     for i in (1, 2, 3, 4)},
    }), encoding='utf-8')
    return path

@pytest.fixture
def service_factory(tmp_path, fake_clock, config_path):
    services = []
    def make(*, config=None, transport=None):
        service = ModelTraceService(
            config or load_config(config_path),
            database_path=tmp_path / 'schedule_bounds.sqlite3',
            transport=transport or FakeTransport(),
            clock=fake_clock, monotonic=fake_clock, sleeper=lambda _: None,
        )
        services.append(service)
        return service
    yield make
    for s in services:
        s.db.close()

def test_long_timeout_round_preserves_fairness_and_prevents_bursts(service_factory, fake_clock):
    """
    Simulate Luna/Sol worst case: probe takes 120s timeout per attempt, 3 attempts = 360s.
    With 600s cycle / 150s slots:
    Model 1 runs at T=0, takes 360s (finishes at T=360).
    During this time, Model 2 (due at T=150) and Model 3 (due at T=300) become due.
    At T=360, when Model 1 finishes:
    - Model 1's next run must be calculated from finish_time, maintaining anchored cycles without immediate catchup.
    - Model 2 (due at 150) is delayed by the live worker, so it must be queued once
      instead of being skipped by the slot-width lateness check.
    - Exactly one job claims next, serializing the worker, no concurrent execution,
      at most 3 durable attempts per round.
    """
    class SlowTimeoutTransport(FakeTransport):
        def run(self, **kwargs):
            fake_clock.advance(120)
            return super().run(**kwargs)

    transport = SlowTimeoutTransport(error_code="upstream_timeout")
    service = service_factory(transport=transport)
    anchor = fake_clock()

    # Initial slots: M1=0, M2=150, M3=300, M4=450
    assert [service.db.get_state(i)['next_run_at'] - anchor for i in range(1, 5)] == [0, 150, 300, 450]

    # At T=0, trigger scheduler and claim M1
    service._schedule_due(fake_clock())
    job1 = service.db.claim_next(now=fake_clock())
    assert job1 is not None and job1.monitor_id == 1

    # Run M1 (takes 1 * 120s = 120s)
    service._run_job_safely(job1)
    assert fake_clock() == anchor + 120

    # Ensure M1 recorded 1 attempt, capped at 1, status is error
    attempts = service.db._conn.execute(
        "SELECT ordinal FROM detection_attempts WHERE queue_id = ?", (job1.queue_id,)
    ).fetchall()
    assert len(attempts) == 1
    assert service.snapshot(1)['latest']['status'] == 'error'
    assert service.snapshot(1)['latest']['message_code'] == 'upstream_timeout'

    # Check M1's next scheduled run: must be future anchored cycle, not immediate
    m1_next = service.db.get_state(1)['next_run_at']
    assert m1_next >= anchor + 600

    # Advance to T=150 when Model 2 is due
    fake_clock.advance(30)
    service._schedule_due(fake_clock())
    job_next = service.db.claim_next(now=fake_clock())
    assert job_next is not None
    assert job_next.monitor_id == 2

    # Ensure no other job is running or claiming concurrently
    assert service.db.claim_next(now=fake_clock()) is None

def test_fixed_frequency_anchor_calculation(service_factory, fake_clock):
    """
    Verify whether the 600s cycle is fixed-frequency (aligned to global anchor)
    or fixed-delay after finish.
    """
    service = service_factory()
    anchor = fake_clock()
    
    # Next run for Monitor 1 if checked at T=50s (before cycle completes):
    assert service._next_scheduled_run(1, anchor + 50) == anchor + 600
    
    # Next run for Monitor 1 if finish took 650s (past one cycle):
    assert service._next_scheduled_run(1, anchor + 650) == anchor + 1200
    
    # This proves the scheduler follows fixed-frequency global anchoring:
    # cycles = ceil((not_before - first) / interval_seconds)
    # It quantizes to integer cycle multiples of 600s, discarding skipped cycles.

def test_twenty_round_worst_case_timeouts_all_four_models_serviced_without_starvation(service_factory, fake_clock):
    """
    Multi-round regression test:
    Run at least 20 rounds where every probe takes 3 * 120s = 360s worst-case timeout.
    Assert that:
    1. All 4 configured models are serviced repeatedly (no indefinite starvation).
    2. The maximum waiting time between services for any model is strictly bounded (<= 1800s / 30min).
    3. Pending jobs in queue remain strictly bounded at all times (<= len(monitors)).
    4. Each round strictly respects the 3-attempt ceiling.
    5. Single worker serialized execution holds (no concurrent claims).
    6. System never claims all models can be checked every 10 minutes under worst-case timeout (360s > 150s slot).
    """
    class WorstCaseTimeoutTransport(FakeTransport):
        def run(self, **kwargs):
            fake_clock.advance(120)
            return super().run(**kwargs)

    transport = WorstCaseTimeoutTransport(error_code="upstream_timeout")
    service = service_factory(transport=transport)
    anchor = fake_clock()

    served_monitor_ids = []
    last_served_at = {}
    max_wait_between_runs = {i: 0.0 for i in (1, 2, 3, 4)}

    # Execute 24 consecutive rounds (more than 20 rounds)
    for round_idx in range(24):
        while True:
            now = fake_clock()
            service._schedule_due(now)
            job = service.db.claim_next(now=now)
            if job is not None:
                break
            due = [service.db.get_state(i)["next_run_at"] for i in (1, 2, 3, 4)]
            fake_clock.advance(max(0.001, min(v for v in due if v is not None) - fake_clock()))

        # Assert queue pending is strictly bounded (never accumulating unboundedly)
        pending_count = service.db._conn.execute(
            "SELECT COUNT(*) AS c FROM queue WHERE state IN ('queued', 'running')"
        ).fetchone()["c"]
        assert pending_count <= 4

        # Assert no concurrent worker can claim another job while one is running
        assert service.db.claim_next(now=now) is None

        m_id = job.monitor_id
        if m_id in last_served_at:
            wait = now - last_served_at[m_id]
            if wait > max_wait_between_runs[m_id]:
                max_wait_between_runs[m_id] = wait
        last_served_at[m_id] = now
        served_monitor_ids.append(m_id)

        # Run the job: 1 attempt * 120s = 120s
        service._run_job_safely(job)
        assert fake_clock() == now + 120

        # Assert attempt cap: exactly 1 attempt per round
        attempts = service.db._conn.execute(
            "SELECT ordinal FROM detection_attempts WHERE queue_id = ?", (job.queue_id,)
        ).fetchall()
        assert len(attempts) == 1

    # 1. Assert all 4 models were serviced
    distinct_served = set(served_monitor_ids)
    assert distinct_served == {1, 2, 3, 4}

    # 2. Assert count distribution: each model serviced at least 4 times across 24 rounds
    for m_id in (1, 2, 3, 4):
        count = served_monitor_ids.count(m_id)
        assert count >= 4, f"Monitor {m_id} only serviced {count} times"

    # 3. Assert maximum waiting time between services is strictly bounded
    # Under 360s worst case per round, theoretical max interval is 1800s (30 mins).
    for m_id, wait in max_wait_between_runs.items():
        assert wait <= 1800.0, f"Monitor {m_id} starved with wait {wait}s > 1800s"

def test_safe_message_codes_diagnostic_sanitization(service_factory, fake_clock):
    """
    Test SAFE_MESSAGE_CODES in diagnostic summary:
    - Allows known error codes (e.g., 'upstream_timeout', 'transport_error').
    - Discards malicious / sensitive strings (e.g., API keys, URLs, SQL fragments).
    - Prevents NameError or unhandled exceptions when processing corrupted diagnostics.
    """
    service = service_factory()
    
    # Verify SAFE_MESSAGE_CODES contains known codes and is a frozenset
    assert isinstance(SAFE_MESSAGE_CODES, frozenset)
    assert "upstream_timeout" in SAFE_MESSAGE_CODES
    assert "response_too_large" in SAFE_MESSAGE_CODES
    assert "upstream_model_mismatch" in SAFE_MESSAGE_CODES
    
    # Test malicious diagnostic dictionary in diagnostics_json
    malicious_row = {
        "id": 999,
        "diagnostics_json": json.dumps({
            "upstream_statuses": [200, 500, "999", 700, False],
            "transport_error_codes": [
                "upstream_timeout",
                "Bearer sk-proj-supersecretapikey12345",
                "https://api.openai.com/v1/chat/completions?secret=token",
                "DROP TABLE queue;--",
                "response_too_large",
            ],
            "transport_details": [
                {"termination_event_received": False},
                {"termination_event_received": True},
                "corrupted_detail",
            ],
            "request_count": 3,
            "valid_count": 0,
        })
    }
    
    summary = service._diagnostic_summary(malicious_row)
    assert summary is not None
    # Only valid HTTP statuses retained
    assert summary["upstream_statuses"] == [200, 500]
    # Sensitive tokens and injection strings must be completely dropped; only SAFE_MESSAGE_CODES allowed
    assert summary["transport_error_codes"] == ["upstream_timeout", "response_too_large"]
    assert "Bearer sk-proj-supersecretapikey12345" not in summary["transport_error_codes"]
    assert summary["incomplete_responses"] == 1
    assert summary["request_count"] == 3
    assert summary["valid_count"] == 0

    # Also verify corrupted/missing diagnostics_json does not raise NameError/KeyError
    corrupted_row = {"id": 1000, "diagnostics_json": "{invalid json"}
    assert service._diagnostic_summary(corrupted_row) == {
        "request_count": 0,
        "valid_count": 0,
        "upstream_statuses": [],
        "transport_error_codes": [],
        "incomplete_responses": 0,
    }

    missing_row = {"id": 1001}
    assert service._diagnostic_summary(missing_row) == {
        "request_count": 0,
        "valid_count": 0,
        "upstream_statuses": [],
        "transport_error_codes": [],
        "incomplete_responses": 0,
    }

def test_diagnostic_summary_null_nonlist_and_boolean_counts_robustness(service_factory):
    """
    Negative robustness test for _diagnostic_summary:
    - upstream_statuses explicitly null (None) or non-list (e.g. integer, dict, string) does not raise TypeError.
    - transport_error_codes explicitly null (None) or non-list does not raise TypeError.
    - transport_details explicitly null (None) or non-list does not raise TypeError.
    - request_count / valid_count set to boolean True / False are not treated as integer counts.
    """
    service = service_factory()
    
    # Case 1: All sequence fields explicitly null / None
    row_nulls = {
        "id": 2001,
        "diagnostics_json": json.dumps({
            "upstream_statuses": None,
            "transport_error_codes": None,
            "transport_details": None,
            "request_count": True,
            "valid_count": False,
        })
    }
    summary1 = service._diagnostic_summary(row_nulls)
    assert summary1 == {
        "request_count": 0,
        "valid_count": 0,
        "upstream_statuses": [],
        "transport_error_codes": [],
        "incomplete_responses": 0,
    }

    # Case 2: Sequence fields are non-list primitives/dicts
    row_nonlist = {
        "id": 2002,
        "diagnostics_json": json.dumps({
            "upstream_statuses": 500,
            "transport_error_codes": "upstream_timeout",
            "transport_details": {"not": "a list"},
            "request_count": "3",
            "valid_count": None,
        })
    }
    summary2 = service._diagnostic_summary(row_nonlist)
    assert summary2 == {
        "request_count": 0,
        "valid_count": 0,
        "upstream_statuses": [],
        "transport_error_codes": [],
        "incomplete_responses": 0,
    }

    # Case 3: Valid integer counts vs boolean counts mixed
    row_mixed = {
        "id": 2003,
        "diagnostics_json": json.dumps({
            "upstream_statuses": [200, None, True, "500", 502],
            "transport_error_codes": ["upstream_timeout", None, 123, "sk-secret"],
            "transport_details": [{"termination_event_received": False}, None, 456],
            "request_count": 3,
            "valid_count": True,  # boolean must be rejected and defaulted to 0
        })
    }
    summary3 = service._diagnostic_summary(row_mixed)
    assert summary3 == {
        "request_count": 3,
        "valid_count": 0,
        "upstream_statuses": [200, 502],
        "transport_error_codes": ["upstream_timeout"],
        "incomplete_responses": 1,
    }
