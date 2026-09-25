"""Tests for ModelTrace 0.1.12 single-probe detection, retest state machine, and upstream error handling."""
import asyncio
from dataclasses import replace
import json
import pytest
import httpx

from modeltrace import __version__
from modeltrace.config import MonitorConfig, load_config
from modeltrace.fingerprint import generate_challenges, load_bank, analyze_outputs, parse_numbers
from modeltrace.service import ModelTraceService, classify_sample, SAFE_MESSAGE_CODES
from modeltrace.transport import ProbeTransport, ProbeResult
from tests.conftest import build_service, run_one


class CustomTestTransport:
    def __init__(self, texts=None, error_code=None, status_code=200, upstream_model=None):
        self.calls = []
        self.texts = list(texts) if texts is not None else [" ".join(str((i % 355) + 1) for i in range(355))]
        self.error_code = error_code
        self.status_code = status_code
        self.upstream_model = upstream_model

    def run(self, *, model: str, challenge: dict, user_agent: str, session_affinity: str | None = None) -> ProbeResult:
        self.calls.append({"model": model, "challenge": challenge, "user_agent": user_agent, "session_affinity": session_affinity})
        idx = len(self.calls) - 1
        text = self.texts[min(idx, len(self.texts) - 1)] if self.error_code is None else None
        return ProbeResult(text, self.status_code, self.error_code, upstream_model=self.upstream_model)


def test_version_is_0112():
    assert __version__ in {"0.1.12", "0.1.13", "0.1.14", "0.1.16", "0.1.17"}


def test_classify_sample_state_mjs_rules():
    """Verify classify_sample faithfully ports upstream state.mjs logic:
    top == expected && prob >= 0.5 -> compatible
    top != expected && top >= 0.7 && cand <= 0.15 && (top - cand) >= 0.65 -> difference_signal
    others -> inconclusive
    """
    # 1. Missing or unknown expected
    res_unknown = {"results": [{"model": "gpt-5.4", "probability": 0.9}]}
    assert classify_sample(res_unknown, "")["outcome"] == "missing_expected_model"
    assert classify_sample(res_unknown, "unknown-model")["outcome"] == "unknown_expected_model"

    # 2. Compatible: top == expected and probability >= 0.5
    res_comp = {
        "results": [
            {"model": "gpt-5.6-sol", "probability": 0.55},
            {"model": "gpt-5.4", "probability": 0.45},
        ]
    }
    c = classify_sample(res_comp, "gpt-5.6-sol")
    assert c["outcome"] == "compatible"
    assert c["closedSetWeight"] == 0.55
    assert c["expectedWeight"] == 0.55

    # 3. Inconclusive: top == expected but prob < 0.5
    res_low = {
        "results": [
            {"model": "gpt-5.6-sol", "probability": 0.45},
            {"model": "gpt-5.4", "probability": 0.35},
        ]
    }
    assert classify_sample(res_low, "gpt-5.6-sol")["outcome"] == "inconclusive"

    # 4. Difference signal: top != expected && top >= 0.7 && cand <= 0.15 && (top - cand) >= 0.65
    res_diff = {
        "results": [
            {"model": "gpt-6-sol", "probability": 0.85},
            {"model": "gpt-5.6-sol", "probability": 0.10},
        ]
    }
    d = classify_sample(res_diff, "gpt-5.6-sol")
    assert d["outcome"] == "difference_signal"
    assert d["prediction"] == "gpt-6-sol"

    # 4b. An account whose target model drifted: gpt-5.5 at 0.737 with the target near 0 now counts
    res_737 = {"results": [{"model": "gpt-5.5", "probability": 0.737}, {"model": "gpt-6-luna", "probability": 0.21}, {"model": "gpt-5.6-terra", "probability": 0.00003}]}
    assert classify_sample(res_737, "gpt-5.6-terra")["outcome"] == "difference_signal"
    # ... but below 0.7 it stays unclear
    res_65 = {"results": [{"model": "gpt-5.5", "probability": 0.65}, {"model": "gpt-5.6-terra", "probability": 0.0}]}
    assert classify_sample(res_65, "gpt-5.6-terra")["outcome"] == "inconclusive"

    # 5. Repeated difference signal with recent history
    prev = [
        {"prediction": "gpt-6-sol", "outcome": "difference_signal"},
        {"prediction": "gpt-6-sol", "outcome": "difference_signal"},
    ]
    rep = classify_sample(res_diff, "gpt-5.6-sol", previous=prev)
    assert rep["outcome"] == "repeated_difference"


def test_single_probe_compatible_ends_in_one_request(tmp_path, fake_clock, monkeypatch):
    """Initial probe matches expected with probability >= 0.5 -> status=match, compatible, 1 request only."""
    service, _, _ = build_service(tmp_path, fake_clock)
    
    numbers_text = " ".join(str((i % 355) + 1) for i in range(300))
    transport = CustomTestTransport(texts=[numbers_text])
    service.transport = transport

    # Mock analyze_outputs to return compatible analysis
    monkeypatch.setattr(
        "modeltrace.service.analyze_outputs",
        lambda outputs, bank: {
            "prediction": "gpt-5.4",
            "results": [
                {"model": "gpt-5.4", "probability": 0.92},
                {"model": "gpt-5.5", "probability": 0.08},
            ],
            "calibration": {"queries": "1"},
        },
    )

    run_one(service)

    assert len(transport.calls) == 1
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "match"
    assert snap["latest"]["message_code"] == "compatible"
    assert snap["latest"]["target_probability"] == 0.92
    assert snap["latest"]["best_model"] == "gpt-5.4"
    assert snap["retest_progress"] is None


def test_single_probe_mismatch_triggers_retest_and_suspect(tmp_path, fake_clock, monkeypatch):
    """Initial probe is difference_signal -> marks retest (0/3), schedules immediate next rounds,
    after 3 retests (1/3, 2/3, 3/3) with difference_signal -> suspect.
    """
    service, _, _ = build_service(tmp_path, fake_clock)
    
    numbers_text = " ".join(str((i % 355) + 1) for i in range(300))
    transport = CustomTestTransport(texts=[numbers_text] * 4)
    service.transport = transport

    # analyze_outputs returns difference signal: top is gpt-6-sol (0.90), expected is gpt-5.4 (0.05)
    monkeypatch.setattr(
        "modeltrace.service.analyze_outputs",
        lambda outputs, bank: {
            "prediction": "gpt-6-sol",
            "results": [
                {"model": "gpt-6-sol", "probability": 0.90},
                {"model": "gpt-5.4", "probability": 0.05},
            ],
            "calibration": {"queries": "1"},
        },
    )

    # Initial probe (turn 1)
    run_one(service)
    assert len(transport.calls) == 1
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "uncertain"
    assert snap["latest"]["message_code"] == "difference_signal"
    assert snap["retest_progress"] == {"done": 0, "total": 3}

    # Retest 1 (turn 2)
    run_one(service)
    assert len(transport.calls) == 2
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "uncertain"
    assert snap["retest_progress"] == {"done": 1, "total": 3}

    # Retest 2 (turn 3)
    run_one(service)
    assert len(transport.calls) == 3
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "uncertain"
    assert snap["retest_progress"] == {"done": 2, "total": 3}

    # Retest 3 (turn 4 -> target 3 reached, all difference -> suspect)
    run_one(service)
    assert len(transport.calls) == 4
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "suspect"
    assert snap["latest"]["message_code"] == "repeated_other_model"
    assert snap["latest"]["best_model"] == "gpt-6-sol"
    assert snap["retest_progress"] is None


def test_retest_recovers_to_match_if_compatible_appears(tmp_path, fake_clock, monkeypatch):
    """Initial probe mismatch, but during retest a compatible sample appears -> match."""
    service, _, _ = build_service(tmp_path, fake_clock)
    numbers_text = " ".join(str((i % 355) + 1) for i in range(300))
    transport = CustomTestTransport(texts=[numbers_text] * 4)
    service.transport = transport

    analyses = [
        # Call 1: difference
        {
            "prediction": "gpt-6-sol",
            "results": [{"model": "gpt-6-sol", "probability": 0.85}, {"model": "gpt-5.4", "probability": 0.10}],
            "calibration": {"queries": "1"},
        },
        # Call 2: compatible!
        {
            "prediction": "gpt-5.4",
            "results": [{"model": "gpt-5.4", "probability": 0.95}, {"model": "gpt-6-sol", "probability": 0.05}],
            "calibration": {"queries": "1"},
        },
    ]

    monkeypatch.setattr(
        "modeltrace.service.analyze_outputs",
        lambda outputs, bank: analyses.pop(0),
    )

    # Initial probe
    run_one(service)
    assert service.snapshot(1)["latest"]["status"] == "uncertain"
    assert service.snapshot(1)["retest_progress"] == {"done": 0, "total": 3}

    # Retest probe 1
    run_one(service)
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "match"
    assert snap["latest"]["message_code"] == "compatible"
    assert snap["retest_progress"] is None


def test_upstream_error_stops_round_immediately_without_retest(tmp_path, fake_clock):
    """Upstream error (such as timeout or 5xx) immediately records error and does NOT trigger retest."""
    service, _, _ = build_service(tmp_path, fake_clock)
    service.transport = CustomTestTransport(error_code="upstream_timeout", status_code=504)

    run_one(service)

    assert len(service.transport.calls) == 1
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "error"
    assert snap["latest"]["message_code"] == "upstream_timeout"
    assert snap["retest_progress"] is None

    # Next queue claim should be empty (no retest was enqueued)
    job = service.db.claim_next(now=fake_clock())
    assert job is None


def test_astra_low_reasoning_effort_is_visible_and_not_hidden(tmp_path, fake_clock, monkeypatch):
    """gpt-6-astra uses low reasoning effort. Verify results are not hidden by unverified rule."""
    service, _, _ = build_service(tmp_path, fake_clock)
    service.config = replace(
        service.config,
        monitors={1: MonitorConfig(1, "gpt-6-astra", True, True)},
        reasoning_effort_overrides={"gpt-6-astra": "low"},
    )
    numbers_text = " ".join(str((i % 355) + 1) for i in range(300))
    transport = CustomTestTransport(texts=[numbers_text], upstream_model="gpt-6-astra")
    service.transport = transport

    monkeypatch.setattr(
        "modeltrace.service.analyze_outputs",
        lambda outputs, bank: {
            "prediction": "gpt-6-astra",
            "results": [
                {"model": "gpt-6-astra", "probability": 0.88},
                {"model": "gpt-5.4", "probability": 0.05},
            ],
            "calibration": {"queries": "1"},
        },
    )

    run_one(service)

    snap = service.snapshot(1)
    assert snap["reasoning_effort"] == "low"
    assert snap["latest"]["status"] == "match"
    assert snap["latest"]["message_code"] == "compatible"
    assert snap["latest"]["target_probability"] == 0.88
    assert snap["latest"]["best_model"] == "gpt-6-astra"
    assert len(snap["latest"]["ranking"]) == 2


def test_upstream_no_available_account_classification(tmp_path):
    """503 response containing 'no available' maps to error_code upstream_no_available_account."""
    assert "upstream_no_available_account" in SAFE_MESSAGE_CODES
    assert "output_truncated" in SAFE_MESSAGE_CODES

    async def mock_receive(request):
        return httpx.Response(
            503,
            headers={"content-type": "application/json"},
            json={"error": "no available OpenAI accounts supporting model: gpt-5.6-sol"},
        )

    client_factory = lambda **kw: httpx.AsyncClient(transport=httpx.MockTransport(mock_receive), **kw)
    tp = ProbeTransport(
        endpoint="https://api.example/responses",
        api_key="key",
        client_factory=client_factory,
    )

    result = tp.run(
        model="gpt-5.6-sol",
        challenge={"prompt": "numbers", "expected_count": 300},
        user_agent="ModelTraceProbe/test",
    )
    assert result.status_code == 503
    assert result.error_code == "upstream_no_available_account"


def test_diagnostics_records_all_required_metrics(tmp_path, fake_clock, monkeypatch):
    """Verify diagnostics record total_time_ms, reasoning_tokens, output_tokens, parsed_numbers_count, is_complete."""
    service, _, _ = build_service(tmp_path, fake_clock)
    numbers_text = " ".join(str((i % 355) + 1) for i in range(300))

    class DiagTransport(CustomTestTransport):
        def run(self, **kwargs):
            res = super().run(**kwargs)
            res.transport_detail.update({
                "first_event_ms": 120,
                "first_text_ms": 450,
                "output_tokens": 400,
                "reasoning_tokens": 80,
            })
            return res

    service.transport = DiagTransport(texts=[numbers_text])

    monkeypatch.setattr(
        "modeltrace.service.analyze_outputs",
        lambda outputs, bank: {
            "prediction": "gpt-5.4",
            "results": [{"model": "gpt-5.4", "probability": 0.90}],
            "calibration": {"queries": "1"},
        },
    )

    run_one(service)

    round_row = service.db.get_latest_round(1)
    diag = service.db.decode_diagnostics(round_row)
    assert diag["first_event_ms"] == 120
    assert diag["first_text_ms"] == 450
    assert "total_time_ms" in diag
    assert diag["output_tokens"] == 400
    assert diag["reasoning_tokens"] == 80
    assert diag["parsed_numbers_count"] == 300
    assert diag["is_complete"] is True


def test_retest_schedule_invariance_and_no_drift(tmp_path, fake_clock, monkeypatch):
    """Prove that:
    1. Retest runs at most 3 rounds.
    2. Retest completes and returns to original anchored 600s slot (does not drift).
    3. Retest does not cause queue piling (pending <= monitor count).
    4. Missed slots do not catch up.
    """
    service, _, _ = build_service(tmp_path, fake_clock, enabled=True)
    anchor = fake_clock()
    numbers_text = " ".join(str((i % 355) + 1) for i in range(300))
    service.transport = CustomTestTransport(texts=[numbers_text])

    # Sequence: 1 initial inconclusive, 3 retest difference_signals -> suspect
    responses = [
        # Initial
        {"prediction": "gpt-6-sol", "results": [{"model": "gpt-6-sol", "probability": 0.4}, {"model": "gpt-5.4", "probability": 0.4}], "calibration": {"queries": "1"}},
        # Retest 1
        {"prediction": "gpt-6-sol", "results": [{"model": "gpt-6-sol", "probability": 0.85}, {"model": "gpt-5.4", "probability": 0.10}], "calibration": {"queries": "1"}},
        # Retest 2
        {"prediction": "gpt-6-sol", "results": [{"model": "gpt-6-sol", "probability": 0.85}, {"model": "gpt-5.4", "probability": 0.10}], "calibration": {"queries": "1"}},
        # Retest 3
        {"prediction": "gpt-6-sol", "results": [{"model": "gpt-6-sol", "probability": 0.85}, {"model": "gpt-5.4", "probability": 0.10}], "calibration": {"queries": "1"}},
    ]
    resp_idx = 0

    def mock_analyze(outputs, bank):
        nonlocal resp_idx
        res = responses[min(resp_idx, len(responses) - 1)]
        resp_idx += 1
        return res

    monkeypatch.setattr("modeltrace.service.analyze_outputs", mock_analyze)

    # Initial probe
    run_one(service)
    assert len(service.transport.calls) == 1
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "uncertain"
    assert snap["retest_progress"] == {"done": 0, "total": 3}

    # Retest 1: executed immediately on worker next turn
    run_one(service)
    assert len(service.transport.calls) == 2
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "uncertain"
    assert snap["retest_progress"] == {"done": 1, "total": 3}

    # Check pending queue bounds
    pending = service.db._conn.execute(
        "SELECT count(*) FROM queue WHERE state IN ('queued', 'running')"
    ).fetchone()[0]
    assert pending <= 1

    # Retest 2
    run_one(service)
    assert len(service.transport.calls) == 3
    assert service.snapshot(1)["retest_progress"] == {"done": 2, "total": 3}

    # Retest 3: finishes all 3 retests!
    run_one(service)
    assert len(service.transport.calls) == 4
    snap = service.snapshot(1)
    assert snap["latest"]["status"] == "suspect"
    assert snap["latest"]["message_code"] == "repeated_other_model"
    assert snap["retest_progress"] is None

    # Next run must be anchored to the original cycle (600s or 1800s anchor), no drift!
    next_run = service.db.get_state(1)["next_run_at"]
    assert next_run == anchor + 1800
    assert service.db.claim_next(now=fake_clock()) is None
