from __future__ import annotations

import uuid

import httpx
import pytest

from modeltrace.transport import ProbeResult
from tests.conftest import FakeReceipts, FakeTransport, build_service, run_one
from tests.test_transport import (
    ChunkStream,
    completed,
    make_transport,
    run_probe,
    sse,
    sse_response,
)


EXPECTED_MODEL = "gpt-5.4"
OTHER_MODEL = "gpt-5.6-luna"


def _run_json(payload: dict) -> ProbeResult:
    return run_probe(make_transport(httpx.Response(200, json=payload)).transport)


def _run_sse(events: list[dict]) -> ProbeResult:
    stream = ChunkStream([sse(event) for event in events])
    result = run_probe(make_transport(sse_response(stream)).transport)
    return result


@pytest.mark.parametrize("wire_format", ["json", "sse"])
@pytest.mark.parametrize("structured_identity", ["correct", "missing"])
def test_identity_is_structural_and_body_model_names_are_not_identity(
    wire_format: str, structured_identity: str
):
    """A model-looking answer body must not manufacture or override identity."""
    body = f"The answer mentions model={OTHER_MODEL}; numbers: 1 2 3"
    if wire_format == "json":
        payload = completed(body)
        if structured_identity == "correct":
            payload["model"] = EXPECTED_MODEL
        result = _run_json(payload)
    else:
        response = completed(body)
        if structured_identity == "correct":
            response["model"] = EXPECTED_MODEL
        result = _run_sse(
            [
                {"type": "response.created", "response": ({"model": EXPECTED_MODEL} if structured_identity == "correct" else {})},
                {"type": "response.output_text.delta", "delta": body},
                {"type": "response.completed", "response": response},
            ]
        )

    assert result.error_code is None
    assert result.text == body
    assert result.upstream_model == (EXPECTED_MODEL if structured_identity == "correct" else None)


def test_json_root_and_nested_model_disagreement_fails_closed_even_with_complete_output():
    payload = {
        "model": EXPECTED_MODEL,
        "response": {"model": OTHER_MODEL},
        "status": "completed",
        "output_text": "1 2 3",
    }
    result = _run_json(payload)
    assert result.error_code == "upstream_model_mismatch"
    assert result.text is None
    assert result.upstream_model == EXPECTED_MODEL


@pytest.mark.parametrize("conflict_at", ["created", "terminal"])
def test_sse_created_or_terminal_identity_conflict_fails_closed(conflict_at: str):
    created_model = OTHER_MODEL if conflict_at == "created" else EXPECTED_MODEL
    terminal_model = OTHER_MODEL if conflict_at == "terminal" else EXPECTED_MODEL
    stream = ChunkStream(
        [
            sse({"type": "response.created", "response": {"model": created_model}}),
            sse({"type": "response.output_text.delta", "delta": "1 2 3"}),
            sse({"type": "response.completed", "response": {**completed("1 2 3"), "model": terminal_model}}),
        ]
    )
    result = run_probe(make_transport(sse_response(stream)).transport)
    assert result.error_code == "upstream_model_mismatch"
    assert result.text is None
    assert stream.read_count == (1 if conflict_at == "created" else 3)
    assert stream.closed


def _identity_sequence_transport(mismatch_at: int, *, metadata_only: bool = False):
    class IdentitySequenceTransport(FakeTransport):
        def run(self, **kwargs):
            self.calls.append(kwargs)
            ordinal = len(self.calls) - 1
            if ordinal == mismatch_at:
                return ProbeResult(
                    self.text,
                    200,
                    None if metadata_only else "upstream_model_mismatch",
                    upstream_model=OTHER_MODEL,
                )
            return ProbeResult(self.text, 200, None, upstream_model=EXPECTED_MODEL)

    return IdentitySequenceTransport()


def _probe_statuses(service):
    return [
        row[0]
        for row in service.db._conn.execute(
            "SELECT 'attempted' FROM detection_attempts ORDER BY ordinal"
        ).fetchall()
    ]


def test_service_mismatch_stops_round_without_reaching_classifier(
    tmp_path, fake_clock, monkeypatch
):
    import modeltrace.service as service_module

    transport = _identity_sequence_transport(0)
    service, _, receipts = build_service(tmp_path, fake_clock, transport=transport)
    monkeypatch.setattr(
        service_module,
        "analyze_outputs",
        lambda *_: pytest.fail("identity mismatch must never reach the classifier"),
    )

    run_one(service)

    latest = service.snapshot(1)["latest"]
    diagnostics = service.db.decode_diagnostics(service.db.get_latest_round(1))
    assert len(transport.calls) == 1
    assert len(receipts.calls) == 0
    assert latest["status"] == "error"
    assert latest["message_code"] == "upstream_model_mismatch"
    assert latest["target_probability"] is None
    assert latest["best_model"] is None
    assert latest["ranking"] == []
    assert diagnostics["request_count"] == 1
    assert _probe_statuses(service) == ["attempted"]
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0
    assert service.snapshot(1)["retest_progress"] is None


def test_service_mismatch_with_missing_receipt_does_not_read_receipts_and_stops_tail(
    tmp_path, fake_clock, monkeypatch
):
    import modeltrace.service as service_module

    transport = _identity_sequence_transport(0)
    service, _, receipts = build_service(
        tmp_path,
        fake_clock,
        transport=transport,
        receipts=FakeReceipts(missing=True),
    )
    monkeypatch.setattr(
        service_module,
        "analyze_outputs",
        lambda *_: pytest.fail("identity mismatch must never reach the classifier"),
    )

    run_one(service)

    diagnostics = service.db.decode_diagnostics(service.db.get_latest_round(1))
    assert len(transport.calls) == 1
    assert len(receipts.calls) == 0
    assert service.snapshot(1)["latest"]["message_code"] == "upstream_model_mismatch"
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0
    assert _probe_statuses(service) == ["attempted"]


def test_service_rejects_mismatched_probe_metadata_even_if_adapter_forgets_error_code(
    tmp_path, fake_clock, monkeypatch
):
    import modeltrace.service as service_module

    transport = _identity_sequence_transport(0, metadata_only=True)
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport)
    monkeypatch.setattr(
        service_module,
        "analyze_outputs",
        lambda *_: pytest.fail("mismatched structured metadata must never reach the classifier"),
    )

    run_one(service)

    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "error"
    assert latest["message_code"] == "upstream_model_mismatch"
    assert len(transport.calls) == 1
    assert _probe_statuses(service) == ["attempted"]


def test_db_records_successful_tail_cancellation_only_after_queue_is_terminal(
    tmp_path, fake_clock
):
    service, _, _ = build_service(tmp_path, fake_clock)
    _, queue_id = service.db.enqueue(1, now=fake_clock(), trigger="identity-test")
    job = service.db.claim_next(now=fake_clock())
    assert job is not None
    agents = [f"ModelTraceProbe/{uuid.uuid4()}" for _ in range(3)]
    reservation = service.db.reserve_budget(
        1,
        queue_id,
        0.1,
        daily_budget_usd=5,
        now=fake_clock(),
        probe_model=EXPECTED_MODEL,
        probe_user_agents=agents,
    )
    assert reservation is not None
    assert service.db.mark_probe_attempted(reservation.reservation_id, 0, now=fake_clock())
    assert not service.db.cancel_probe(reservation.reservation_id, 1, now=fake_clock())

    service.db.record_round_and_finish(
        job,
        status="error",
        target_probability=None,
        best_model=None,
        checked_at=fake_clock(),
        message_code="upstream_model_mismatch",
        ranking=[],
        diagnostics={"request_count": 1},
        next_run_at=None,
        next_run_after=None,
        upstream_statuses=[200],
        receipt_count=1,
        receipt_consistent=False,
        actual_cost_usd=None,
        reserved_usd=reservation.amount_usd,
    )

    assert service.db.cancel_probe(reservation.reservation_id, 1, now=fake_clock())
    assert service.db.cancel_probe(reservation.reservation_id, 2, now=fake_clock())
    assert not service.db.cancel_probe(reservation.reservation_id, 1, now=fake_clock())
    assert [r[0] for r in service.db._conn.execute("SELECT status FROM reconciliation_probes ORDER BY ordinal")] == ["attempted", "cancelled", "cancelled"]
