"""Audit tests: verified partial receipts + explicit missing auto-estimate conservatively upon reaching retry limit,
while invalid/mismatched/query-failing receipts remain strictly held.
"""
from dataclasses import replace
import json
import logging
import pytest
from modeltrace.config import Pricing
from modeltrace.receipts import Receipt, ReceiptResult
from tests.conftest import build_service
from tests.test_reconciliation_service import (
    MODEL, RecordingTransport, BatchReceiptStub, receipt, run_one,
)

pytestmark = pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")


@pytest.mark.parametrize("terminal_error", ["upstream_5xx", "upstream_network_error"])
def test_audit_partial_receipts_auto_estimate_at_retry_limit(tmp_path, fake_clock, terminal_error):
    """When a terminal round has verified valid receipts for some probes and explicit receipt_missing
    for the remainder, it stays held while checks < reconciliation_max_checks, then safely auto-estimates
    at max(reserve, known_cost_floor), unfreezing the monitor without assertion of confirmed actual spend.
    Late actual receipts can still be idempotently trued up via existing admin_true_up_conservative.
    """
    transport = RecordingTransport()
    reader = BatchReceiptStub()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=reader)
    service.config = replace(
        service.config,
        budget_basis="lei_group4_v1",
        pricing_upper_bound={MODEL: Pricing(2.5, 1.25, 15)},
        billing_prices={MODEL: Pricing(2.5, 1.25, 15)},
        reconciliation_max_checks=2,
    )
    run_one(service)
    # Match terminal queue outcome without network call
    service.db._conn.execute(
        "UPDATE queue SET state='error', error_code=? WHERE id=(SELECT MAX(id) FROM queue)", (terminal_error,)
    )
    service.db._conn.commit()

    # Capture initial reservation amount and budget day before reconciliation
    initial_res = service.db._conn.execute(
        "SELECT id, budget_day, amount_usd FROM budget_reservations ORDER BY id DESC LIMIT 1"
    ).fetchone()
    rid = initial_res["id"]
    budget_day = initial_res["budget_day"]
    orig_reserve_amount = float(initial_res["amount_usd"])

    # Initial budget day ledger
    ledger_before = service.db._conn.execute(
        "SELECT spent_usd, reserved_usd FROM budget_days WHERE budget_day = ?",
        (budget_day,),
    ).fetchone()
    assert ledger_before["reserved_usd"] >= orig_reserve_amount

    # Probe 0 is missing, Probe 1 and 2 have verified valid receipts
    def mixed_batch(agents):
        return {
            ua: ReceiptResult([], "receipt_missing") if i == 0 else ReceiptResult([receipt(cost=0.05)], None)
            for i, ua in enumerate(agents)
        }

    # Check 1: checks < reconciliation_max_checks (2) -> remains held
    reader.queue_batch(mixed_batch)
    fake_clock.advance(300)
    res1 = service.reconcile_pending()
    assert res1 == {"checked": 1, "settled": 0, "estimated": 0, "held": 1}
    assert service.db.has_unsettled_monitor(1) is True

    # Check 2: reaches reconciliation_max_checks (2) -> eligible for conservative auto-estimate
    reader.queue_batch(mixed_batch)
    fake_clock.advance(300)
    res2 = service.reconcile_pending()
    assert res2 == {"checked": 1, "settled": 0, "estimated": 1, "held": 0}

    # Verify reconciliation_rounds row state in DB: state becomes 'settled' upon conservative resolution
    round_row = service.db._conn.execute(
        "SELECT checks, state FROM reconciliation_rounds WHERE reservation_id = ?",
        (rid,),
    ).fetchone()
    assert round_row["checks"] == 1
    assert round_row["state"] == "settled"
    assert service.db.has_unsettled_monitor(1) is False  # Monitor is unblocked

    res_row = service.db._conn.execute(
        "SELECT state, settlement_basis, settled_amount_usd, amount_usd FROM budget_reservations WHERE id = ?",
        (rid,),
    ).fetchone()
    assert res_row["state"] == "settled"
    assert res_row["settlement_basis"] == "conservative_estimate"
    assert res_row["settled_amount_usd"] is None  # Never asserted as confirmed actual

    # Verify conservative resolution in DB: estimate = max(orig_reserve, floor)
    resolution = service.db._conn.execute(
        "SELECT * FROM reservation_conservative_resolutions WHERE reservation_id = ?",
        (rid,),
    ).fetchone()
    assert resolution is not None
    observed_floor = float(resolution["observed_floor_usd"])
    conservative_amount = float(resolution["conservative_amount_usd"])
    assert observed_floor > 0
    assert conservative_amount == pytest.approx(max(orig_reserve_amount, observed_floor))

    # Verify budget ledger updated on original budget day: reserved released, spent debited by conservative_amount
    ledger_after = service.db._conn.execute(
        "SELECT spent_usd, reserved_usd FROM budget_days WHERE budget_day = ?",
        (budget_day,),
    ).fetchone()
    assert ledger_after["reserved_usd"] == pytest.approx(ledger_before["reserved_usd"] - orig_reserve_amount)
    assert ledger_after["spent_usd"] == pytest.approx(ledger_before["spent_usd"] + conservative_amount)

    # Verify evidence stored in resolution audit
    evidence = json.loads(resolution["evidence_json"])
    assert evidence["partial_receipts_conservative"] is True
    assert evidence["verified_receipt_count"] == 2
    assert evidence["missing_receipt_count"] == 1
    assert evidence["observed_cost_usd"] > 0
    assert evidence["known_cost_floor_usd"] == pytest.approx(observed_floor, rel=1e-6) or evidence["known_cost_floor_usd"] >= observed_floor
    assert evidence["actual_receipt_verified"] is False
    assert evidence["model_requests_after_terminal_failure"] == 0

    # Ensure no inference calls were ever replayed
    assert len(transport.calls) == 3

    # Verify late arrival of real receipt can be trued up via existing admin_true_up_conservative
    actual_cost = 0.50
    assert actual_cost >= observed_floor
    true_up = service.db.admin_true_up_conservative(
        rid,
        actual_cost_usd=actual_cost,
        now=fake_clock(),
        actor="admin",
        reason="verified_late_actual_receipt",
        evidence={"receipt_ref": "late-receipt-1"},
    )
    assert true_up["verified_actual_usd"] == pytest.approx(actual_cost)
    assert true_up["settlement_basis"] == "verified_actual"

    # Verify duplicate trueup attempt is prevented once transitioned to verified_actual
    with pytest.raises(ValueError, match="reservation_not_conservative_estimate"):
        service.db.admin_true_up_conservative(
            rid,
            actual_cost_usd=actual_cost,
            now=fake_clock(),
            actor="admin",
            reason="verified_late_actual_receipt",
            evidence={"receipt_ref": "late-receipt-1"},
        )


@pytest.mark.parametrize(
    "corrupt_kind,bad_result_fn,expected_error",
    [
        (
            "receipt_identity_mismatch",
            lambda ua: ReceiptResult([receipt(model="wrong-model")], None),
            "receipt_identity_mismatch",
        ),
        (
            "receipt_cardinality_multiple",
            lambda ua: ReceiptResult([receipt(cost=0.02), receipt(cost=0.03)], None),
            "receipt_cardinality",
        ),
        (
            "receipt_cost_unknown",
            lambda ua: ReceiptResult(
                [
                    Receipt(
                        account_id="acct-1",
                        model=MODEL,
                        input_tokens=None,
                        output_tokens=1,
                        cache_read_tokens=0,
                        total_cost=0.01,
                        account_stats_cost=None,
                        service_tier="default",
                        created_at=None,
                    )
                ],
                None,
            ),
            "receipt_cost_unknown",
        ),
        (
            "receipt_query_failed",
            lambda ua: ReceiptResult([], "receipt_query_failed"),
            "receipt_query_failed",
        ),
    ],
)
def test_bad_receipts_strictly_held_beyond_retry_limit(
    tmp_path, fake_clock, corrupt_kind, bad_result_fn, expected_error
):
    """Any non-missing error on any attempted probe (mismatch, cardinality, unknown cost, query failure)
    must strictly prevent auto-estimate, keeping the monitor held indefinitely even beyond retry limit.
    """
    transport = RecordingTransport()
    reader = BatchReceiptStub()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=reader)
    service.config = replace(
        service.config,
        budget_basis="lei_group4_v1",
        pricing_upper_bound={MODEL: Pricing(2.5, 1.25, 15)},
        billing_prices={MODEL: Pricing(2.5, 1.25, 15)},
        reconciliation_max_checks=2,
    )
    run_one(service)
    service.db._conn.execute(
        "UPDATE queue SET state='error', error_code=? WHERE id=(SELECT MAX(id) FROM queue)", ("upstream_network_error",)
    )
    service.db._conn.commit()

    # Probe 0 is missing, Probe 1 has a bad receipt, Probe 2 has a valid receipt
    def corrupt_batch(agents):
        out = {}
        for i, ua in enumerate(agents):
            if i == 0:
                out[ua] = ReceiptResult([], "receipt_missing")
            elif i == 1:
                out[ua] = bad_result_fn(ua)
            else:
                out[ua] = ReceiptResult([receipt(cost=0.05)], None)
        return out

    # Run for 4 checks (exceeding max_checks=2)
    for _ in range(4):
        reader.queue_batch(corrupt_batch)
        fake_clock.advance(300)
        res = service.reconcile_pending()
        assert res == {"checked": 1, "settled": 0, "estimated": 0, "held": 1}

    # Verify it remains strictly held and monitor is unsettled
    row = service.db._conn.execute(
        "SELECT checks, state, error_code FROM reconciliation_rounds ORDER BY reservation_id DESC LIMIT 1"
    ).fetchone()
    assert row["checks"] == 4 > service.config.reconciliation_max_checks
    assert row["state"] == "pending"
    assert row["error_code"] == expected_error
    assert service.db.has_unsettled_monitor(1) is True
    assert service.db._conn.execute("SELECT count(*) FROM reservation_conservative_resolutions").fetchone()[0] == 0
    assert len(transport.calls) == 3


def test_safety_logging_sanitized_worker_loop(tmp_path, fake_clock, caplog):
    """Directly execute _worker_loop with real service code when reconcile_pending raises,
    verifying caplog.records contains only sanitized fields (reason, exception_type) without
    leaking probe text, token, or secret.
    """
    transport = RecordingTransport()
    reader = BatchReceiptStub()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=reader)

    # Force reconcile_pending to raise an exception containing a sensitive string
    def broken_reconcile():
        raise RuntimeError("secret_key_or_probe_payload_leak")

    service.reconcile_pending = broken_reconcile
    service._next_reconciliation_at = fake_clock() - 1

    # Mock _schedule_due to immediately set _stop so _worker_loop exits cleanly after one iteration
    def stop_on_schedule(now):
        service._stop.set()

    service._schedule_due = stop_on_schedule

    with caplog.at_level(logging.WARNING, logger="modeltrace.service"):
        service._worker_loop()

    # Find the log record emitted by _worker_loop
    matching_records = [
        r for r in caplog.records
        if r.name == "modeltrace.service" and r.getMessage() == "reconcile_pending_failed"
    ]
    assert len(matching_records) == 1
    record = matching_records[0]

    # Verify record attributes
    assert getattr(record, "reason", None) == "reconcile_pending_failed"
    assert getattr(record, "exception_type", None) == "RuntimeError"

    # Verify no raw exception repr or secret leaked into formatted log or message
    assert "secret_key_or_probe_payload_leak" not in record.getMessage()
    assert "secret_key_or_probe_payload_leak" not in caplog.text
