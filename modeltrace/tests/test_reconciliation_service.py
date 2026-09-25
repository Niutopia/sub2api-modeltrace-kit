from __future__ import annotations

import math
from dataclasses import replace
from collections.abc import Callable

import pytest

from modeltrace.config import Pricing
from modeltrace.receipts import Receipt, ReceiptResult
from modeltrace.transport import ProbeResult
from tests.conftest import build_service

pytestmark = pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")


MODEL = "gpt-5.4"


class RecordingTransport:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.text = " ".join(str((i % 355) + 1) for i in range(355))

    def run(self, *, model: str, challenge: dict, user_agent: str, session_affinity: str | None = None) -> ProbeResult:
        self.calls.append({"model": model, "challenge": challenge, "user_agent": user_agent})
        return ProbeResult(self.text, 200, None)


class BatchReceiptStub:
    """Own batch-only stub: existing conftest FakeReceipts intentionally has no read_many."""

    def __init__(self) -> None:
        self.read_calls: list[str] = []
        self.batch_calls: list[list[str]] = []
        self._batches: list[Callable[[list[str]], dict[str, ReceiptResult]]] = []

    def read_nonbillable_many(self, user_agents):
        return {}

    def queue_batch(self, batch: Callable[[list[str]], dict[str, ReceiptResult]]) -> None:
        self._batches.append(batch)

    def read(self, user_agent: str) -> ReceiptResult:
        self.read_calls.append(user_agent)
        return ReceiptResult([], "receipt_missing")

    def read_many(self, user_agents: list[str]) -> dict[str, ReceiptResult]:
        self.batch_calls.append(list(user_agents))
        if self._batches:
            return self._batches.pop(0)(list(user_agents))
        return {user_agent: ReceiptResult([], "receipt_missing") for user_agent in user_agents}


def receipt(*, cost: object = 0.01, model: str = MODEL, tier: str = "default") -> Receipt:
    return Receipt(
        account_id="acct-1",
        model=model,
        input_tokens=1,
        output_tokens=1,
        cache_read_tokens=0,
        total_cost=0.01,
        account_stats_cost=cost,
        service_tier=tier,
        created_at=None,
    )


def valid_batch(user_agents: list[str], costs: list[object] | None = None) -> dict[str, ReceiptResult]:
    values = costs or [0.01] * len(user_agents)
    return {
        user_agent: ReceiptResult([receipt(cost=values[index])], None)
        for index, user_agent in enumerate(user_agents)
    }


def missing_batch(user_agents: list[str]) -> dict[str, ReceiptResult]:
    return {user_agent: ReceiptResult([], "receipt_missing") for user_agent in user_agents}


def run_one(service, monitor_id: int = 1):
    queued, queue_id = service.db.enqueue(monitor_id, now=service.clock(), trigger="reconciliation-test")
    assert queued and queue_id is not None
    job = service.db.claim_next(now=service.clock())
    assert job is not None
    service._run_job_safely(job)
    return job


def recon_row(service):
    return service.db._conn.execute(
        "SELECT * FROM reconciliation_rounds ORDER BY reservation_id DESC LIMIT 1"
    ).fetchone()


def test_delayed_receipts_release_once_after_grace_and_never_call_transport_again(
    tmp_path, fake_clock
):
    transport = RecordingTransport()
    receipts = BatchReceiptStub()
    service, _, _ = build_service(
        tmp_path, fake_clock, transport=transport, receipts=receipts
    )
    run_one(service)

    assert len(transport.calls) == 3
    user_agents = [call["user_agent"] for call in transport.calls]
    receipts.queue_batch(valid_batch)

    fake_clock.advance(299)
    assert service.reconcile_pending() == {"checked": 0, "settled": 0, "estimated": 0, "held": 0}
    assert receipts.batch_calls == []

    fake_clock.advance(1)
    assert service.reconcile_pending() == {"checked": 1, "settled": 1, "estimated": 0, "held": 0}
    assert receipts.batch_calls == [user_agents]
    assert len(transport.calls) == 3, "reconciliation is receipt-only and must not replay probes"

    # A settled reconciliation is no longer pending and cannot debit again.
    assert service.reconcile_pending() == {"checked": 0, "settled": 0, "estimated": 0, "held": 0}
    assert len(receipts.batch_calls) == 1
    budget = service.db.budget_snapshot(now=fake_clock())
    assert budget.spent_usd == pytest.approx(0.03)
    assert budget.reserved_usd == 0


def test_missing_receipts_are_held_and_rescheduled_without_transport(tmp_path, fake_clock):
    transport = RecordingTransport()
    receipts = BatchReceiptStub()
    service, _, _ = build_service(
        tmp_path, fake_clock, transport=transport, receipts=receipts
    )
    run_one(service)
    reserved_before = service.db.budget_snapshot(now=fake_clock()).reserved_usd
    receipts.queue_batch(missing_batch)

    fake_clock.advance(300)
    assert service.reconcile_pending() == {"checked": 1, "settled": 0, "estimated": 0, "held": 1}
    assert len(transport.calls) == 3
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == pytest.approx(reserved_before)
    assert recon_row(service)["error_code"] == "receipt_missing"
    assert recon_row(service)["next_check_at"] == pytest.approx(fake_clock() + 300)


def test_duplicate_receipt_rows_are_held_and_preserve_observed_cost_floor(tmp_path, fake_clock):
    receipts = BatchReceiptStub()
    service, _, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    run_one(service)

    def duplicate_first(user_agents: list[str]) -> dict[str, ReceiptResult]:
        result = valid_batch(user_agents)
        result[user_agents[0]] = ReceiptResult([receipt(cost=0.09), receipt(cost=0.09)], None)
        return result

    receipts.queue_batch(duplicate_first)
    fake_clock.advance(300)
    assert service.reconcile_pending() == {"checked": 1, "settled": 0, "estimated": 0, "held": 1}
    assert recon_row(service)["error_code"] == "receipt_cardinality"
    floor = service.db._conn.execute(
        "SELECT known_cost_floor_usd FROM reservation_cost_floors LIMIT 1"
    ).fetchone()[0]
    assert floor == pytest.approx(0.20)
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0


def test_wrong_identity_or_invalid_cost_stays_held(tmp_path, fake_clock):
    cases = [
        ("receipt_identity_mismatch", lambda: {"model": "wrong-model"}),
        ("receipt_identity_mismatch", lambda: {"tier": "flex"}),
        ("receipt_cost_unknown", lambda: {"cost": float("nan")}),
    ]
    for index, (expected_error, make_kwargs) in enumerate(cases):
        receipts = BatchReceiptStub()
        case_path = tmp_path / str(index)
        case_path.mkdir()
        service, _, _ = build_service(case_path, fake_clock, receipts=receipts)
        run_one(service)

        def mismatched_batch(user_agents: list[str], kwargs=make_kwargs()):
            values = []
            for _ in user_agents:
                values.append(receipt(**kwargs))
            return {user_agent: ReceiptResult([values[i]], None) for i, user_agent in enumerate(user_agents)}

        receipts.queue_batch(mismatched_batch)
        fake_clock.advance(300)
        assert service.reconcile_pending() == {"checked": 1, "settled": 0, "estimated": 0, "held": 1}
        assert recon_row(service)["error_code"] == expected_error
        assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0
        fake_clock.advance(300)


def test_reconciliation_settles_on_original_budget_day(tmp_path, fake_clock):
    receipts = BatchReceiptStub()
    service, _, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    original_day = service.db.budget_day(fake_clock())
    run_one(service)
    receipts.queue_batch(valid_batch)

    fake_clock.advance(86400 + 300)
    assert service.reconcile_pending() == {"checked": 1, "settled": 1, "estimated": 0, "held": 0}
    current_day = service.db.budget_day(fake_clock())
    original_spent = service.db._conn.execute(
        "SELECT spent_usd FROM budget_days WHERE budget_day = ?", (original_day,)
    ).fetchone()[0]
    assert original_day != current_day
    assert original_spent == pytest.approx(0.03)
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0


def test_bounded_reconciliation_closes_as_estimate_not_zero_or_actual(
    tmp_path, fake_clock
):
    transport = RecordingTransport()
    receipts = BatchReceiptStub()
    service, _, _ = build_service(
        tmp_path, fake_clock, transport=transport, receipts=receipts
    )
    service.config = replace(
        service.config,
        budget_basis="lei_group4_v1",
        pricing_upper_bound={MODEL: Pricing(2.5, 1.25, 15)},
        billing_prices={MODEL: Pricing(2.5, 1.25, 15)},
        reconciliation_max_checks=2,
    )
    run_one(service)
    receipts.queue_batch(missing_batch)

    fake_clock.advance(300)
    assert service.reconcile_pending() == {
        "checked": 1, "settled": 0, "estimated": 0, "held": 1
    }
    reserved = service.db.budget_snapshot(now=fake_clock()).reserved_usd
    assert reserved > 0

    fake_clock.advance(300)
    assert service.reconcile_pending() == {
        "checked": 1, "settled": 0, "estimated": 1, "held": 0
    }
    row = service.db._conn.execute(
        "select state, settled_amount_usd, settlement_basis from budget_reservations"
    ).fetchone()
    assert row["state"] == "settled"
    assert row["settled_amount_usd"] is None
    assert row["settlement_basis"] == "conservative_estimate"
    budget = service.db.budget_snapshot(now=fake_clock())
    assert budget.reserved_usd == 0
    assert budget.spent_usd == pytest.approx(reserved)
    assert service.db._conn.execute(
        "select count(*) from reservation_conservative_resolutions"
    ).fetchone()[0] == 1
    assert len(transport.calls) == 3



@pytest.mark.parametrize(
    ("corruption", "expected_error"),
    [
        ("billing_snapshot_invalid", "billing_snapshot_invalid"),
        ("probe_identity_incomplete", "probe_identity_incomplete"),
        ("receipt_identity_mismatch", "receipt_identity_mismatch"),
    ],
)
def test_bounded_reconciliation_keeps_integrity_errors_held(
    tmp_path, fake_clock, corruption, expected_error
):
    case_path = tmp_path / corruption
    case_path.mkdir()
    receipts = BatchReceiptStub()
    service, _, _ = build_service(case_path, fake_clock, receipts=receipts)
    service.config = replace(
        service.config,
        budget_basis="lei_group4_v1",
        pricing_upper_bound={MODEL: Pricing(2.5, 1.25, 15)},
        billing_prices={MODEL: Pricing(2.5, 1.25, 15)},
        reconciliation_max_checks=1,
    )
    run_one(service)

    reservation_id = service.db._conn.execute(
        "select id from budget_reservations order by id desc limit 1"
    ).fetchone()[0]
    if corruption == "billing_snapshot_invalid":
        service.db._conn.execute(
            "update budget_reservations set billing_prices_json = null where id = ?",
            (reservation_id,),
        )
        service.db._conn.commit()
    elif corruption == "probe_identity_incomplete":
        service.db._conn.execute(
            "delete from reconciliation_probes where reservation_id = ? and ordinal = 2",
            (reservation_id,),
        )
        service.db._conn.commit()
    else:
        def wrong_model_batch(user_agents: list[str]) -> dict[str, ReceiptResult]:
            return {
                user_agent: ReceiptResult([receipt(model="wrong-model")], None)
                for user_agent in user_agents
            }

        receipts.queue_batch(wrong_model_batch)
    if corruption != "receipt_identity_mismatch":
        receipts.queue_batch(missing_batch)

    fake_clock.advance(300)
    result = service.reconcile_pending()
    assert result == {
        "checked": 1, "settled": 0, "estimated": 0, "held": 1
    }
    row = service.db._conn.execute(
        "select state, settlement_basis from budget_reservations where id = ?",
        (reservation_id,),
    ).fetchone()
    assert row["state"] == "reserved"
    assert row["settlement_basis"] is None
    recon = service.db._conn.execute(
        "select error_code from reconciliation_rounds where reservation_id = ?",
        (reservation_id,),
    ).fetchone()
    assert recon["error_code"] == expected_error
    assert service.db._conn.execute(
        "select count(*) from reservation_conservative_resolutions"
    ).fetchone()[0] == 0


@pytest.mark.parametrize(
    ("daily_budget", "batch", "expected_settled", "expected_paused"),
    [
        (0.20, valid_batch, 1, False),
        (0.20, missing_batch, 0, True),
        (0.125, valid_batch, 1, True),
    ],
)

def test_resume_requires_verified_settlement_and_room_for_next_round(
    tmp_path, fake_clock, monkeypatch, daily_budget, batch, expected_settled, expected_paused
):
    receipts = BatchReceiptStub()
    case_path = tmp_path / str(daily_budget)
    case_path.mkdir()
    service, _, _ = build_service(
        case_path, fake_clock, enabled=True,
        daily_budget_usd=daily_budget, receipts=receipts,
    )
    # Make both the reservation and the post-reconciliation admission bound deterministic.
    monkeypatch.setattr(service, "_estimate_run_cost", lambda model, challenges: 0.10)
    run_one(service)
    service.db.pause_budget(now=fake_clock())
    receipts.queue_batch(batch)

    fake_clock.advance(300)
    result = service.reconcile_pending()
    assert result == {"checked": 1, "settled": expected_settled, "estimated": 0, "held": 1 - expected_settled}
    budget = service.db.budget_snapshot(now=fake_clock())
    assert (budget.paused_until is not None and budget.paused_until > fake_clock()) is expected_paused
    if expected_settled:
        assert budget.spent_usd == pytest.approx(0.03)
    else:
        assert budget.spent_usd == 0


@pytest.mark.parametrize(
    ("err_code", "status_code", "detail"),
    [
        ("upstream_auth", 401, "401 Unauthorized"),
        ("upstream_5xx", 500, "500 Internal Server Error"),
    ],
)
def test_upstream_error_auto_estimate_closes_conservatively_and_unfreezes_monitor(
    tmp_path, fake_clock, err_code, status_code, detail
):
    from modeltrace.transport import ProbeResult

    class ErrorTransport:
        def __init__(self):
            self.calls = []

        def run(self, **kwargs):
            self.calls.append(kwargs)
            return ProbeResult(
                text=None,
                status_code=status_code,
                error_code=err_code,
                transport_detail=detail,
            )

    transport = ErrorTransport()
    receipts = BatchReceiptStub()
    service, _, _ = build_service(
        tmp_path, fake_clock, transport=transport, receipts=receipts, daily_budget_usd=5.0
    )
    service.config = replace(
        service.config,
        budget_basis="lei_group4_v1",
        pricing_upper_bound={MODEL: Pricing(2.5, 1.25, 15)},
        billing_prices={MODEL: Pricing(2.5, 1.25, 15)},
        reconciliation_max_checks=12,
    )

    run_one(service)

    # Verify queue failed with error and round was recorded
    queue_row = service.db._conn.execute(
        "select state, error_code, reservation_id from queue order by id desc limit 1"
    ).fetchone()
    assert queue_row["state"] == "error"
    assert queue_row["error_code"] == err_code
    reservation_id = queue_row["reservation_id"]

    # The monitor has unsettled reservation, so scheduling should block new runs
    assert service.db.has_unsettled_monitor(1) is True

    # Under receipt_missing for all attempted probes, checks < 12 stay held
    receipts.queue_batch(missing_batch)
    for check_i in range(1, 12):
        fake_clock.advance(300)
        res = service.reconcile_pending()
        assert res == {"checked": 1, "settled": 0, "estimated": 0, "held": 1}
        round_row = service.db._conn.execute(
            "select checks, state, error_code from reconciliation_rounds where reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        assert round_row["checks"] == check_i
        assert round_row["state"] == "pending"
        assert round_row["error_code"] == "receipt_missing"
        assert service.db.has_unsettled_monitor(1) is True

    # Check 12: reaches reconciliation_max_checks, should auto-estimate closeout
    fake_clock.advance(300)
    res = service.reconcile_pending()
    assert res == {"checked": 1, "settled": 0, "estimated": 1, "held": 0}

    # Verify conservative settlement properties
    res_row = service.db._conn.execute(
        "select state, settled_amount_usd, settlement_basis, amount_usd from budget_reservations where id = ?",
        (reservation_id,),
    ).fetchone()
    assert res_row["state"] == "settled"
    assert res_row["settlement_basis"] == "conservative_estimate"
    # Never cleared to zero or actual receipt cost: retains full reserved amount
    assert res_row["settled_amount_usd"] is None
    reserved_amount = res_row["amount_usd"]
    assert reserved_amount > 0

    # Verify budget impact: spent reflects conservative estimate, daily budget still constrained
    budget = service.db.budget_snapshot(now=fake_clock())
    assert budget.reserved_usd == 0
    assert budget.spent_usd == pytest.approx(reserved_amount)
    assert budget.spent_usd > 0
    # Verify daily budget ceiling remains active and respected
    assert service.config.daily_budget_usd == 5.0
    assert budget.spent_usd + budget.reserved_usd <= service.config.daily_budget_usd

    # Verify monitor is unfrozen and eligible for scheduling
    assert service.db.has_unsettled_monitor(1) is False


@pytest.mark.parametrize(
    ("err_code", "status_code", "detail"),
    [
        ("upstream_auth", 403, "403 Forbidden"),
        ("upstream_5xx", 503, "503 Service Unavailable"),
    ],
)
def test_upstream_error_stays_held_on_receipt_query_failed_or_identity_corruption(
    tmp_path, fake_clock, err_code, status_code, detail
):
    from modeltrace.transport import ProbeResult

    class ErrorTransport:
        def run(self, **kwargs):
            return ProbeResult(
                text=None,
                status_code=status_code,
                error_code=err_code,
                transport_detail=detail,
            )

    # Case 1: receipt_query_failed does not auto-estimate even if checks >= 12
    receipts1 = BatchReceiptStub()
    case1_path = tmp_path / f"case1_{err_code}"
    case1_path.mkdir(parents=True, exist_ok=True)
    service1, _, _ = build_service(
        case1_path, fake_clock, transport=ErrorTransport(), receipts=receipts1
    )
    service1.config = replace(
        service1.config,
        budget_basis="lei_group4_v1",
        pricing_upper_bound={MODEL: Pricing(2.5, 1.25, 15)},
        billing_prices={MODEL: Pricing(2.5, 1.25, 15)},
        reconciliation_max_checks=1,
    )
    run_one(service1)

    def query_failed_batch(user_agents: list[str]) -> dict[str, ReceiptResult]:
        return {ua: ReceiptResult([], "receipt_query_failed") for ua in user_agents}

    receipts1.queue_batch(query_failed_batch)
    fake_clock.advance(300)
    res1 = service1.reconcile_pending()
    assert res1 == {"checked": 1, "settled": 0, "estimated": 0, "held": 1}
    assert service1.db.has_unsettled_monitor(1) is True

    # Case 2: integrity corruption (corrupt billing snapshot) stays held
    receipts2 = BatchReceiptStub()
    case2_path = tmp_path / f"case2_{err_code}"
    case2_path.mkdir(parents=True, exist_ok=True)
    service2, _, _ = build_service(
        case2_path, fake_clock, transport=ErrorTransport(), receipts=receipts2
    )
    service2.config = replace(
        service2.config,
        budget_basis="lei_group4_v1",
        pricing_upper_bound={MODEL: Pricing(2.5, 1.25, 15)},
        billing_prices={MODEL: Pricing(2.5, 1.25, 15)},
        reconciliation_max_checks=1,
    )
    run_one(service2)
    res2_id = service2.db._conn.execute(
        "select id from budget_reservations order by id desc limit 1"
    ).fetchone()[0]
    service2.db._conn.execute(
        "update budget_reservations set billing_prices_json = null where id = ?",
        (res2_id,),
    )
    service2.db._conn.commit()

    receipts2.queue_batch(missing_batch)
    fake_clock.advance(300)
    res2 = service2.reconcile_pending()
    assert res2 == {"checked": 1, "settled": 0, "estimated": 0, "held": 1}
    assert service2.db.has_unsettled_monitor(1) is True


@pytest.mark.parametrize(
    ("err_code", "status_code", "detail"),
    [
        ("upstream_auth", 401, "401 Unauthorized"),
        ("upstream_5xx", 502, "502 Bad Gateway"),
    ],
)
def test_upstream_error_with_real_receipt_settles_actual_without_auto_estimate(
    tmp_path, fake_clock, err_code, status_code, detail
):
    from modeltrace.transport import ProbeResult

    class ErrorTransport:
        def run(self, **kwargs):
            return ProbeResult(
                text=None,
                status_code=status_code,
                error_code=err_code,
                transport_detail=detail,
            )

    receipts = BatchReceiptStub()
    service, _, _ = build_service(
        tmp_path, fake_clock, transport=ErrorTransport(), receipts=receipts, daily_budget_usd=5.0
    )
    service.config = replace(
        service.config,
        budget_basis="lei_group4_v1",
        pricing_upper_bound={MODEL: Pricing(2.5, 1.25, 15)},
        billing_prices={MODEL: Pricing(2.5, 1.25, 15)},
        reconciliation_max_checks=12,
    )

    run_one(service)

    queue_row = service.db._conn.execute(
        "select state, error_code, reservation_id from queue order by id desc limit 1"
    ).fetchone()
    assert queue_row["state"] == "error"
    assert queue_row["error_code"] == err_code
    reservation_id = queue_row["reservation_id"]

    # If actual receipt is returned, reconcile should settle with actual cost, NOT conservative estimate
    def actual_receipt_batch(user_agents: list[str]) -> dict[str, ReceiptResult]:
        return {ua: ReceiptResult([receipt(cost=0.02)], None) for ua in user_agents}

    receipts.queue_batch(actual_receipt_batch)
    fake_clock.advance(300)
    res = service.reconcile_pending()
    # 1 attempted probe settled
    assert res == {"checked": 1, "settled": 1, "estimated": 0, "held": 0}

    res_row = service.db._conn.execute(
        "select state, settled_amount_usd, settlement_basis from budget_reservations where id = ?",
        (reservation_id,),
    ).fetchone()
    assert res_row["state"] == "settled"
    assert res_row["settlement_basis"] == "confirmed_actual"
    assert res_row["settled_amount_usd"] is not None and res_row["settled_amount_usd"] > 0
    # Confirms it settled actual receipt cost and did not invoke conservative estimate
    assert service.db._conn.execute("select count(*) from reservation_conservative_resolutions").fetchone()[0] == 0
    assert service.db.has_unsettled_monitor(1) is False
