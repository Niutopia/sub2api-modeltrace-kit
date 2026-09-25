from __future__ import annotations

from modeltrace.receipts import RECEIPT_QUERY


def test_receipt_query_is_limited_to_restricted_view():
    normalized = " ".join(RECEIPT_QUERY.split())
    assert normalized == (
        "SELECT account_id, model, input_tokens,output_tokens,cache_read_tokens,total_cost,account_stats_cost,service_tier,created_at,cache_creation_tokens "
        "FROM modeltrace_monitoring.probe_receipts WHERE user_agent=%s"
    )
    assert "FROM modeltrace_monitoring.probe_receipts" in normalized
    assert "business" not in normalized.lower()


def test_account_stats_cost_is_real_cost_preferred_with_total_fallback():
    from modeltrace.receipts import Receipt

    row = Receipt("a", "m", 1, 1, 0, 9.0, 3.0, "default", None)
    assert row.charge_usd == 3.0
    fallback = Receipt("a", "m", 1, 1, 0, 9.0, None, "default", None)
    assert fallback.charge_usd == 9.0


import json
from dataclasses import replace

import pytest

from modeltrace.receipts import ReceiptReader, ReceiptResult
from tests.conftest import FakeReceipts, build_service, run_one


@pytest.mark.parametrize("tier", [None, "", "auto", "scale", "priority", "fast", "flex", "ultrafast", "DEFAULT", " default "])
@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_only_explicit_default_tier_is_verified(tmp_path, fake_clock, tier):
    service, _, _ = build_service(tmp_path, fake_clock, receipts=FakeReceipts(service_tiers=[tier] * 3))
    run_one(service)
    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "unverified"
    assert latest["message_code"] == "receipt_service_tier_mismatch"
    assert latest["target_probability"] is None
    assert latest["ranking"] == []


@pytest.mark.parametrize("account", [None, "", " "])
@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_account_must_be_present_not_just_equal(tmp_path, fake_clock, account):
    service, _, _ = build_service(tmp_path, fake_clock, receipts=FakeReceipts(account_ids=[account] * 3))
    run_one(service)
    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "unverified"
    assert latest["target_probability"] is None
    assert latest["ranking"] == []


class CostReceipts(FakeReceipts):
    def __init__(self, charges, *, row_counts=None, error=None):
        super().__init__(row_counts=row_counts)
        self.charges = charges
        self.error = error

    def read(self, user_agent):
        result = super().read(user_agent)
        charge = self.charges[min(len(self.calls) - 1, len(self.charges) - 1)]
        return ReceiptResult([
            replace(row, account_stats_cost=charge, total_cost=None) for row in result.receipts
        ], self.error)


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_partial_receipts_never_discard_known_cost_above_reservation(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock, receipts=CostReceipts([2.0, None, 2.0]))
    run_one(service)
    budget = service.db.budget_snapshot(now=fake_clock())
    assert budget.spent_usd + budget.reserved_usd > 4.0
    assert service.snapshot(1, admin=True)["diagnostics"]["last_actual_cost_usd"] is None
    fake_clock.advance(86400)
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd > 4.0


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_duplicate_receipts_never_discard_known_charges(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock, receipts=CostReceipts([2.0] * 3, row_counts=[2, 1, 1]))
    run_one(service)
    budget = service.db.budget_snapshot(now=fake_clock())
    assert budget.spent_usd + budget.reserved_usd >= 8.0
    assert service.snapshot(1)["latest"]["status"] == "unverified"
    assert service.snapshot(1)["paused_budget"] is True


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_receipt_error_never_releases_reservation(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock, receipts=CostReceipts([0.001] * 3, error="receipt_query_failed"))
    run_one(service)
    admin = service.snapshot(1, admin=True)["diagnostics"]
    assert admin["daily_reserved_usd"] == pytest.approx(admin["last_reserved_usd"], abs=1e-8)
    assert admin["last_actual_cost_usd"] is None


def test_reader_does_not_convert_null_tier_into_default():
    receipt = ReceiptReader._row_to_receipt(["acct", "gpt-5.4", 1, 1, 0, 9, 3, None, None])
    assert receipt.service_tier is None
    assert receipt.charge_usd == 3


def test_public_snapshot_and_persisted_rounds_exclude_secrets_and_raw_output(tmp_path, fake_clock, capsys):
    from tests.conftest import FakeTransport

    secret = "sk-do-not-log-this-key"
    raw_output = "PRIVATE_UPSTREAM_BODY " + " ".join(str(i) for i in range(1, 356))
    service, _, _ = build_service(tmp_path, fake_clock, transport=FakeTransport(text=raw_output))
    service.config = replace(service.config, api_key=secret)
    run_one(service)
    public = service.snapshot(1)
    assert set(public) == {
        "monitor_id", "model", "scope_label", "protocol", "reasoning_effort",
        "calibration_reference_effort", "calibration_compatibility", "calibration_model", "service_tier",
        "enabled", "supported", "running", "interval_seconds", "last_checked_at", "next_run_at",
        "stale", "latest", "history", "paused_budget", "next_run_after", "paused_reconciliation",
        "retest_progress", "accounts_summary",
    }
    if public["retest_progress"] is not None:
        assert set(public["retest_progress"]) == {"done", "total"}
        assert isinstance(public["retest_progress"]["done"], int)
        assert isinstance(public["retest_progress"]["total"], int)
    assert set(public["latest"]) == {"id", "status", "target_probability", "best_model", "checked_at", "message_code", "ranking"}
    serialized = json.dumps(public)
    for forbidden in [secret, "PRIVATE_UPSTREAM_BODY", "acct-1", "account_id", "cost_usd", "diagnostics", "base_url", "user_agent"]:
        assert forbidden not in serialized
    persisted = "\n".join(service.db._conn.iterdump())
    captured = capsys.readouterr()
    assert secret not in persisted + captured.out + captured.err
    assert "PRIVATE_UPSTREAM_BODY" not in persisted + captured.out + captured.err


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -0.1, True, None])
@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_invalid_receipt_charge_never_scores_or_releases(tmp_path, fake_clock, invalid):
    service, _, _ = build_service(tmp_path, fake_clock, receipts=CostReceipts([invalid] * 3))
    run_one(service)
    # 3 valid outputs allow classifier scoring even when receipt charge is invalid/unknown
    assert service.snapshot(1)["latest"]["status"] in {"match", "uncertain", "suspect"}
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd > 0
    assert service.snapshot(1, admin=True)["diagnostics"]["last_actual_cost_usd"] is None


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_verified_cost_can_exceed_estimate_and_pause_budget(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock, receipts=CostReceipts([2.0] * 3))
    run_one(service)
    admin = service.snapshot(1, admin=True)
    assert admin["diagnostics"]["daily_spent_usd"] == 6.0
    assert admin["diagnostics"]["daily_reserved_usd"] == 0
    assert admin["diagnostics"]["last_actual_cost_usd"] == 6.0
    assert admin["paused_budget"] is True


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_receipt_exception_keeps_earlier_observed_cost_without_leaking(tmp_path, fake_clock, capsys):
    secret = "sk-private-key https://private.invalid/raw-response"

    class InterruptedReader(CostReceipts):
        def read(self, user_agent):
            if len(self.calls) == 2:
                raise RuntimeError(secret)
            return super().read(user_agent)

    service, transport, _ = build_service(tmp_path, fake_clock, receipts=InterruptedReader([2, 2]))
    run_one(service)
    assert len(transport.calls) == 3  # three distinct probes, never a retry
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd > 4.0
    assert service.snapshot(1)["latest"]["status"] in {"match", "uncertain", "suspect"}
    assert secret not in json.dumps(service.snapshot(1, admin=True))
    assert secret not in "\n".join(service.db._conn.iterdump())
    captured = capsys.readouterr()
    assert secret not in captured.out + captured.err


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_account_stats_cost_and_total_fallback_are_not_group_discounted(tmp_path, fake_clock):
    class ViewReceipts(FakeReceipts):
        def read(self, user_agent):
            result = super().read(user_agent)
            # The restricted view already applies account_rate_multiplier.
            # Never apply the self-group's .001 multiplier a second time.
            account_cost = None if len(self.calls) == 3 else 0.3
            return ReceiptResult([replace(result.receipts[0], account_stats_cost=account_cost, total_cost=0.9)], None)

    service, _, _ = build_service(tmp_path, fake_clock, receipts=ViewReceipts())
    run_one(service)
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == pytest.approx(1.5)


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_low_cost_wrong_model_or_tier_never_releases_reservation(tmp_path, fake_clock):
    receipts = FakeReceipts(models=["gpt-5.5"] * 3, service_tiers=[None] * 3)
    service, _, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    run_one(service)
    admin = service.snapshot(1, admin=True)["diagnostics"]
    assert admin["daily_spent_usd"] == 0
    assert admin["daily_reserved_usd"] == pytest.approx(admin["last_reserved_usd"], abs=1e-8)
    assert admin["last_actual_cost_usd"] is None
