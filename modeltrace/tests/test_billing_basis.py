from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from modeltrace.config import Pricing, load_config
from modeltrace.receipts import (
    RECEIPT_BATCH_QUERY_WITH_CACHE_CREATION,
    RECEIPT_QUERY_WITH_CACHE_CREATION,
    Receipt,
    ReceiptResult,
    ReceiptReader,
)
from tests.conftest import build_service, run_one


MODEL = "gpt-5.4"


class TokenReceipts:
    def __init__(self, *, receipt: Receipt, delayed: bool = False):
        self.receipt = receipt
        self.delayed = delayed
        self.read_calls: list[str] = []
        self.batch_calls: list[list[str]] = []

    def read(self, user_agent: str) -> ReceiptResult:
        self.read_calls.append(user_agent)
        if self.delayed:
            return ReceiptResult([], "receipt_missing")
        return ReceiptResult([self.receipt], None)

    def read_many(self, user_agents: list[str]) -> dict[str, ReceiptResult]:
        self.batch_calls.append(list(user_agents))
        return {
            user_agent: ReceiptResult([self.receipt], None)
            for user_agent in user_agents
        }


def token_receipt(*, charge: float, input_tokens: int | None = 100, cache_read_tokens: int | None = 20,
                  output_tokens: int | None = 30, cache_creation_tokens: int | None = 0,
                  tier: str = "default") -> Receipt:
    return Receipt(
        account_id="acct-1",
        model=MODEL,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_tokens=cache_read_tokens,
        total_cost=charge,
        account_stats_cost=charge,
        service_tier=tier,
        created_at=None,
        cache_creation_tokens=cache_creation_tokens,
    )


def use_group4(service, *, upper: Pricing, billing: Pricing):
    service.config = replace(
        service.config,
        budget_basis="lei_group4_v1",
        pricing_upper_bound={MODEL: upper},
        billing_prices={MODEL: billing},
    )
    return service


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_group4_config_has_separate_billing_prices_and_ignores_timestamp(tmp_path: Path):
    path = tmp_path / "config.json"
    entry = {
        "input_per_1m_usd": 12.5,
        "cache_read_per_1m_usd": 1.25,
        "output_per_1m_usd": 62.5,
    }
    path.write_text(
        json.dumps({
            "base_url": "https://api.example.test/v1",
            "api_key": "key",
            "budget_basis": "lei_group4_v1",
            "billing_price_snapshot_at": "not parsed by service",
            "monitors": {"1": {"model": MODEL}},
            "pricing_upper_bound": {MODEL: entry},
            "billing_prices": {MODEL: entry},
        }),
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.budget_basis == "lei_group4_v1"
    assert config.price_for(MODEL) == config.billing_price_for(MODEL)
    assert config.billing_price_for(MODEL).output_per_million_usd == 62.5


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_group4_initial_settlement_uses_tokens_not_upstream_cost_columns(tmp_path, fake_clock):
    receipt = token_receipt(charge=999.0)
    service, _, _ = build_service(
        tmp_path,
        fake_clock,
        receipts=TokenReceipts(receipt=receipt),
    )
    use_group4(
        service,
        # The deployment currently makes these equal, but a higher reserve
        # upper bound must remain valid without changing the billing formula.
        upper=Pricing(10, 20, 30),
        billing=Pricing(1, 2, 3),
    )

    run_one(service)

    expected = 3 * (100 * 1 + 20 * 2 + 30 * 3) / 1_000_000
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == pytest.approx(expected)
    assert service.snapshot(1, admin=True)["diagnostics"]["budget_basis"] == "lei_group4_v1"
    assert service.snapshot(1, admin=True)["diagnostics"]["effective_budget_basis"] == "lei_group4_v1"


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_group4_delayed_reconciliation_uses_persisted_snapshot_after_config_change(tmp_path, fake_clock):
    old_prices = Pricing(1, 2, 3)
    new_prices = Pricing(10, 20, 30)
    receipt = token_receipt(charge=777.0)
    receipts = TokenReceipts(receipt=receipt, delayed=True)
    service, _, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    use_group4(service, upper=old_prices, billing=old_prices)

    run_one(service)
    stored = service.db._conn.execute(
        "SELECT budget_basis, billing_prices_json, reserve_prices_json "
        "FROM budget_reservations ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert stored["budget_basis"] == "lei_group4_v1"
    assert json.loads(stored["billing_prices_json"])["output_per_million_usd"] == 3

    service.config = replace(
        service.config,
        pricing_upper_bound={MODEL: new_prices},
        billing_prices={MODEL: new_prices},
    )
    fake_clock.advance(300)
    assert service.reconcile_pending() == {"checked": 1, "settled": 1, "estimated": 0, "held": 0}

    old_cost = 3 * (100 * 1 + 20 * 2 + 30 * 3) / 1_000_000
    new_cost = 3 * (100 * 10 + 20 * 20 + 30 * 30) / 1_000_000
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == pytest.approx(old_cost)
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd != pytest.approx(new_cost)


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_group4_missing_tokens_and_cache_creation_fail_closed(tmp_path, fake_clock):
    for index, (receipt, expected_code) in enumerate([
        (token_receipt(charge=0.01, input_tokens=None), "receipt_cost_unknown"),
        (token_receipt(charge=0.01, cache_creation_tokens=1), "receipt_cost_unknown"),
        (token_receipt(charge=0.01, tier="scale"), "receipt_service_tier_mismatch"),
    ]):
        case = tmp_path / str(index)
        case.mkdir()
        service, _, _ = build_service(
            case,
            fake_clock,
            receipts=TokenReceipts(receipt=receipt),
        )
        use_group4(service, upper=Pricing(2.5, 1.25, 15), billing=Pricing(2.5, 1.25, 15))
        run_one(service)
        latest = service.snapshot(1)["latest"]
        if expected_code == "receipt_service_tier_mismatch":
            assert latest["status"] == "unverified"
            assert latest["message_code"] == expected_code
        else:
            # 3 valid outputs allow classifier scoring even when cost is unknown
            assert latest["status"] in {"match", "uncertain", "suspect"}
        assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0
        assert service.db.budget_snapshot(now=fake_clock()).reserved_usd > 0
        fake_clock.advance(1800)


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_old_untagged_reservation_stays_on_legacy_charge_path_under_group4_config(tmp_path, fake_clock):
    receipt = token_receipt(charge=0.03)
    receipts = TokenReceipts(receipt=receipt, delayed=True)
    service, _, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    run_one(service)
    use_group4(service, upper=Pricing(2.5, 1.25, 15), billing=Pricing(1, 2, 3))

    fake_clock.advance(300)
    assert service.reconcile_pending() == {"checked": 1, "settled": 1, "estimated": 0, "held": 0}
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == pytest.approx(0.09)
    diagnostics = service.snapshot(1, admin=True)["diagnostics"]
    assert diagnostics["legacy_day_settled_usd"] == pytest.approx(0.09)
    assert diagnostics["legacy_day_held_usd"] == 0
    assert diagnostics["legacy_carried_conservative_usd"] == 0


def test_group4_reader_contract_requires_appended_cache_creation_column():
    normalized = " ".join(RECEIPT_QUERY_WITH_CACHE_CREATION.split())
    batch_normalized = " ".join(RECEIPT_BATCH_QUERY_WITH_CACHE_CREATION.split())
    assert normalized.endswith("created_at,cache_creation_tokens FROM modeltrace_monitoring.probe_receipts WHERE user_agent=%s")
    assert batch_normalized.endswith("created_at,cache_creation_tokens FROM modeltrace_monitoring.probe_receipts WHERE user_agent = ANY(%s)")
    parsed = ReceiptReader._row_to_receipt(
        ["acct", MODEL, 1, 2, 3, 4, 5, "default", None, 0],
        include_cache_creation_tokens=True,
    )
    assert parsed.cache_creation_tokens == 0
    assert parsed.input_tokens == 1
    assert parsed.total_cost == 4
