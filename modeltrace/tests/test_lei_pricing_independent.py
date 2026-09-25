from __future__ import annotations

import pytest

from modeltrace.config import Pricing
from modeltrace.receipts import Receipt
from modeltrace.service import ModelTraceService

pytestmark = pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")


ACTUAL_PRICES = (
    pytest.param(
        "gpt-6-astra",
        12.5,
        62.5,
        1.25,
        id="astra-12.5-62.5-1.25",
    ),
    pytest.param(
        "gpt-5.6-sol",
        6.25,
        37.5,
        0.625,
        id="sol-6.25-37.5-0.625",
    ),
    pytest.param(
        "gpt-5.6-terra",
        2.5,
        15.0,
        0.25,
        id="terra-2.5-15-0.25",
    ),
    pytest.param(
        "gpt-5.6-luna",
        0.25,
        1.5,
        0.025,
        id="luna-0.25-1.5-0.025",
    ),
)


def make_receipt(**overrides: object) -> Receipt:
    values: dict[str, object] = {
        "account_id": "acct-lei",
        "model": "gpt-6-astra",
        "input_tokens": 1_000,
        "output_tokens": 3_000,
        "cache_read_tokens": 2_000,
        "total_cost": 10**99,
        "account_stats_cost": 10**99,
        "service_tier": "default",
        "created_at": None,
        "cache_creation_tokens": 0,
    }
    values.update(overrides)
    return Receipt(**values)


@pytest.mark.parametrize(
    ("model", "input_price", "output_price", "cache_read_price"), ACTUAL_PRICES
)
def test_receipt_token_cost_uses_actual_model_prices_without_cache_subtraction(
    model: str, input_price: float, output_price: float, cache_read_price: float
) -> None:
    pricing = Pricing(
        input_per_million_usd=input_price,
        cache_read_per_million_usd=cache_read_price,
        output_per_million_usd=output_price,
    )
    receipt = make_receipt(model=model)

    expected = (
        1_000 * input_price / 1_000_000
        + 2_000 * cache_read_price / 1_000_000
        + 3_000 * output_price / 1_000_000
    )

    assert ModelTraceService._receipt_token_cost(receipt, pricing) == pytest.approx(expected)


def test_receipt_token_cost_ignores_huge_upstream_cost_columns() -> None:
    pricing = Pricing(
        input_per_million_usd=12.5,
        cache_read_per_million_usd=1.25,
        output_per_million_usd=62.5,
    )
    receipt = make_receipt(total_cost=10**99, account_stats_cost=10**99)

    expected = 1_000 * 12.5 / 1_000_000 + 2_000 * 1.25 / 1_000_000 + 3_000 * 62.5 / 1_000_000

    assert ModelTraceService._receipt_token_cost(receipt, pricing) == pytest.approx(expected)


@pytest.mark.parametrize("field", [
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
])
@pytest.mark.parametrize("invalid_count", [None, -1, True, False], ids=["none", "negative", "true", "false"])
def test_receipt_token_cost_invalid_token_counts_fail_closed(field: str, invalid_count: object) -> None:
    receipt = make_receipt(**{field: invalid_count})

    assert ModelTraceService._receipt_token_cost(receipt, Pricing(input_per_million_usd=12.5, cache_read_per_million_usd=1.25, output_per_million_usd=62.5)) is None


def test_receipt_token_cost_nondefault_service_tier_fails_closed() -> None:
    receipt = make_receipt(service_tier="flex")

    assert ModelTraceService._receipt_token_cost(receipt, Pricing(input_per_million_usd=12.5, cache_read_per_million_usd=1.25, output_per_million_usd=62.5)) is None


def test_receipt_token_cost_context_over_66560_tokens_fails_closed() -> None:
    receipt = make_receipt(input_tokens=66_561, output_tokens=0, cache_read_tokens=0)

    assert ModelTraceService._receipt_token_cost(receipt, Pricing(input_per_million_usd=12.5, cache_read_per_million_usd=1.25, output_per_million_usd=62.5)) is None


def test_receipt_token_cost_all_zero_usage_fails_closed() -> None:
    receipt = make_receipt(input_tokens=0, output_tokens=0, cache_read_tokens=0)

    assert ModelTraceService._receipt_token_cost(
        receipt,
        Pricing(input_per_million_usd=12.5, cache_read_per_million_usd=1.25, output_per_million_usd=62.5),
    ) is None
