from __future__ import annotations

import json

import pytest

from modeltrace.config import (
    DEFAULT_MAX_RESPONSE_BYTES,
    MAX_RESPONSE_BYTES,
    ConfigError,
    load_config,
)


def write_config(tmp_path, **overrides):
    raw = {
        "base_url": "https://api.example.test/v1",
        "api_key": "test-key",
        "monitors": {"1": {"model": "gpt-5.4"}},
    }
    raw.update(overrides)
    path = tmp_path / "config.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    return path


def test_default_response_envelope_budget_is_eight_mib_without_changing_probe_limits(tmp_path):
    config = load_config(write_config(tmp_path))

    assert config.max_response_bytes == DEFAULT_MAX_RESPONSE_BYTES == 8 * 1024 * 1024
    assert config.max_output_tokens is None
    assert config.timeout_seconds == 120


def test_response_envelope_budget_accepts_configured_high_cap_and_legacy_cap(tmp_path):
    high = load_config(write_config(tmp_path, max_response_bytes=MAX_RESPONSE_BYTES))
    assert high.max_response_bytes == 8 * 1024 * 1024

    legacy = load_config(write_config(tmp_path, max_response_bytes=1_048_576))
    assert legacy.max_response_bytes == 1_048_576


def test_response_envelope_budget_rejects_unbounded_values(tmp_path):
    with pytest.raises(ConfigError, match="max_response_bytes"):
        load_config(write_config(tmp_path, max_response_bytes=MAX_RESPONSE_BYTES + 1))


def test_reasoning_effort_overrides_are_per_model_and_default_is_none(tmp_path):
    config = load_config(write_config(tmp_path, reasoning_effort_overrides={"gpt-6-astra": "low"}))
    assert config.reasoning_effort_for("gpt-6-astra") == "low"
    assert config.reasoning_effort_for("gpt-5.4") == "none"


def test_astra_uses_supported_default_and_rejects_none(tmp_path):
    config = load_config(write_config(
        tmp_path,
        monitors={"1": {"model": "gpt-6-astra"}},
    ))
    assert config.reasoning_effort_for("gpt-6-astra") == "low"
    with pytest.raises(ConfigError, match="cannot be none"):
        load_config(write_config(
            tmp_path,
            monitors={"1": {"model": "gpt-6-astra"}},
            reasoning_effort_overrides={"gpt-6-astra": "none"},
        ))


@pytest.mark.parametrize("value", ["invalid", 1, None])
def test_reasoning_effort_override_rejects_invalid_values(tmp_path, value):
    overrides = {"gpt-6-astra": value}
    with pytest.raises(ConfigError, match="reasoning_effort_overrides"):
        load_config(write_config(tmp_path, reasoning_effort_overrides=overrides))


def test_retired_fee_configuration_is_ignored(tmp_path):
    config = load_config(write_config(tmp_path, daily_budget_usd=0, reconciliation_max_checks=1001,
                                     budget_basis='retired', billing_prices=None, pricing_upper_bound=None))
    assert config.reconciliation_max_checks == 0
    assert config.daily_budget_usd == 0
    assert config.budget_basis is None
    assert config.billing_prices == {} and config.pricing_upper_bound == {}
    assert config.interval_seconds == 3600


def test_config_max_output_tokens_omitted_or_null_is_none(tmp_path):
    d1 = tmp_path / "omitted"
    d1.mkdir()
    config_omitted = load_config(write_config(d1))
    assert config_omitted.max_output_tokens is None

    d2 = tmp_path / "null"
    d2.mkdir()
    config_null = load_config(write_config(d2, max_output_tokens=None))
    assert config_null.max_output_tokens is None


def test_config_max_output_tokens_explicit_validation(tmp_path):
    config = load_config(write_config(tmp_path, max_output_tokens=3000))
    assert config.max_output_tokens == 3000

    d_zero = tmp_path / "zero"
    d_zero.mkdir()
    with pytest.raises(ConfigError, match="max_output_tokens"):
        load_config(write_config(d_zero, max_output_tokens=0))

    d_large = tmp_path / "too_large"
    d_large.mkdir()
    with pytest.raises(ConfigError, match="max_output_tokens"):
        load_config(write_config(d_large, max_output_tokens=65537))
