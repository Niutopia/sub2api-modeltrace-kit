from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


DEFAULT_CONFIG_PATH = Path("/run/modeltrace/config.json")
DEFAULT_SCOPE_LABEL = "本站自用分组 5 · Responses 整链路（独立于连通性探测）"
DEFAULT_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
DEFAULT_IDLE_TIMEOUT_SECONDS = 600
VALID_REASONING_EFFORTS = frozenset({"none", "low", "medium", "high", "xhigh", "max"})
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
ASTRA_REASONING_MODELS = frozenset({"gpt-6-astra"})
DEFAULT_REASONING_EFFORTS = {"gpt-6-astra": "low"}


class ConfigError(ValueError):
    """Raised when the mounted service configuration is unsafe or incomplete."""


@dataclass(frozen=True)
class Pricing:
    """Upper-bound USD prices per one million tokens."""

    input_per_million_usd: float
    cache_read_per_million_usd: float
    output_per_million_usd: float

    @property
    def input_per_token(self) -> float:
        return self.input_per_million_usd / 1_000_000.0

    @property
    def cache_read_per_token(self) -> float:
        return self.cache_read_per_million_usd / 1_000_000.0

    @property
    def output_per_token(self) -> float:
        return self.output_per_million_usd / 1_000_000.0

    @property
    def worst_input_per_token(self) -> float:
        return max(self.input_per_token, self.cache_read_per_token)

    def as_dict(self) -> dict[str, float]:
        return {
            "input_per_million_usd": self.input_per_million_usd,
            "cache_read_per_million_usd": self.cache_read_per_million_usd,
            "output_per_million_usd": self.output_per_million_usd,
        }


@dataclass(frozen=True)
class MonitorConfig:
    monitor_id: int
    model: str
    enabled: bool
    configured_supported: bool = True


@dataclass(frozen=True)
class ServiceConfig:
    base_url: str
    api_key: str
    interval_seconds: int
    daily_budget_usd: float
    enabled: bool
    max_output_tokens: int | None
    timeout_seconds: int
    pricing_upper_bound: dict[str, Pricing]
    auto_retests: int
    scope_label: str
    max_response_bytes: int
    max_prompt_bytes: int
    monitors: dict[int, MonitorConfig]
    config_path: str
    reconciliation_max_checks: int = 12
    budget_basis: str | None = None
    billing_prices: dict[str, Pricing] = field(default_factory=dict)
    reasoning_effort_overrides: dict[str, str] = field(default_factory=dict)
    idle_timeout_seconds: int = DEFAULT_IDLE_TIMEOUT_SECONDS
    total_timeout_seconds: int | None = None
    host_api_base: str | None = None
    active_interval_seconds: int = 300
    idle_interval_seconds: int = 3600
    active_window_seconds: int = 600
    per_account_enabled: bool = True
    auto_pause_enabled: bool = True
    pause_minutes_oauth: int = 1440
    pause_minutes_apikey: int = 60
    paused_recheck_seconds_apikey: int = 1800

    def reasoning_effort_for(self, model: str) -> str:
        return self.reasoning_effort_overrides.get(model, DEFAULT_REASONING_EFFORTS.get(model, "none"))

    def price_for(self, model: str) -> Pricing | None:
        return self.pricing_upper_bound.get(model)

    def billing_price_for(self, model: str) -> Pricing | None:
        return self.billing_prices.get(model)

    @property
    def endpoint(self) -> str:
        return f"{self.base_url.rstrip('/')}/responses"


def _as_bool(value: Any, *, field: str, default: bool | None = None) -> bool:
    if value is None and default is not None:
        return default
    if isinstance(value, bool):
        return value
    raise ConfigError(f"{field} must be a boolean")


def _as_int(value: Any, *, field: str, minimum: int, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{field} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        limit = f"..{maximum}" if maximum is not None else f">={minimum}"
        raise ConfigError(f"{field} must be {limit}")
    return value


def _as_nonnegative_float(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{field} must be a non-negative number")
    result = float(value)
    if result < 0 or result != result or result in (float("inf"), float("-inf")):
        raise ConfigError(f"{field} must be a finite non-negative number")
    return result


def _first_number(raw: dict[str, Any], names: tuple[str, ...], *, field: str) -> float:
    for name in names:
        if name in raw:
            return _as_nonnegative_float(raw[name], field=f"{field}.{name}")
    raise ConfigError(f"{field} is missing one of: {', '.join(names)}")


def _parse_pricing(raw: Any, *, field_name: str = "pricing_upper_bound") -> dict[str, Pricing]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{field_name} must be an object")
    result: dict[str, Pricing] = {}
    for model, entry in raw.items():
        if not isinstance(model, str) or not model.strip():
            raise ConfigError(f"{field_name} model keys must be non-empty strings")
        if not isinstance(entry, dict):
            raise ConfigError(f"{field_name}.{model} must be an object")
        result[model] = Pricing(
            input_per_million_usd=_first_number(
                entry,
                ("input_per_1m_usd", "input_per_million_usd", "input_usd_per_million"),
                field=f"{field_name}.{model}",
            ),
            cache_read_per_million_usd=_first_number(
                entry,
                (
                    "cache_read_per_1m_usd",
                    "cache_read_per_million_usd",
                    "cache_read_usd_per_million",
                ),
                field=f"{field_name}.{model}",
            ),
            output_per_million_usd=_first_number(
                entry,
                ("output_per_1m_usd", "output_per_million_usd", "output_usd_per_million"),
                field=f"{field_name}.{model}",
            ),
        )
    return result


def _parse_monitors(raw: dict[str, Any], *, global_enabled: bool) -> dict[int, MonitorConfig]:
    source = raw.get("monitors")
    if source is None:
        source = raw.get("monitor_models")
    if source is None:
        raise ConfigError("monitors (or monitor_models) is required")

    items: list[tuple[Any, Any]]
    if isinstance(source, dict):
        items = list(source.items())
    elif isinstance(source, list):
        items = []
        for item in source:
            if not isinstance(item, dict):
                raise ConfigError("each monitors list item must be an object")
            if "id" not in item:
                raise ConfigError("each monitor must include id")
            items.append((item["id"], item))
    else:
        raise ConfigError("monitors must be an object or list")

    result: dict[int, MonitorConfig] = {}
    for raw_id, value in items:
        try:
            monitor_id = int(raw_id)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"monitor id {raw_id!r} must be an integer") from exc
        if monitor_id <= 0:
            raise ConfigError("monitor ids must be positive integers")

        if isinstance(value, str):
            model = value
            monitor_enabled = global_enabled
            configured_supported = True
        elif isinstance(value, dict):
            model = value.get("model", value.get("model_id"))
            if not isinstance(model, str) or not model.strip():
                raise ConfigError(f"monitor {monitor_id} must include a non-empty model")
            monitor_enabled = _as_bool(value.get("enabled"), field=f"monitors.{monitor_id}.enabled", default=global_enabled)
            configured_supported = _as_bool(
                value.get("supported"),
                field=f"monitors.{monitor_id}.supported",
                default=True,
            )
        else:
            raise ConfigError(f"monitor {monitor_id} must be a model string or object")

        if monitor_id in result:
            raise ConfigError(f"duplicate monitor id {monitor_id}")
        result[monitor_id] = MonitorConfig(
            monitor_id=monitor_id,
            model=model.strip(),
            enabled=monitor_enabled and global_enabled,
            configured_supported=configured_supported,
        )

    if not result:
        raise ConfigError("at least one monitor is required")
    return result


def _parse_reasoning_effort_overrides(raw: Any) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("reasoning_effort_overrides must be an object")
    result: dict[str, str] = {}
    for model, effort in raw.items():
        if not isinstance(model, str) or not model.strip():
            raise ConfigError("reasoning_effort_overrides model keys must be non-empty strings")
        if not isinstance(effort, str) or effort not in VALID_REASONING_EFFORTS:
            allowed = ", ".join(sorted(VALID_REASONING_EFFORTS))
            raise ConfigError(f"reasoning_effort_overrides.{model} must be one of: {allowed}")
        model_name = model.strip()
        if model_name in ASTRA_REASONING_MODELS and effort == "none":
            raise ConfigError(f"reasoning_effort_overrides.{model} cannot be none for Astra")
        result[model_name] = effort
    return result


def _validate_base_url(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError("base_url must be a non-empty URL")
    value = value.strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError("base_url must use http or https and include a host")
    if parsed.query or parsed.fragment:
        raise ConfigError("base_url must not include a query or fragment")
    return value


def load_config(path: str | Path | None = None) -> ServiceConfig:
    config_path = Path(path or os.environ.get("MODELTRACE_CONFIG", DEFAULT_CONFIG_PATH))
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"config file is not valid JSON: {config_path}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("config root must be an object")

    base_url = _validate_base_url(raw.get("base_url"))
    api_key = raw.get("api_key")
    if not isinstance(api_key, str) or not api_key.strip():
        raise ConfigError("api_key must be a non-empty string in the mounted config")
    global_enabled = _as_bool(raw.get("enabled"), field="enabled", default=False)
    interval_seconds = _as_int(raw.get("interval_seconds", 3600), field="interval_seconds", minimum=60)
    daily_budget_usd = 0.0  # compatibility only; detector has no fee controls
    max_output_tokens = (
        _as_int(raw["max_output_tokens"], field="max_output_tokens", minimum=1, maximum=65536)
        if "max_output_tokens" in raw and raw["max_output_tokens"] is not None
        else None
    )
    
    timeout_seconds = _as_int(raw.get("timeout_seconds", 120), field="timeout_seconds", minimum=1, maximum=120)
    
    # Idle timeout protection: default 600s, configurable via idle_timeout_seconds or overridden by timeout_seconds
    if "idle_timeout_seconds" in raw and raw["idle_timeout_seconds"] is not None:
        idle_timeout_seconds = _as_int(raw["idle_timeout_seconds"], field="idle_timeout_seconds", minimum=1)
    elif "timeout_seconds" in raw and raw["timeout_seconds"] is not None:
        idle_timeout_seconds = timeout_seconds
    else:
        idle_timeout_seconds = DEFAULT_IDLE_TIMEOUT_SECONDS

    # Optional wall-clock cap per probe; unset = none (only the idle guard applies).
    total_timeout_seconds = (
        _as_int(raw["total_timeout_seconds"], field="total_timeout_seconds", minimum=1)
        if raw.get("total_timeout_seconds") is not None
        else None
    )

    raw_host_api_base = raw.get("host_api_base")
    host_api_base = _validate_base_url(raw_host_api_base) if raw_host_api_base is not None else None
    active_interval_seconds = _as_int(raw.get("active_interval_seconds", 300), field="active_interval_seconds", minimum=1)
    idle_interval_seconds = _as_int(raw.get("idle_interval_seconds", 3600), field="idle_interval_seconds", minimum=1)
    active_window_seconds = _as_int(raw.get("active_window_seconds", 600), field="active_window_seconds", minimum=1)
    per_account_enabled = _as_bool(raw.get("per_account_enabled"), field="per_account_enabled", default=True)
    auto_pause_enabled = _as_bool(raw.get("auto_pause_enabled"), field="auto_pause_enabled", default=True)
    pause_minutes_oauth = _as_int(raw.get("pause_minutes_oauth", 1440), field="pause_minutes_oauth", minimum=1)
    pause_minutes_apikey = _as_int(raw.get("pause_minutes_apikey", 60), field="pause_minutes_apikey", minimum=1)
    paused_recheck_seconds_apikey = _as_int(
        raw.get("paused_recheck_seconds_apikey", 1800),
        field="paused_recheck_seconds_apikey",
        minimum=1,
    )

    auto_retests = _as_int(raw.get("auto_retests", 0), field="auto_retests", minimum=0, maximum=0)
    reconciliation_max_checks = 0  # retired
    max_response_bytes = _as_int(
        raw.get("max_response_bytes", DEFAULT_MAX_RESPONSE_BYTES),
        field="max_response_bytes",
        minimum=4_096,
        maximum=MAX_RESPONSE_BYTES,
    )
    max_prompt_bytes = _as_int(
        raw.get("max_prompt_bytes", 65_536),
        field="max_prompt_bytes",
        minimum=4_096,
        maximum=1_048_576,
    )
    scope_label = raw.get("scope_label", DEFAULT_SCOPE_LABEL)
    if not isinstance(scope_label, str) or not scope_label.strip():
        raise ConfigError("scope_label must be a non-empty string")

    pricing_upper_bound = {}  # retired, retained only for old config compatibility
    budget_basis = None
    billing_prices = {}
    monitors = _parse_monitors(raw, global_enabled=global_enabled)
    reasoning_effort_overrides = _parse_reasoning_effort_overrides(raw.get("reasoning_effort_overrides"))

    return ServiceConfig(
        base_url=base_url,
        api_key=api_key,
        interval_seconds=interval_seconds,
        daily_budget_usd=daily_budget_usd,
        enabled=global_enabled,
        max_output_tokens=max_output_tokens,
        timeout_seconds=timeout_seconds,
        pricing_upper_bound=pricing_upper_bound,
        auto_retests=auto_retests,
        scope_label=scope_label.strip(),
        max_response_bytes=max_response_bytes,
        max_prompt_bytes=max_prompt_bytes,
        monitors=monitors,
        config_path=str(config_path),
        reconciliation_max_checks=reconciliation_max_checks,
        budget_basis=budget_basis,
        billing_prices=billing_prices,
        reasoning_effort_overrides=reasoning_effort_overrides,
        idle_timeout_seconds=idle_timeout_seconds,
        total_timeout_seconds=total_timeout_seconds,
        host_api_base=host_api_base,
        active_interval_seconds=active_interval_seconds,
        idle_interval_seconds=idle_interval_seconds,
        active_window_seconds=active_window_seconds,
        per_account_enabled=per_account_enabled,
        auto_pause_enabled=auto_pause_enabled,
        pause_minutes_oauth=pause_minutes_oauth,
        pause_minutes_apikey=pause_minutes_apikey,
        paused_recheck_seconds_apikey=paused_recheck_seconds_apikey,
    )
