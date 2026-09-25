from __future__ import annotations

import json
from pathlib import Path

import pytest

from modeltrace.config import load_config
from modeltrace.db import ModelTraceDB
from modeltrace.receipts import Receipt, ReceiptResult
from modeltrace.service import ModelTraceService
from modeltrace.transport import ProbeResult


class FakeClock:
    def __init__(self, value: float = 1_800_000_000.0):
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class FakeTransport:
    def __init__(self, text: str | None = None, error_code: str | None = None):
        self.calls: list[dict] = []
        self.text = text if text is not None else " ".join(str((index % 355) + 1) for index in range(355))
        self.error_code = error_code

    def run(self, *, model: str, challenge: dict, user_agent: str, session_affinity: str | None = None) -> ProbeResult:
        self.calls.append({"model": model, "challenge": challenge, "user_agent": user_agent, "session_affinity": session_affinity})
        return ProbeResult(self.text, 200, self.error_code)


class FakeReceipts:
    def __init__(
        self,
        account_ids: list[str] | None = None,
        missing: bool = False,
        models: list[str] | None = None,
        service_tiers: list[str] | None = None,
        row_counts: list[int] | None = None,
    ):
        self.calls: list[str] = []
        self.account_ids = account_ids or ["acct-1", "acct-1", "acct-1"]
        self.models = models or ["gpt-5.4", "gpt-5.4", "gpt-5.4"]
        self.service_tiers = service_tiers or ["default", "default", "default"]
        self.row_counts = row_counts or [1, 1, 1]
        self.missing = missing

    def read(self, user_agent: str) -> ReceiptResult:
        self.calls.append(user_agent)
        if self.missing:
            return ReceiptResult([], "receipt_missing")
        index = len(self.calls) - 1
        account_id = self.account_ids[min(index, len(self.account_ids) - 1)]
        model = self.models[min(index, len(self.models) - 1)]
        service_tier = self.service_tiers[min(index, len(self.service_tiers) - 1)]
        row_count = self.row_counts[min(index, len(self.row_counts) - 1)]
        rows = [
            Receipt(
                account_id=account_id,
                model=model,
                input_tokens=500,
                output_tokens=400,
                cache_read_tokens=0,
                total_cost=0.01,
                account_stats_cost=0.01,
                service_tier=service_tier,
                created_at=None,
            )
            for _ in range(row_count)
        ]
        return ReceiptResult(rows, None)


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


def build_service(
    tmp_path: Path,
    fake_clock: FakeClock,
    *,
    enabled: bool = False,
    daily_budget_usd: float = 5,
    transport: FakeTransport | None = None,
    receipts: FakeReceipts | None = None,
) -> tuple[ModelTraceService, FakeTransport, FakeReceipts]:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "base_url": "https://api.example.com/v1",
                "api_key": "test-key-not-used-by-fake",
                "enabled": enabled,
                "interval_seconds": 1800,
                "daily_budget_usd": daily_budget_usd,
                "max_output_tokens": 2048,
                "timeout_seconds": 120,
                "auto_retests": 0,
                "scope_label": "本站自用分组 5 · Responses 整链路（独立于连通性探测）",
                "monitors": {"1": {"model": "gpt-5.4", "enabled": enabled}},
                "pricing_upper_bound": {
                    "gpt-5.4": {
                        "input_per_1m_usd": 2.5,
                        "cache_read_per_1m_usd": 1.25,
                        "output_per_1m_usd": 15.0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    config = load_config(config_path)
    fake_transport = transport or FakeTransport()
    fake_receipts = receipts or FakeReceipts()
    service = ModelTraceService(
        config,
        database_path=tmp_path / "modeltrace.sqlite3",
        transport=fake_transport,
        receipt_reader=fake_receipts,
        clock=fake_clock,
        monotonic=fake_clock,
        sleeper=lambda _: None,
    )
    return service, fake_transport, fake_receipts


def run_one(service: ModelTraceService, monitor_id: int = 1) -> None:
    queued = service.db.enqueue(monitor_id, now=service.clock(), trigger="test")[1]
    assert queued is not None
    job = service.db.claim_next(now=service.clock())
    assert job is not None
    service._run_job_safely(job)
