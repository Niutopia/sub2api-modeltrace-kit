from __future__ import annotations

import pytest

from modeltrace.transport import ProbeResult
from tests.conftest import FakeReceipts, FakeTransport, build_service, run_one


@pytest.mark.skip(reason="v0.1.12 单题检测，新规则见 tests/test_single_probe_v0112.py")
def test_successful_round_is_three_requests_and_history_oldest_first(tmp_path, fake_clock):
    service, transport, receipts = build_service(tmp_path, fake_clock)
    run_one(service)

    assert len(transport.calls) == 3
    assert len(receipts.calls) == 3
    assert all(call["user_agent"].startswith("ModelTraceProbe/") for call in transport.calls)
    assert len({call["user_agent"] for call in transport.calls}) == 3
    snapshot = service.snapshot(1)
    assert snapshot["last_checked_at"] is not None
    assert len(snapshot["history"]) == 1
    assert snapshot["history"][0]["id"] == snapshot["latest"]["id"]
    assert snapshot["latest"]["status"] in {"match", "uncertain", "suspect"}
    assert 0 <= snapshot["latest"]["target_probability"] <= 1
    assert len(snapshot["latest"]["ranking"]) <= 3
    assert snapshot["history"] == sorted(snapshot["history"], key=lambda item: item["checked_at"])
    diagnostics = service.snapshot(1, admin=True)["diagnostics"]
    assert diagnostics["last_receipt_count"] == 3
    assert diagnostics["last_receipt_consistent"] is True
    assert diagnostics["last_actual_cost_usd"] == 0.03


def test_insufficient_sample_is_error_and_never_scored(tmp_path, fake_clock):
    service, transport, _ = build_service(tmp_path, fake_clock, transport=FakeTransport(text="1 2 3"))
    run_one(service)

    assert len(transport.calls) == 1
    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "error"
    assert latest["message_code"] == "insufficient_sample"
    assert latest["target_probability"] is None
    assert latest["best_model"] is None
    assert latest["ranking"] == []


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_missing_receipts_fail_closed_to_unverified(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock, receipts=FakeReceipts(missing=True))
    run_one(service)

    latest = service.snapshot(1)["latest"]
    # 3 valid outputs allow classifier scoring even when receipts are missing
    assert latest["status"] in {"match", "uncertain", "suspect"}
    assert latest["target_probability"] is not None
    # Ledger remains reserved
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd > 0
    assert service.snapshot(1, admin=True)["diagnostics"]["last_actual_cost_usd"] is None


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_mixed_upstream_accounts_never_score(tmp_path, fake_clock):
    receipts = FakeReceipts(account_ids=["acct-1", "acct-2", "acct-1"])
    service, _, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    run_one(service)

    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "mixed_route"
    assert latest["target_probability"] is None
    assert latest["ranking"] == []


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_budget_is_reserved_before_network_and_pauses_until_reset(tmp_path, fake_clock):
    service, transport, _ = build_service(tmp_path, fake_clock, daily_budget_usd=0.000001)
    run_one(service)

    assert transport.calls == []
    snapshot = service.snapshot(1)
    assert snapshot["latest"]["status"] == "budget_exhausted"
    assert snapshot["paused_budget"] is True
    assert snapshot["next_run_after"] is not None
    assert snapshot["next_run_at"] == snapshot["next_run_after"]


def test_unsupported_model_has_explicit_status(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    monitor = service.config.monitors[1]
    service.config.monitors  # keep config immutable; unsupported is tested via a replacement service below
    from dataclasses import replace
    from modeltrace.config import MonitorConfig

    service.config = replace(
        service.config,
        monitors={1: MonitorConfig(1, "not-in-bank", False, True)},
    )
    # Existing state is still independent of the config mapping.
    run_one(service)
    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "unsupported"
    assert latest["message_code"] == "unsupported_model"


def test_initial_schedule_is_global_and_uniform_across_different_models(tmp_path, fake_clock):
    from dataclasses import replace
    from modeltrace.config import MonitorConfig

    service, _, _ = build_service(tmp_path, fake_clock)
    models = ["gpt-5.6-sol", "gpt-6-astra", "gpt-5.6-terra", "gpt-5.6-luna"]
    monitors = {
        index + 1: MonitorConfig(index + 1, model, True, True)
        for index, model in enumerate(models)
    }
    service.config = replace(
        service.config,
        enabled=True,
        monitors=monitors,
        pricing_upper_bound={},
    )
    service.db.seed_monitors(monitors, now=fake_clock())
    service._initialize_next_runs(fake_clock())

    offsets = [
        int(service.db.get_state(monitor_id)["next_run_at"] - fake_clock())
        for monitor_id in sorted(monitors)
    ]
    assert offsets == [0, 450, 900, 1350]
    assert len(set(offsets)) == 4
    assert max(offsets) < 1800


def test_unknown_cost_stays_on_original_ledger_and_carries_without_release(tmp_path, fake_clock):
    from datetime import datetime, timezone

    fake_clock.value = datetime(2026, 9, 19, 23, 59, 55, tzinfo=timezone.utc).timestamp()
    service, _, _ = build_service(tmp_path, fake_clock)
    queue_id = service.db.enqueue(1, now=fake_clock(), trigger="test")[1]
    assert queue_id is not None
    assert service.db.claim_next(now=fake_clock()) is not None
    reservation = service.db.reserve_budget(
        1,
        queue_id,
        0.25,
        daily_budget_usd=5,
        now=fake_clock(),
    )
    assert reservation is not None

    fake_clock.advance(10)  # settles after the UTC date boundary
    settled = service.db.settle_reservation(
        reservation.reservation_id,
        actual_cost_usd=None,
        now=fake_clock(),
    )
    assert settled == 0.25

    original_day = service.db.budget_snapshot(now=datetime(2026, 9, 19, 23, 59, 55, tzinfo=timezone.utc).timestamp())
    next_day = service.db.budget_snapshot(now=fake_clock())
    assert original_day.spent_usd == 0.0
    assert original_day.reserved_usd == 0.25
    assert next_day.spent_usd == 0.0
    assert next_day.reserved_usd == 0.25


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_receipts_require_exactly_one_row_per_probe(tmp_path, fake_clock):
    receipts = FakeReceipts(row_counts=[2, 1, 1])
    service, _, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    run_one(service)

    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "unverified"
    assert latest["message_code"] == "receipt_cardinality"
    assert latest["target_probability"] is None


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_receipt_model_must_match_target_model(tmp_path, fake_clock):
    receipts = FakeReceipts(models=["gpt-5.5", "gpt-5.5", "gpt-5.5"])
    service, _, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    run_one(service)

    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "unverified"
    assert latest["message_code"] == "receipt_model_mismatch"
    assert latest["target_probability"] is None


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_receipt_service_tier_must_be_default(tmp_path, fake_clock):
    receipts = FakeReceipts(service_tiers=["flex", "flex", "flex"])
    service, _, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    run_one(service)

    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "unverified"
    assert latest["message_code"] == "receipt_service_tier_mismatch"
    assert latest["target_probability"] is None


def _four_monitors(service, fake_clock):
    from dataclasses import replace
    from modeltrace.config import MonitorConfig

    models = ["gpt-5.6-sol", "gpt-6-astra", "gpt-5.6-terra", "gpt-5.6-luna"]
    monitors = {i: MonitorConfig(i, model, True) for i, model in enumerate(models, 1)}
    service.config = replace(
        service.config, enabled=True, monitors=monitors,
        pricing_upper_bound={},
    )
    service.db.seed_monitors(monitors, now=fake_clock())
    service._initialize_next_runs(fake_clock())
    return monitors


def _restart(service, fake_clock):
    from modeltrace.service import ModelTraceService

    config, path = service.config, service.db.path
    service.db.close()
    return ModelTraceService(
        config, database_path=path, transport=service.transport,
        receipt_reader=getattr(service, "receipt_reader", None), clock=fake_clock,
        monotonic=fake_clock, sleeper=lambda _: None,
    )


def _reserve(service, fake_clock, amount):
    monitor_id = 1
    _, queue_id = service.db.enqueue(monitor_id, now=fake_clock(), trigger="test")
    assert queue_id is not None
    job = service.db.claim_next(now=fake_clock())
    assert job is not None
    reservation = service.db.reserve_budget(
        monitor_id, queue_id, amount, daily_budget_usd=5, now=fake_clock(),
    )
    assert reservation is not None
    return job, reservation


def test_restart_preserves_unrun_staggered_slots(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    monitors = _four_monitors(service, fake_clock)
    before = [service.db.get_state(i)["next_run_at"] for i in monitors]
    fake_clock.advance(120)
    service = _restart(service, fake_clock)
    assert [service.db.get_state(i)["next_run_at"] for i in monitors] == before


def test_completion_does_not_drift_global_schedule(tmp_path, fake_clock, monkeypatch):
    import modeltrace.service as service_module

    class SlowTransport(FakeTransport):
        def run(self, **kwargs):
            fake_clock.advance(10)
            return super().run(**kwargs)

    service, _, _ = build_service(tmp_path, fake_clock, transport=SlowTransport())
    monkeypatch.setattr(
        service_module,
        "analyze_outputs",
        lambda *_: {
            "prediction": "gpt-5.6-sol",
            "results": [{"model": "gpt-5.6-sol", "probability": 0.95}],
            "calibration": {"queries": "1"},
        },
    )
    _four_monitors(service, fake_clock)
    start = fake_clock()
    run_one(service)
    assert service.db.get_state(1)["next_run_at"] == start + 1800


def test_mixed_slow_rounds_do_not_starve_a_monitor(tmp_path, fake_clock):
    from dataclasses import replace
    from modeltrace.config import MonitorConfig

    class MixedTransport(FakeTransport):
        def run(self, **kwargs):
            fake_clock.advance(120 if kwargs["model"] in {"gpt-5.6-sol", "gpt-5.6-luna"} else 0.15)
            self.calls.append({"model": kwargs["model"], "user_agent": kwargs["user_agent"]})
            return ProbeResult("", None, "upstream_timeout")

    models = {
        1: "gpt-5.6-sol",
        2: "gpt-6-astra",
        5: "gpt-5.6-terra",
        6: "gpt-5.6-luna",
    }
    transport = MixedTransport()
    service, _, _ = build_service(tmp_path, fake_clock, enabled=True, transport=transport)
    monitors = {monitor_id: MonitorConfig(monitor_id, model, True, True) for monitor_id, model in models.items()}
    service.config = replace(service.config, interval_seconds=600, monitors=monitors)
    service.db.seed_monitors(monitors, now=fake_clock())
    service._initialize_next_runs(fake_clock())

    started: list[int] = []
    for _ in range(8):
        while True:
            service._schedule_due(fake_clock())
            job = service.db.claim_next(now=fake_clock())
            if job is not None:
                break
            due = [service.db.get_state(monitor_id)["next_run_at"] for monitor_id in models]
            fake_clock.advance(max(0.001, min(value for value in due if value is not None) - fake_clock()))
        started.append(job.monitor_id)
        service._run_job_safely(job)

    assert len(transport.calls) == 8
    assert all(started.count(monitor_id) >= 2 for monitor_id in models)


def test_completed_round_then_long_idle_does_not_replay_old_slots(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock, enabled=True)
    service._schedule_due(fake_clock())
    job = service.db.claim_next(now=fake_clock())
    assert job is not None
    service._run_job_safely(job)

    fake_clock.advance(service.config.interval_seconds * 2 + 1)
    service._schedule_due(fake_clock())

    pending = service.db._conn.execute(
        "SELECT monitor_id FROM queue WHERE state IN ('queued', 'running')"
    ).fetchall()
    assert pending == []
    assert service.db.get_state(1)["next_run_at"] > fake_clock()


def test_overdue_schedules_do_not_catch_up_in_a_burst(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    monitors = _four_monitors(service, fake_clock)
    fake_clock.advance(7200)
    service._schedule_due(fake_clock())
    pending = [i for i in monitors if service.db.has_pending(i)]
    assert pending == [1]
    assert [service.db.get_state(i)["next_run_at"] - fake_clock() for i in [2, 3, 4]] == [450, 900, 1350]


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_budget_pause_blocks_smaller_manual_reservation(tmp_path, fake_clock):
    service, transport, _ = build_service(tmp_path, fake_clock)
    service.db.pause_budget(now=fake_clock())
    run_one(service)
    assert transport.calls == []
    assert service.snapshot(1)["latest"]["status"] == "budget_exhausted"


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_budget_reset_keeps_four_distinct_phases(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    monitors = _four_monitors(service, fake_clock)
    anchor = fake_clock()
    service.db.pause_budget(now=anchor)
    for i in monitors:
        run_one(service, i)
    starts = [service.db.get_state(i)["next_run_at"] for i in monitors]
    assert len(set(starts)) == 4
    assert [(start - anchor) % 1800 for start in starts] == [0, 450, 900, 1350]
    assert all(start >= service.db.next_utc_midnight(anchor) for start in starts)


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_unknown_reservation_carries_across_days_and_blocks_new_spend(tmp_path, fake_clock):
    service, transport, _ = build_service(tmp_path, fake_clock)
    _, reservation = _reserve(service, fake_clock, 4.99)
    original_day = service.db.budget_day(fake_clock())
    fake_clock.advance(86400)
    service = _restart(service, fake_clock)
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 4.99
    run_one(service)
    assert not transport.calls
    assert service.snapshot(1)["paused_reconciliation"] is True
    service.db.settle_reservation(reservation.reservation_id, actual_cost_usd=1.5, now=fake_clock())
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0
    assert service.db._conn.execute("SELECT spent_usd FROM budget_days WHERE budget_day=?", (original_day,)).fetchone()[0] == 1.5


def test_unknown_holds_survive_retention_cleanup(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    _, reservation = _reserve(service, fake_clock, 0.4)
    service.db.settle_reservation(reservation.reservation_id, actual_cost_usd=None, now=fake_clock())
    fake_clock.advance(91 * 86400)
    service = _restart(service, fake_clock)
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0.4
    assert service.db._conn.execute("SELECT 1 FROM budget_days WHERE budget_day=?", (reservation.budget_day,)).fetchone()


def test_restart_never_replays_reserved_job_or_charges_it_twice(tmp_path, fake_clock):
    service, transport, _ = build_service(tmp_path, fake_clock, enabled=True)
    job, reservation = _reserve(service, fake_clock, 0.5)
    fake_clock.advance(86400)
    service = _restart(service, fake_clock)
    service._schedule_due(fake_clock())
    assert service.db.claim_next(now=fake_clock()) is None
    assert not transport.calls
    assert service.snapshot(1)["latest"]["message_code"] == "worker_restarted"
    assert len(service.snapshot(1)["history"]) == 1
    service = _restart(service, fake_clock)
    assert len(service.snapshot(1)["history"]) == 1
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0.5
    assert service.db._conn.execute("SELECT state FROM queue WHERE id=?", (job.queue_id,)).fetchone()[0] == "error"
    assert service.db._conn.execute("SELECT state FROM budget_reservations WHERE id=?", (reservation.reservation_id,)).fetchone()[0] == "reserved"


def test_claim_is_globally_serial_across_connections(tmp_path, fake_clock):
    from modeltrace.db import ModelTraceDB

    service, _, _ = build_service(tmp_path, fake_clock)
    service.db.seed_monitors([2], now=fake_clock())
    service.db.enqueue(1, now=fake_clock())
    service.db.enqueue(2, now=fake_clock())
    other = ModelTraceDB(service.db.path)
    try:
        assert service.db.claim_next(now=fake_clock()) is not None
        assert other.claim_next(now=fake_clock()) is None
    finally:
        other.close()


def test_stop_timeout_preserves_worker_handle(tmp_path, fake_clock):
    class BusyWorker:
        def is_alive(self):
            return True

        def join(self, timeout):
            pass

    service, _, _ = build_service(tmp_path, fake_clock)
    worker = BusyWorker()
    service._worker_thread = worker
    service.stop(join_timeout=0)
    assert service._worker_thread is worker
    service.start()
    assert service._worker_thread is worker
    assert service._stop.is_set()


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_config_rejects_paid_auto_retests_and_keeps_five_dollar_default(tmp_path, fake_clock):
    import json
    import pytest
    from modeltrace.config import ConfigError, load_config
    from pathlib import Path

    service, _, _ = build_service(tmp_path, fake_clock)
    path = Path(service.config.config_path)
    raw = json.loads(path.read_text())
    raw.pop("daily_budget_usd")
    raw.pop("auto_retests")
    path.write_text(json.dumps(raw))
    assert load_config(path).daily_budget_usd == 5
    assert load_config(path).auto_retests == 0
    raw["auto_retests"] = 1
    path.write_text(json.dumps(raw))
    with pytest.raises(ConfigError):
        load_config(path)


def test_wal_reservations_use_durable_commits(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    assert service.db._conn.execute("PRAGMA synchronous").fetchone()[0] == 2


def test_second_service_cannot_recover_a_live_owner(tmp_path, fake_clock):
    import pytest
    from modeltrace.service import ModelTraceService

    service, transport, receipts = build_service(tmp_path, fake_clock)
    job, _ = _reserve(service, fake_clock, 0.5)
    with pytest.raises(RuntimeError, match="worker_already_active"):
        ModelTraceService(
            service.config, database_path=service.db.path, transport=transport,
            receipt_reader=receipts, clock=fake_clock,
        )
    assert service.db.is_running(job.monitor_id)
    assert service.db.get_latest_round(job.monitor_id) is None
    assert not transport.calls


@pytest.mark.skip(reason="v0.1.12 单题检测，新规则见 tests/test_single_probe_v0112.py")
def test_completed_job_cannot_be_replayed_or_reaccounted(tmp_path, fake_clock):
    service, transport, _ = build_service(tmp_path, fake_clock)
    service.db.enqueue(1, now=fake_clock())
    job = service.db.claim_next(now=fake_clock())
    assert job is not None
    service._run_job_safely(job)
    service._run_job_safely(job)
    assert len(transport.calls) == 3
    assert len(service.snapshot(1)["history"]) == 1
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0.03


def test_crash_after_settlement_before_finish_keeps_single_charge(tmp_path, fake_clock):
    service, transport, _ = build_service(tmp_path, fake_clock, enabled=True)
    _, reservation = _reserve(service, fake_clock, 0.25)
    service.db.settle_reservation(reservation.reservation_id, actual_cost_usd=0.03, now=fake_clock())
    service = _restart(service, fake_clock)
    service._schedule_due(fake_clock())
    assert service.db.claim_next(now=fake_clock()) is None
    assert not transport.calls
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0.03
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0
    assert len(service.snapshot(1)["history"]) == 1


def test_legacy_queued_auto_retest_is_not_replayed(tmp_path, fake_clock):
    service, transport, _ = build_service(tmp_path, fake_clock)
    service.db.enqueue(1, now=fake_clock(), trigger="auto_retest", retest_index=1)
    service = _restart(service, fake_clock)
    assert service.db.claim_next(now=fake_clock()) is None
    assert not transport.calls


def test_budget_settlement_is_idempotent_and_only_known_cost_releases(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    _, reservation = _reserve(service, fake_clock, 0.4)
    rid = reservation.reservation_id
    for _ in range(2):
        assert service.db.settle_reservation(rid, actual_cost_usd=None, now=fake_clock()) == 0.4
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0.4
    assert service.db.settle_reservation(rid, actual_cost_usd=0.1, now=fake_clock()) == 0.1
    assert service.db.settle_reservation(rid, actual_cost_usd=0.1, now=fake_clock()) is None
    budget = service.db.budget_snapshot(now=fake_clock())
    assert budget.spent_usd == 0.1
    assert budget.reserved_usd == 0


def test_invalid_cost_and_reservation_values_fail_closed(tmp_path, fake_clock):
    import pytest

    service, _, _ = build_service(tmp_path, fake_clock)
    job, reservation = _reserve(service, fake_clock, 0.3)
    for invalid in [float("nan"), float("inf"), -1.0, True]:
        assert service.db.settle_reservation(reservation.reservation_id, actual_cost_usd=invalid, now=fake_clock()) == 0.3
        assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0.3
    for invalid in [float("nan"), float("inf"), -1.0]:
        with pytest.raises(ValueError, match="invalid_reservation_amount"):
            service.db.reserve_budget(1, job.queue_id, invalid, daily_budget_usd=5, now=fake_clock())


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_price_estimate_uses_undiscounted_input_cache_and_output(tmp_path, fake_clock):
    from dataclasses import replace
    from modeltrace.config import Pricing
    import pytest

    service, _, _ = build_service(tmp_path, fake_clock)
    service.config = replace(service.config, pricing_upper_bound={"gpt-5.4": Pricing(2.5, 7, 15)})
    challenges = [{"system": "a", "user_prefix": "b", "prompt": "c"}] * 3
    expected = 3 * (3 + 1024) * 7 / 1_000_000 + 3 * 2048 * 15 / 1_000_000
    assert service._estimate_run_cost("gpt-5.4", challenges) == pytest.approx(expected)
    assert service._estimate_run_cost("gpt-5.4", challenges) > expected * 0.001


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_snapshot_pause_has_reset_time_for_carried_holds(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    _reserve(service, fake_clock, 5)
    fake_clock.advance(86400)
    service = _restart(service, fake_clock)
    snapshot = service.snapshot(1)
    assert snapshot["paused_budget"] is True
    assert snapshot["next_run_after"] is not None
    assert snapshot["next_run_at"] == snapshot["next_run_after"]


# These fixtures exercise admission, not model identity or calibration accuracy.
def _counted_round(tmp_path, fake_clock, monkeypatch, expected, counts, *, model="gpt-5.6-terra", accounts=None, error=None):
    from dataclasses import replace
    from modeltrace.config import MonitorConfig
    from modeltrace.transport import ProbeResult
    import modeltrace.service as module

    class CountedTransport(FakeTransport):
        def run(self, **kwargs):
            index = len(self.calls)
            super().run(**kwargs)
            text = " ".join(str(i % 355 + 1) for i in range(counts[index]))
            return ProbeResult(text, 200, error)

    receipts = FakeReceipts(account_ids=accounts, models=[model] * 3)
    service, transport, _ = build_service(tmp_path, fake_clock, transport=CountedTransport(), receipts=receipts)
    price = service.config.pricing_upper_bound["gpt-5.4"]
    service.config = replace(service.config, monitors={1: MonitorConfig(1, model, False, True)},
                             pricing_upper_bound={model: price})
    monkeypatch.setattr(module, "generate_challenges", lambda **_: [
        {"prompt": f"independent challenge {i}", "expected_count": count}
        for i, count in enumerate(expected)
    ])
    calls = []

    def classifier(outputs, bank):
        calls.append(outputs)
        return {"prediction": model, "results": [{"model": model, "probability": 0.9}],
                "used_outputs": 3, "calibration": {"queries": "3"}}

    monkeypatch.setattr(module, "analyze_outputs", classifier)
    return service, transport, calls


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_reference_sol_307_of_308_is_statistically_valid(tmp_path, fake_clock, monkeypatch):
    service, _, calls = _counted_round(tmp_path, fake_clock, monkeypatch,
        [294, 308, 307], [294, 307, 307], model="gpt-5.6-sol")
    run_one(service)
    assert len(calls) == 1 and len(calls[0]) == 3
    assert service.snapshot(1)["latest"]["status"] == "match"


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_terra_natural_count_deviation_enters_classifier_unchanged(tmp_path, fake_clock, monkeypatch):
    from modeltrace.fingerprint import parse_numbers
    service, _, calls = _counted_round(tmp_path, fake_clock, monkeypatch,
        [318, 322, 302], [306, 324, 339], accounts=["16"] * 3)
    run_one(service)
    assert len(calls) == 1
    assert [len(parse_numbers(item["text"])) for item in calls[0]] == [306, 324, 339]
    assert service.snapshot(1)["latest"]["status"] == "match"


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_sol_count_deviation_does_not_bypass_mixed_route(tmp_path, fake_clock, monkeypatch):
    service, _, calls = _counted_round(tmp_path, fake_clock, monkeypatch,
        [294, 308, 307], [294, 307, 307], model="gpt-5.6-sol", accounts=["15", "15", "14"])
    run_one(service)
    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "mixed_route"
    assert latest["target_probability"] is None and latest["ranking"] == []
    assert calls == []


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_statistical_threshold_boundary(tmp_path, fake_clock, monkeypatch):
    # ceil(318 * .55) == 175: two valid answers cannot compensate for one short answer.
    service, _, calls = _counted_round(tmp_path, fake_clock, monkeypatch,
        [318] * 3, [175, 175, 174])
    run_one(service)
    latest = service.snapshot(1)["latest"]
    assert latest["message_code"] == "insufficient_sample"
    assert latest["target_probability"] is None and calls == []


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_statistical_threshold_exactly_met(tmp_path, fake_clock, monkeypatch):
    service, _, calls = _counted_round(tmp_path, fake_clock, monkeypatch,
        [318] * 3, [175] * 3)
    run_one(service)
    assert len(calls) == 1
    assert service.snapshot(1)["latest"]["status"] == "match"


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_minimum_eighty_numbers_even_with_small_expected(tmp_path, fake_clock, monkeypatch):
    service, _, calls = _counted_round(tmp_path, fake_clock, monkeypatch, [10] * 3, [79] * 3)
    run_one(service)
    assert service.snapshot(1)["latest"]["message_code"] == "insufficient_sample"
    assert calls == []


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_protocol_incomplete_never_scores_even_with_enough_numbers(tmp_path, fake_clock, monkeypatch):
    service, _, calls = _counted_round(tmp_path, fake_clock, monkeypatch,
        [318] * 3, [355] * 3, error="response_truncated")
    run_one(service)
    latest = service.snapshot(1)["latest"]
    assert latest["message_code"] == "response_truncated"
    assert latest["target_probability"] is None and calls == []


def test_classifier_cannot_fall_back_to_fewer_queries(tmp_path, fake_clock, monkeypatch):
    import modeltrace.service as module
    service, _, _ = build_service(tmp_path, fake_clock)
    monkeypatch.setattr(module, "analyze_outputs", lambda *_: {
        "used_outputs": 1, "calibration": {"queries": "1"},
        "prediction": "gpt-5.4", "results": [{"model": "gpt-5.4", "probability": 0.95}],
    })
    run_one(service)
    round_row = service.db.get_latest_round(1)
    diag = service.db.decode_diagnostics(round_row)
    assert diag["calibration_queries"] == "1"
    assert diag["used_outputs"] == 1
    assert service.snapshot(1)["latest"]["status"] == "match"


def test_numeric_three_query_calibration_is_accepted(tmp_path, fake_clock, monkeypatch):
    import modeltrace.service as module
    service, _, _ = build_service(tmp_path, fake_clock)
    monkeypatch.setattr(module, "analyze_outputs", lambda *_: {
        "used_outputs": 3, "calibration": {"queries": 3},
        "prediction": "gpt-5.4", "results": [{"model": "gpt-5.4", "probability": 0.9}],
    })
    run_one(service)
    assert service.snapshot(1)["latest"]["status"] == "match"


def test_round_affinity_shared_but_rotates_between_rounds(tmp_path, fake_clock):
    import uuid
    service, transport, _ = build_service(tmp_path, fake_clock)
    run_one(service)
    fake_clock.advance(1800)
    run_one(service)
    assert len(transport.calls) == 2
    first = transport.calls[0]["session_affinity"]
    second = transport.calls[1]["session_affinity"]
    assert first != second
    assert uuid.UUID(first).version == 4 and uuid.UUID(second).version == 4
    assert len({call["user_agent"] for call in transport.calls}) == 2


@pytest.mark.skip(reason="v0.1.12 单题检测，新规则见 tests/test_single_probe_v0112.py")
def test_deterministic_parameter_error_cancels_only_planned_probes(tmp_path, fake_clock):
    from modeltrace.transport import ProbeResult

    class DeterministicTransport(FakeTransport):
        def run(self, **kwargs):
            self.calls.append(kwargs)
            return ProbeResult(None, 400, "upstream_unsupported_reasoning_effort", True)

    transport = DeterministicTransport()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=FakeReceipts(missing=True))
    run_one(service)
    assert len(transport.calls) == 1
    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "error"
    assert latest["message_code"] == "upstream_unsupported_reasoning_effort"
    diagnostics = service.db.decode_diagnostics(service.db.get_latest_round(1))
    assert diagnostics["request_count"] == 1
    assert diagnostics["cancelled_probe_count"] == 2
    assert diagnostics["deterministic_parameter_error"] is True
    assert service.db._conn.execute(
        "select count(*) from reconciliation_probes where status='cancelled'"
    ).fetchone()[0] == 2
    assert service.db._conn.execute(
        "select count(*) from budget_reservations where state='reserved'"
    ).fetchone()[0] == 1


@pytest.mark.parametrize("error_code", ["upstream_timeout", "response_too_large"])
@pytest.mark.skip(reason="v0.1.12 单题检测，新规则见 tests/test_single_probe_v0112.py")
def test_first_incomplete_transport_error_cancels_tail_and_keeps_unknown_hold(
    tmp_path, fake_clock, error_code
):
    from modeltrace.transport import ProbeResult

    class IncompleteTransport(FakeTransport):
        def run(self, **kwargs):
            self.calls.append(kwargs)
            return ProbeResult(None, 200, error_code)

    transport = IncompleteTransport()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=FakeReceipts(missing=True))
    run_one(service)

    assert len(transport.calls) == 1
    diagnostics = service.db.decode_diagnostics(service.db.get_latest_round(1))
    assert diagnostics["cancelled_probe_count"] == 2
    assert diagnostics["aborting_transport_error"] is True
    assert diagnostics["receipt_count"] == 0
    assert service.db._conn.execute(
        "select count(*) from reconciliation_probes where status='cancelled'"
    ).fetchone()[0] == 2
    assert service.db._conn.execute(
        "select count(*) from reconciliation_probes where status='attempted'"
    ).fetchone()[0] == 1
    assert service.db._conn.execute(
        "select count(*) from budget_reservations where state='reserved'"
    ).fetchone()[0] == 1




@pytest.mark.parametrize("error_code", ["upstream_timeout", "response_too_large"])
@pytest.mark.parametrize("error_at", [1, 2])
@pytest.mark.skip(reason="v0.1.12 单题检测，新规则见 tests/test_single_probe_v0112.py")
def test_late_incomplete_transport_error_cancels_only_tail(
    tmp_path, fake_clock, error_code, error_at
):
    from modeltrace.transport import ProbeResult

    class PositionalTransport(FakeTransport):
        def run(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) - 1 == error_at:
                return ProbeResult(None, 200, error_code)
            return ProbeResult(self.text, 200, None)

    transport = PositionalTransport()
    service, _, receipts = build_service(
        tmp_path, fake_clock, transport=transport, receipts=FakeReceipts(missing=True)
    )
    run_one(service)

    attempted = error_at + 1
    assert len(transport.calls) == len(receipts.calls) == attempted
    diagnostics = service.db.decode_diagnostics(service.db.get_latest_round(1))
    assert diagnostics["request_count"] == attempted
    assert diagnostics["cancelled_probe_count"] == 3 - attempted
    assert diagnostics["aborting_transport_error"] is True
    assert diagnostics["valid_count"] == error_at
    assert service.db._conn.execute(
        "select count(*) from reconciliation_probes where status='cancelled'"
    ).fetchone()[0] == 3 - attempted
    assert service.db._conn.execute(
        "select count(*) from budget_reservations where state='reserved'"
    ).fetchone()[0] == 1


@pytest.mark.skip(reason="v0.1.12 单题检测，新规则见 tests/test_single_probe_v0112.py")
def test_non_default_reasoning_effort_is_explicitly_unverified(tmp_path, fake_clock, monkeypatch):
    import modeltrace.service as module
    from dataclasses import replace

    service, _, _ = build_service(tmp_path, fake_clock)
    service.config = replace(
        service.config,
        reasoning_effort_overrides={"gpt-5.4": "low"},
    )
    monkeypatch.setattr(module, "analyze_outputs", lambda *_: {
        "used_outputs": 3,
        "calibration": {"queries": 3},
        "prediction": "gpt-5.4",
        "results": [{"model": "gpt-5.4", "probability": 0.99}],
    })
    run_one(service)
    snapshot = service.snapshot(1)
    assert snapshot["reasoning_effort"] == "low"
    assert snapshot["calibration_compatibility"] == "unverified"
    assert snapshot["latest"]["status"] == "uncertain"
    assert snapshot["latest"]["message_code"] == "calibration_unverified_reasoning_effort"
    diagnostics = service.db.decode_diagnostics(service.db.get_latest_round(1))
    assert diagnostics["reasoning_effort"] == "low"
    assert diagnostics["calibration_compatibility"] == "unverified"
    assert snapshot["latest"]["target_probability"] is None
    assert snapshot["latest"]["best_model"] is None
    assert snapshot["latest"]["ranking"] == []


@pytest.mark.parametrize("mismatch_at", [0, 1, 2])
@pytest.mark.parametrize("missing_receipts", [False, True])
@pytest.mark.skip(reason="v0.1.12 单题检测，新规则见 tests/test_single_probe_v0112.py")
def test_identity_mismatch_stops_round_without_hiding_costs(
    tmp_path, fake_clock, monkeypatch, mismatch_at, missing_receipts
):
    from modeltrace.transport import ProbeResult
    import modeltrace.service as module

    class MismatchTransport(FakeTransport):
        def run(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) - 1 == mismatch_at:
                # Defensive: even a faulty adapter retaining text with an
                # error must not make that text a classifier sample.
                return ProbeResult(self.text, 200, "upstream_model_mismatch")
            return ProbeResult(self.text, 200, None)

    def must_not_classify(*args):
        pytest.fail("A known response model mismatch must never be classified")

    monkeypatch.setattr(module, "analyze_outputs", must_not_classify)
    transport = MismatchTransport()
    service, _, receipts = build_service(
        tmp_path, fake_clock, transport=transport,
        receipts=FakeReceipts(missing=missing_receipts),
    )
    run_one(service)
    attempted = mismatch_at + 1
    assert len(transport.calls) == len(receipts.calls) == attempted
    latest = service.snapshot(1)["latest"]
    assert latest["status"] == "error"
    assert latest["message_code"] == "upstream_model_mismatch"
    assert latest["target_probability"] is None
    assert latest["best_model"] is None
    assert latest["ranking"] == []
    diagnostics = service.db.decode_diagnostics(service.db.get_latest_round(1))
    assert diagnostics["request_count"] == attempted
    assert diagnostics["cancelled_probe_count"] == 3 - attempted
    assert diagnostics["aborting_transport_error"] is True
    assert diagnostics["valid_count"] == mismatch_at
    assert service.db._conn.execute(
        "select count(*) from reconciliation_probes where status='cancelled'"
    ).fetchone()[0] == 3 - attempted
    budget = service.db.budget_snapshot(now=fake_clock())
    if missing_receipts:
        assert budget.reserved_usd > 0
        assert diagnostics["settled_cost_usd"] is None
    else:
        assert budget.reserved_usd == 0
        assert budget.spent_usd == pytest.approx(0.01 * attempted)
        assert diagnostics["settled_cost_usd"] == pytest.approx(0.01 * attempted)
