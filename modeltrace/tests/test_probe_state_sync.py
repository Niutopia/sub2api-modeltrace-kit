"""State projections must describe their own queue, not history or rotation."""

import pytest

from modeltrace.service import utc_iso
from tests.test_per_account_d2 import MockHostClient, make_service


@pytest.fixture
def service(tmp_path, fake_clock, monkeypatch):
    models = ["gpt-5.6-sol", "gpt-5.4"]
    host = MockHostClient([{
        "account_id": 40, "name": "test-account", "platform": "openai",
        "schedulable": True, "models": models, "real_models_10m": models,
        "last_real_request_at": utc_iso(fake_clock()),
    }])
    monkeypatch.setattr("modeltrace.service.HostClient", lambda *a, **kw: host)
    svc = make_service(tmp_path, fake_clock, host_client=host)
    yield svc
    svc.db.close()


def test_queued_next_model_is_job_model_not_advanced_rotation(service, fake_clock):
    service.db.set_account_next_run(40, fake_clock(), now=fake_clock())
    service._schedule_due_accounts(fake_clock())
    job = service.db._conn.execute(
        "SELECT model FROM account_queue WHERE account_id = 40 AND state = 'queued'"
    ).fetchone()
    snapshot = service.account_snapshot(40)
    assert snapshot["queued"] is True
    assert snapshot["running"] is False
    assert snapshot["next_model"] == job["model"]


def test_running_is_not_also_queued_and_keeps_executing_model(service, fake_clock):
    service.db.set_account_next_run(40, fake_clock(), now=fake_clock())
    service._schedule_due_accounts(fake_clock())
    job = service.db.claim_next_account(now=fake_clock())
    snapshot = service.account_snapshot(40)
    assert snapshot["running"] is True
    assert snapshot["queued"] is False
    assert snapshot["next_model"] == job.model
    assert service.db.has_pending_account(40) is True


def test_per_account_mode_does_not_advertise_old_monitor_schedule(service, fake_clock):
    old = fake_clock() - 3600
    service.db._conn.execute(
        "UPDATE monitor_state SET next_run_at = ? WHERE monitor_id = 1", (old,),
    )
    snapshot = service.snapshot(1)
    # The channel follows the account that will check this model next.
    assert snapshot["next_run_at"] != utc_iso(old)
    account = service.account_snapshot(40)
    assert account["next_model"] == "gpt-5.6-sol"
    assert snapshot["next_run_at"] == account["next_run_at"]
    assert snapshot["schedule_mode"] == "scheduled"


def test_monitor_reports_remote_queued_before_claim(service, fake_clock):
    service.db.enqueue(1, now=fake_clock(), trigger="manual")
    snapshot = service.snapshot(1)
    assert snapshot["queued"] is True
    assert snapshot["running"] is False
    service.db.claim_next(now=fake_clock())
    snapshot = service.snapshot(1)
    assert snapshot["queued"] is False
    assert snapshot["running"] is True


@pytest.fixture
def other_db(service):
    from modeltrace.db import ModelTraceDB

    db = ModelTraceDB(service.db.path)
    yield db
    db.close()


def test_read_snapshot_pins_committed_state_across_connections(service, other_db, fake_clock):
    original = service.db.get_account(40)["next_run_at"]
    changed = fake_clock() + 999
    with service.db.read_snapshot():
        assert service.db.get_account(40)["next_run_at"] == original
        other_db.set_account_next_run(40, changed, now=fake_clock())
        assert service.db.get_account(40)["next_run_at"] == original
    assert service.db.get_account(40)["next_run_at"] == changed


def test_nested_snapshot_keeps_outer_read_view(service, other_db, fake_clock):
    original = service.db.get_account(40)["next_run_at"]
    with service.db.read_snapshot():
        assert service.db.get_account(40)["next_run_at"] == original
        with service.db.read_snapshot():
            other_db.set_account_next_run(40, fake_clock() + 999, now=fake_clock())
            assert service.db.get_account(40)["next_run_at"] == original
        assert service.db.get_account(40)["next_run_at"] == original
    assert not service.db._conn.in_transaction


def test_snapshot_exception_releases_transaction(service, other_db, fake_clock):
    with pytest.raises(RuntimeError, match="interrupted"):
        with service.db.read_snapshot():
            service.db.get_account(40)
            raise RuntimeError("interrupted")
    assert not service.db._conn.in_transaction
    other_db.set_account_next_run(40, fake_clock() + 999, now=fake_clock())
    assert service.db.get_account(40)["next_run_at"] == fake_clock() + 999


def test_account_snapshot_cannot_mix_old_history_with_finished_queue(
    service, other_db, fake_clock, monkeypatch,
):
    service.db.enqueue_account(40, "gpt-5.6-sol", now=fake_clock())
    job = other_db.claim_next_account(now=fake_clock())
    original = service.db.get_account_rounds

    def read_then_complete(*args, **kwargs):
        rows = original(*args, **kwargs)
        other_db.record_account_round_and_finish(
            job, status="error", target_probability=None, best_model=None,
            checked_at=fake_clock(), message_code="upstream_error",
            reasoning_effort=None, ranking=[], diagnostics={},
            next_run_at=fake_clock() + 300,
        )
        return rows

    monkeypatch.setattr(service.db, "get_account_rounds", read_then_complete)
    snapshot = service.account_snapshot(40)
    assert snapshot["latest"] is None
    assert snapshot["running"] is True
    monkeypatch.setattr(service.db, "get_account_rounds", original)
    refreshed = service.account_snapshot(40)
    assert refreshed["running"] is False
    assert refreshed["latest"]["status"] == "error"


def test_snapshot_starts_read_view_before_first_projection_query(service, other_db, fake_clock):
    original = service.db.get_account(40)["next_run_at"]
    with service.db.read_snapshot():
        other_db.set_account_next_run(40, fake_clock() + 999, now=fake_clock())
        assert service.db.get_account(40)["next_run_at"] == original


def test_snapshot_does_not_finish_callers_transaction(service, fake_clock):
    original = service.db.get_account(40)["next_run_at"]
    service.db._conn.execute("BEGIN IMMEDIATE")
    try:
        service.db.set_account_next_run(40, fake_clock() + 999, now=fake_clock())
        with service.db.read_snapshot():
            assert service.db.get_account(40)["next_run_at"] == fake_clock() + 999
        assert service.db._conn.in_transaction
    finally:
        service.db._conn.execute("ROLLBACK")
    assert service.db.get_account(40)["next_run_at"] == original


@pytest.mark.parametrize("disable", ["global", "per_account", "unschedulable", "no_models", "retired"])
def test_inactive_account_schedule_has_no_forecast(service, disable):
    from dataclasses import replace

    if disable == "global":
        service.config = replace(service.config, enabled=False)
    elif disable == "per_account":
        service.config = replace(service.config, per_account_enabled=False)
    elif disable == "unschedulable":
        service.db._conn.execute("UPDATE accounts SET schedulable = 0 WHERE account_id = 40")
    elif disable == "no_models":
        service.db._conn.execute("UPDATE accounts SET models_json = '[]' WHERE account_id = 40")
    else:
        service.db.retire_account(40, now=service.clock())
    snapshot = service.account_snapshot(40)
    assert snapshot["next_run_at"] is None
    assert snapshot["next_model"] is None
    assert snapshot["schedule_mode"] == "manual"
    assert snapshot["latest"] is None


@pytest.mark.parametrize("scope", ["monitor", "account"])
@pytest.mark.parametrize("enabled,trigger,executable", [(True, "scheduled", True), (False, "scheduled", False), (False, "manual", True)])
def test_delayed_pending_time_belongs_to_actual_job(service, fake_clock, scope, enabled, trigger, executable):
    from dataclasses import replace

    service.config = replace(service.config, enabled=enabled)
    now = fake_clock()
    if scope == "monitor":
        service.db.set_next_run(1, now + 999, now=now)
        service.db.enqueue(1, now=now, available_at=now + 60, trigger=trigger)
        read = lambda: service.snapshot(1)
        claim = service.db.claim_next
    else:
        service.db.set_account_next_run(40, now + 999, now=now)
        service.db.enqueue_account(40, "gpt-5.6-sol", now=now, available_at=now + 60, trigger=trigger)
        read = lambda: service.account_snapshot(40)
        claim = service.db.claim_next_account
    snapshot = read()
    assert snapshot["queued"] is True
    assert snapshot["running"] is False
    assert snapshot["next_run_at"] == (utc_iso(now + 60) if executable else None)
    assert snapshot["queued_at"] == utc_iso(now)
    assert snapshot["available_at"] == utc_iso(now + 60)
    assert snapshot["started_at"] is None
    assert claim(now=now, allow_scheduled=enabled) is None
    fake_clock.advance(65)
    if not executable:
        assert claim(now=fake_clock(), allow_scheduled=enabled) is None
        return
    assert claim(now=fake_clock(), allow_scheduled=enabled) is not None
    running = read()
    assert running["running"] is True
    assert running["queued"] is False
    assert running["next_run_at"] is None
    assert running["started_at"] == utc_iso(fake_clock())
    assert running["queued_at"] == snapshot["queued_at"]


def test_model_whitelist_not_narrowed_to_active_or_monitor_models(service, fake_clock):
    host_account = service.host_client.accounts_data[0]
    host_account["models"] = ["gpt-6-astra", "gpt-5.6-luna", "no-baseline"]
    host_account["real_models_10m"] = ["gpt-6-astra"]
    service._refresh_accounts(fake_clock())
    snapshot = service.account_snapshot(40)
    assert snapshot["models"] == ["gpt-6-astra", "gpt-5.6-luna"]
    assert [p["model"] for p in snapshot["per_model"]] == snapshot["models"]
    assert snapshot["next_model"] == "gpt-6-astra"
    assert {"gpt-6-astra", "gpt-5.6-luna"} <= set(service.host_client.fetch_calls[-1])
    result = service.enqueue_manual_account(40, model="gpt-5.6-luna")
    assert result.model == "gpt-5.6-luna"
    assert service.account_snapshot(40)["next_model"] == "gpt-5.6-luna"
    assert not service.transport.calls


def test_alias_route_keeps_own_identity_and_baseline(service, fake_clock):
    from dataclasses import replace

    service.config = replace(service.config, model_aliases={"luna-route": "gpt-5.6-luna"})
    service.host_client.accounts_data[0]["models"] = ["gpt-6-astra", "luna-route", "no-baseline"]
    service._refresh_accounts(fake_clock())
    snapshot = service.account_snapshot(40)
    assert snapshot["models"] == ["gpt-6-astra", "luna-route"]
    assert snapshot["per_model"][1]["calibration_model"] == "gpt-5.6-luna"
    assert service.enqueue_manual_account(40, model="luna-route").model == "luna-route"


def test_rare_model_history_not_lost_behind_other_models(service, fake_clock):
    service.db.insert_account_round(account_id=40, model="gpt-5.4", status="suspect",
                                    message_code="other_model", checked_at=fake_clock() - 200)
    for i in range(101):
        service.db.insert_account_round(account_id=40, model="gpt-5.6-sol", status="error",
                                        message_code="upstream_error", checked_at=fake_clock() - 100 + i)
    snapshot = service.account_snapshot(40)
    rare = next(p for p in snapshot["per_model"] if p["model"] == "gpt-5.4")
    assert rare["latest"] is not None
    assert rare["latest"]["status"] == "suspect"
    assert len(rare["history"]) == 1
    assert service.accounts_summary_for_model("gpt-5.4")["suspect"] == 1
    assert len(snapshot["history"]) == 12


def test_late_inserted_old_round_cannot_override_newer_summary(service, fake_clock):
    service.db.insert_account_round(account_id=40, model="gpt-5.4", status="error",
                                    message_code="upstream_error", checked_at=fake_clock())
    service.db.insert_account_round(account_id=40, model="gpt-5.4", status="match",
                                    message_code="ok", checked_at=fake_clock() - 1)
    assert service.account_snapshot(40)["latest"]["status"] == "error"
    summary = service.accounts_summary_for_model("gpt-5.4")
    assert summary["error"] == 1
    assert summary["match"] == 0


def test_list_snapshot_cannot_mix_retirement_with_old_target_list(service, other_db, fake_clock, monkeypatch):
    original = service._get_all_targets

    def read_then_retire():
        targets = original()
        other_db.retire_account(40, now=fake_clock())
        return targets

    monkeypatch.setattr(service, "_get_all_targets", read_then_retire)
    snapshot = service.all_accounts_snapshot()
    assert [a["mode"] for a in snapshot["accounts"]] == ["active"]
    monkeypatch.setattr(service, "_get_all_targets", original)
    assert service.all_accounts_snapshot()["accounts"] == []


def test_idle_snapshots_expose_capture_time_without_manufacturing_results(service, fake_clock):
    from datetime import datetime

    for read in (lambda: service.snapshot(1), lambda: service.account_snapshot(40)):
        first = read()
        fake_clock.advance(0.001)
        second = read()
        assert datetime.fromisoformat(first["snapshot_at"]) < datetime.fromisoformat(second["snapshot_at"])
        assert first["latest"] == second["latest"]
        assert second["queued_at"] is None
        assert second["available_at"] is None
        assert second["started_at"] is None
    assert not service.transport.calls


def test_read_snapshot_is_consistent_with_separate_process_writer(service, fake_clock):
    import subprocess
    import sys

    original = service.db.get_account(40)["next_run_at"]
    new_time = fake_clock() + 999
    with service.db.read_snapshot():
        subprocess.run(
            [sys.executable, "-c",
             "import sqlite3, sys; c = sqlite3.connect(sys.argv[1]); "
             "c.execute('UPDATE accounts SET next_run_at = ? WHERE account_id = 40', "
             "(float(sys.argv[2]),)); c.commit(); c.close()",
             service.db.path, str(new_time)],
            check=True, capture_output=True, timeout=10,
        )
        assert service.db.get_account(40)["next_run_at"] == original
    assert service.db.get_account(40)["next_run_at"] == new_time


def test_monitor_snapshot_cannot_mix_old_state_and_new_round(service, other_db, fake_clock, monkeypatch):
    service.db.enqueue(1, now=fake_clock(), trigger="manual")
    job = other_db.claim_next(now=fake_clock())
    original = service.db.get_state

    def read_then_complete(*args, **kwargs):
        state = original(*args, **kwargs)
        other_db.record_round_and_finish(
            job, status="error", target_probability=None, best_model=None,
            checked_at=fake_clock(), message_code="upstream_error",
            ranking=[], diagnostics={}, next_run_at=fake_clock() + 3600,
            next_run_after=None, upstream_statuses=[503], receipt_count=0,
            receipt_consistent=None, actual_cost_usd=None, reserved_usd=None,
        )
        return state

    monkeypatch.setattr(service.db, "get_state", read_then_complete)
    snapshot = service.snapshot(1, admin=True)
    assert snapshot["latest"]["status"] == "not_tested"
    assert snapshot["last_checked_at"] is None
    assert snapshot["running"] is True
    assert snapshot["diagnostics"]["queue_pending"] is True
    monkeypatch.setattr(service.db, "get_state", original)
    refreshed = service.snapshot(1, admin=True)
    assert refreshed["latest"]["status"] == "error"
    assert refreshed["last_checked_at"] == refreshed["latest"]["checked_at"]
    assert refreshed["diagnostics"]["queue_pending"] is False
    assert refreshed["running"] is False


def test_summary_reads_members_and_results_from_same_revision(service, other_db, fake_clock, monkeypatch):
    original = service._get_all_targets

    def read_then_record():
        targets = original()
        other_db.insert_account_round(account_id=40, model="gpt-5.4", status="error",
                                      message_code="upstream_error", checked_at=fake_clock())
        return targets

    monkeypatch.setattr(service, "_get_all_targets", read_then_record)
    summary = service.accounts_summary_for_model("gpt-5.4")
    assert summary["total"] == 1
    assert summary["error"] == 0
    monkeypatch.setattr(service, "_get_all_targets", original)
    assert service.accounts_summary_for_model("gpt-5.4")["error"] == 1


def test_pause_expiry_does_not_change_fingerprint_results(service, fake_clock):
    import json

    until = utc_iso(fake_clock() + 60)
    service.db._conn.execute(
        "UPDATE accounts SET paused_models_json = ? WHERE account_id = 40",
        (json.dumps([{"model": "gpt-5.4", "until": until}]),),
    )
    service.db.insert_account_round(account_id=40, model="gpt-5.4", status="suspect",
                                    message_code="other_model", checked_at=fake_clock() - 1)
    snapshot = service.account_snapshot(40)
    paused = next(p for p in snapshot["per_model"] if p["model"] == "gpt-5.4")
    assert paused["paused_until"] == until
    assert snapshot["next_model"] == "gpt-5.4"  # Existing API-key recheck policy.
    assert paused["latest"]["status"] == "suspect"
    fake_clock.advance(61)
    refreshed = service.account_snapshot(40)
    unpaused = next(p for p in refreshed["per_model"] if p["model"] == "gpt-5.4")
    assert unpaused["paused_until"] is None
    assert unpaused["latest"] == paused["latest"]
    assert service.accounts_summary_for_model("gpt-5.4")["suspect"] == 1
    assert not service.transport.calls


def test_no_active_model_uses_whitelist_without_mutating_rotation(service):
    service.db._conn.execute("UPDATE accounts SET real_models_json = '[]', last_model_index = 1 WHERE account_id = 40")
    first = service.account_snapshot(40)
    second = service.account_snapshot(40)
    assert first["models"] == ["gpt-5.6-sol", "gpt-5.4"]
    assert first["next_model"] == second["next_model"] == "gpt-5.4"
    assert service.db.get_account(40)["last_model_index"] == 1
    assert not service.db.has_pending_account(40)


@pytest.mark.parametrize("reason", ["disabled", "unsupported", "no_baseline"])
def test_monitor_forecast_respects_enablement_and_baseline_support(service, reason):
    from dataclasses import replace

    monitor = service.config.monitors[1]
    if reason == "disabled":
        monitor = replace(monitor, enabled=False)
    elif reason == "unsupported":
        monitor = replace(monitor, configured_supported=False)
    else:
        monitor = replace(monitor, model="no-baseline")
    service.config = replace(service.config, per_account_enabled=False, monitors={1: monitor})
    snapshot = service.snapshot(1)
    assert snapshot["schedule_mode"] == "manual"
    assert snapshot["next_run_at"] is None
    assert snapshot["supported"] is (reason == "disabled")


def test_collection_uses_one_capture_time_for_every_account(service):
    snapshot = service.all_accounts_snapshot()
    assert snapshot["accounts"]
    assert all(s["snapshot_at"] == snapshot["snapshot_at"] for s in snapshot["accounts"])


def test_latest_round_ties_use_id_without_changing_model_filter(service, fake_clock):
    for model, status in [("gpt-5.4", "match"), ("gpt-5.4", "error"), ("gpt-5.6-sol", "uncertain")]:
        service.db.insert_account_round(account_id=40, model=model, status=status,
                                        message_code="test", checked_at=fake_clock())
    assert service.db.get_latest_rounds_for_all_accounts()[40]["status"] == "uncertain"
    assert service.db.get_latest_rounds_for_all_accounts("gpt-5.4")[40]["status"] == "error"
    assert service.accounts_summary_for_model("gpt-5.4")["error"] == 1
