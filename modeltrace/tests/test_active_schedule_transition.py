"""Offline active-cadence, target handoff, and actual probe-count regressions."""
import socket

import pytest

from modeltrace.service import DuplicateQueue, NotParticipating, utc_iso
from modeltrace.transport import ProbeResult
from tests.test_per_account_d2 import MockHostClient, MockProbeTransport, make_service

MODEL = "gpt-6-astra"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("schedule regression tests must not access the network")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)


@pytest.fixture
def setup_target(tmp_path, fake_clock, monkeypatch):
    services = []

    def make(*, mode="idle", age=60, due_in=2400, clustered=False, schedulable=True,
             transport=None, config_overrides=None):
        ids = [42, 43] if clustered else [42]
        host = MockHostClient([
            {"account_id": aid, "name": str(aid), "platform": "openai",
             "type": "apikey" if clustered else "oauth", "schedulable": schedulable,
             "models": [MODEL], "last_real_request_at": utc_iso(fake_clock()),
             "real_models_10m": [MODEL],
             "cluster_id": "active-pool" if clustered else None}
            for aid in ids
        ])
        # The service refreshes during construction, before make_service replaces
        # host_client. Stub its constructor too, so startup cannot attempt HTTP.
        monkeypatch.setattr("modeltrace.service.HostClient", lambda *args, **kwargs: host)
        svc = make_service(tmp_path, fake_clock, host_client=host, transport=transport,
                           config_overrides=config_overrides)
        services.append(svc)
        svc._refresh_accounts(fake_clock())
        for aid in ids:
            svc.db._conn.execute(
                "UPDATE accounts SET mode=?, interval_seconds=?, next_run_at=? WHERE account_id=?",
                (mode, 300 if mode == "active" else 3600,
                 fake_clock() + due_in if aid == ids[0] and due_in is not None else None, aid),
            )
        svc.db._conn.commit()
        if age is not None:
            # A cluster result can belong to a fallback key, not its representative.
            svc.db.insert_account_round(
                account_id=ids[-1], model=MODEL, status="match", message_code="compatible",
                checked_at=fake_clock() - age,
            )
        return svc, host

    yield make
    for svc in services:
        svc.db.close()


@pytest.mark.parametrize("mode", ["idle", "active"])
@pytest.mark.parametrize("age", [0, 60, 299, 299.999, 300, 300.001, 301, 1800, None])
def test_active_deadline_is_bounded_by_last_round(setup_target, fake_clock, mode, age):
    svc, _ = setup_target(mode=mode, age=age)
    now = fake_clock()
    svc._refresh_accounts(now)
    expected = now if age is None else max(now, now - age + 300)
    acc = svc.db.get_account(42)
    assert acc["mode"] == "active"
    assert acc["interval_seconds"] == 300
    assert acc["next_run_at"] == pytest.approx(expected, rel=0, abs=0.000001)
    assert svc.account_snapshot(42)["next_run_at"] == utc_iso(expected)

    # Repeated polling must not slide the deadline forward on every refresh.
    fake_clock.advance(30)
    svc._refresh_accounts(fake_clock())
    assert svc.db.get_account(42)["next_run_at"] == pytest.approx(expected, rel=0, abs=0.000001)


@pytest.mark.parametrize("mode", ["idle", "active"])
def test_cluster_uses_latest_member_round_and_one_deadline(setup_target, fake_clock, mode):
    svc, _ = setup_target(mode=mode, clustered=True)
    svc._refresh_accounts(fake_clock())
    due = fake_clock() + 240
    assert svc.db.get_account(42)["next_run_at"] == pytest.approx(due, rel=0, abs=0.000001)
    assert svc.db.get_account(43)["next_run_at"] is None
    svc._schedule_due_accounts(fake_clock())
    assert not svc.db.has_pending_account(42)
    fake_clock.value = due
    svc._schedule_due_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert svc.db.has_pending_account(42)
    assert not svc.db.has_pending_account(43)
    count = svc.db._conn.execute("SELECT COUNT(*) FROM account_queue WHERE state='queued'").fetchone()[0]
    assert count == 1


@pytest.mark.parametrize("mode", ["idle", "active"])
@pytest.mark.parametrize("due_in", [-10, 0, 0.001, 30, 239.999, 240])
def test_active_refresh_preserves_earlier_deadline(setup_target, fake_clock, due_in, mode):
    svc, _ = setup_target(mode=mode, due_in=due_in)
    due = fake_clock() + due_in
    svc._refresh_accounts(fake_clock())
    assert svc.db.get_account(42)["next_run_at"] == pytest.approx(due, rel=0, abs=0.000001)


def test_active_missing_deadline_is_restored(setup_target, fake_clock):
    svc, _ = setup_target(mode="active", due_in=None)
    svc._refresh_accounts(fake_clock())
    assert svc.db.get_account(42)["next_run_at"] == pytest.approx(fake_clock() + 240, rel=0, abs=0.000001)


def test_inactive_account_is_not_rescheduled_or_queued(setup_target, fake_clock):
    svc, _ = setup_target(mode="active", schedulable=False)
    due = svc.db.get_account(42)["next_run_at"]
    svc._refresh_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert svc.db.get_account(42)["next_run_at"] == due
    assert not svc.db.has_pending_account(42)


def test_active_falls_back_to_idle_schedule(setup_target, fake_clock):
    svc, host = setup_target(mode="active")
    host.accounts_data[0]["last_real_request_at"] = utc_iso(fake_clock() - 601)
    host.accounts_data[0]["real_models_10m"] = []
    svc._refresh_accounts(fake_clock())
    acc = svc.db.get_account(42)
    assert acc["mode"] == "idle"
    assert acc["interval_seconds"] == 3600
    assert acc["next_run_at"] == svc._compute_idle_next_run(42, fake_clock())


@pytest.fixture
def matching_transport(monkeypatch):
    """Keep real scheduling/execution/DB paths, but never perform paid probes."""
    monkeypatch.setattr("modeltrace.service.generate_challenges", lambda **kwargs: [
        {"prompt": "offline deterministic scheduling regression", "expected_count": 400},
    ])
    monkeypatch.setattr("modeltrace.service.analyze_outputs", lambda *args, **kwargs: {
        "prediction": MODEL,
        "results": [{"model": MODEL, "probability": 0.99}],
    })
    return MockProbeTransport()


def _pending(svc):
    return svc.db._conn.execute(
        "SELECT * FROM account_queue WHERE state IN ('queued', 'running') ORDER BY id"
    ).fetchall()


def _drain(svc):
    """Actually execute every queued job; queue cardinality alone misses reprobes."""
    jobs = []
    for _ in range(10):
        job = svc.db.claim_next_account(now=svc.clock())
        if job is None:
            return jobs
        jobs.append(job)
        svc._run_account_job(job)
    pytest.fail("unexpected queue growth while draining offline jobs")


def _deadline(svc, account_id, expected):
    assert svc.db.get_account(account_id)["next_run_at"] == pytest.approx(
        expected, rel=0, abs=0.000001,
    )


@pytest.mark.parametrize("interval", [1, 60, 600])
def test_reconciliation_honors_configured_interval(setup_target, fake_clock, interval):
    svc, _ = setup_target(mode="active", age=0.25,
                          config_overrides={"active_interval_seconds": interval})
    svc._refresh_accounts(fake_clock())
    assert svc.db.get_account(42)["interval_seconds"] == interval
    _deadline(svc, 42, fake_clock() - 0.25 + interval)


@pytest.mark.parametrize("clustered", [False, True])
def test_due_boundary_does_not_send_even_one_millisecond_early(
    setup_target, fake_clock, matching_transport, clustered,
):
    svc, _ = setup_target(mode="active", age=0.25, clustered=clustered,
                          transport=matching_transport)
    now = fake_clock()
    due = now + 299.75
    svc._refresh_accounts(now)
    fake_clock.value = due - 0.001
    svc._refresh_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert _drain(svc) == []
    assert matching_transport.calls == []
    _deadline(svc, 42, due)

    fake_clock.value = due
    svc._refresh_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert len(_drain(svc)) == 1
    assert len(matching_transport.calls) == 1
    _deadline(svc, 42, due + 300)


@pytest.mark.parametrize("offset", [-0.001, 0, 0.001])
def test_active_window_boundary_is_inclusive(setup_target, fake_clock, offset):
    svc, host = setup_target(mode="active", age=60)
    origin = fake_clock()
    # utc_iso is second-precision; vary the clock, not the encoded request time.
    fake_clock.value = origin + 600 + offset
    host.accounts_data[0]["last_real_request_at"] = utc_iso(origin)
    svc._refresh_accounts(fake_clock())
    account = svc.db.get_account(42)
    if offset <= 0:
        assert account["mode"] == "active"
        _deadline(svc, 42, fake_clock())
    else:
        assert account["mode"] == "idle"
        _deadline(svc, 42, svc._compute_idle_next_run(42, fake_clock()))


@pytest.mark.parametrize("latest_account", [42, 43])
def test_cluster_uses_checked_time_not_member_order_or_insert_order(
    setup_target, fake_clock, latest_account,
):
    svc, _ = setup_target(mode="active", clustered=True, age=None)
    now = fake_clock()
    # Deliberately insert a backfilled older row last, including on the same key.
    other_account = 43 if latest_account == 42 else 42
    for aid, checked_at in [(latest_account, now - 10.25),
                            (other_account, now - 200),
                            (latest_account, now - 250)]:
        svc.db.insert_account_round(account_id=aid, model=MODEL, status="match",
                                    message_code="compatible", checked_at=checked_at)
    svc._refresh_accounts(now)
    _deadline(svc, 42, now + 289.75)
    assert svc.db.get_account(43)["next_run_at"] is None
    for aid in (42, 43):
        assert svc.account_snapshot(aid)["next_run_at"] == utc_iso(now + 289.75)


@pytest.mark.parametrize("historical_model", ["unknown-historical-model", "gpt-5.6-sol"])
def test_old_model_history_never_reintroduces_unsupported_probe_model(
    setup_target, fake_clock, matching_transport, historical_model,
):
    svc, host = setup_target(mode="active", age=1800, transport=matching_transport)
    now = fake_clock()
    assert historical_model not in host.accounts_data[0]["models"]
    svc.db.insert_account_round(account_id=42, model=historical_model, status="match",
                                message_code="compatible", checked_at=now - 60)
    # Cadence is per target, not per model. Historical evidence may bound cadence
    # but must never re-enter the current supported-model rotation.
    svc._refresh_accounts(now)
    _deadline(svc, 42, now + 240)
    fake_clock.value = now + 240
    svc._schedule_due_accounts(fake_clock())
    jobs = _drain(svc)
    assert [job.model for job in jobs] == [MODEL]
    assert [call["model"] for call in matching_transport.calls] == [MODEL]


@pytest.mark.parametrize("raw_models", [[], ["unknown-unregistered-model"]])
def test_empty_supported_models_never_create_phantom_probe(
    setup_target, fake_clock, matching_transport, raw_models,
):
    svc, host = setup_target(mode="active", age=None, transport=matching_transport)
    host.accounts_data[0]["models"] = raw_models
    host.accounts_data[0]["real_models_10m"] = [MODEL, "unknown-unregistered-model"]
    for _ in range(3):
        svc._refresh_accounts(fake_clock())
        svc._schedule_due_accounts(fake_clock())
        assert _drain(svc) == []
        fake_clock.advance(1)
    assert matching_transport.calls == []
    assert _pending(svc) == []


@pytest.mark.parametrize("disabled_flag", ["enabled", "per_account_enabled"])
@pytest.mark.parametrize("clustered", [False, True])
def test_disabled_scheduler_does_not_enqueue_or_send(
    setup_target, fake_clock, matching_transport, disabled_flag, clustered,
):
    svc, _ = setup_target(mode="active", age=None, clustered=clustered,
                          transport=matching_transport,
                          config_overrides={disabled_flag: False})
    for _ in range(3):
        svc._refresh_accounts(fake_clock())
        svc._schedule_due_accounts(fake_clock())
        assert _drain(svc) == []
        fake_clock.advance(1)
    assert matching_transport.calls == []
    assert _pending(svc) == []


@pytest.mark.parametrize("clustered", [False, True])
def test_nonparticipating_target_rejects_manual_and_scheduled_probe(
    setup_target, fake_clock, matching_transport, clustered,
):
    svc, _ = setup_target(mode="active", age=None, due_in=-1, clustered=clustered,
                          schedulable=False, transport=matching_transport)
    svc._refresh_accounts(fake_clock())
    with pytest.raises(NotParticipating):
        svc.enqueue_manual_account(43 if clustered else 42, model=MODEL)
    svc._schedule_due_accounts(fake_clock())
    assert _drain(svc) == []
    assert matching_transport.calls == []


@pytest.mark.parametrize("trigger", ["manual", "scheduled"])
@pytest.mark.parametrize("state", ["queued", "running"])
@pytest.mark.parametrize("clustered", [False, True])
def test_repeated_refresh_with_pending_job_never_sends_duplicate_probe(
    setup_target, fake_clock, matching_transport, trigger, state, clustered,
):
    svc, _ = setup_target(mode="active", age=600, due_in=0, clustered=clustered,
                          transport=matching_transport)
    now = fake_clock()
    svc._refresh_accounts(now)
    if trigger == "manual":
        svc.enqueue_manual_account(43 if clustered else 42, model=MODEL)
    else:
        svc._schedule_due_accounts(now)
    pending_id = _pending(svc)[0]["id"]
    job = svc.db.claim_next_account(now=now) if state == "running" else None
    for elapsed in (0, 1, 30, 61):
        fake_clock.value = now + elapsed
        svc._refresh_accounts(fake_clock())
        svc._schedule_due_accounts(fake_clock())
        assert [row["id"] for row in _pending(svc)] == [pending_id]
        with pytest.raises(DuplicateQueue):
            svc.enqueue_manual_account(43 if clustered else 42, model=MODEL)
    assert matching_transport.calls == []
    if job is not None:
        svc._run_account_job(job)
    else:
        assert len(_drain(svc)) == 1
    finish = fake_clock()
    assert len(matching_transport.calls) == 1
    assert _pending(svc) == []
    assert svc.db.get_account_latest_round(42)["status"] == "match"
    _deadline(svc, 42, finish + 300)

    for elapsed in (0, 1, 30, 299.999):
        fake_clock.value = finish + elapsed
        svc._refresh_accounts(fake_clock())
        svc._schedule_due_accounts(fake_clock())
        assert _drain(svc) == []
        assert len(matching_transport.calls) == 1
        _deadline(svc, 42, finish + 300)
    fake_clock.value = finish + 300
    svc._refresh_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert len(_drain(svc)) == 1
    assert len(matching_transport.calls) == 2


@pytest.mark.parametrize("prior_mode", ["idle", "active"])
def test_refresh_during_actual_execution_does_not_reprobe(
    setup_target, fake_clock, matching_transport, prior_mode,
):
    svc, _ = setup_target(mode=prior_mode, age=1800, transport=matching_transport)
    # Seed already-admitted work: the public enqueue endpoint may itself refresh
    # accounts, hiding the actual in-flight idle -> active transition.
    queued, _ = svc.db.enqueue_account(42, MODEL, now=fake_clock(), trigger="manual")
    assert queued
    original_run = matching_transport.run

    def run_with_refresh(**kwargs):
        fake_clock.advance(1)
        svc._refresh_accounts(fake_clock())
        svc._schedule_due_accounts(fake_clock())
        assert len(_pending(svc)) == 1
        return original_run(**kwargs)

    matching_transport.run = run_with_refresh
    assert len(_drain(svc)) == 1
    finish = fake_clock()
    svc._refresh_accounts(finish)
    # The refresh contract intentionally preserves an already earlier deadline.
    # An in-flight idle job may have finished on its original hourly slot.
    expected = (min(finish + 300, svc._compute_idle_next_run(42, finish))
                if prior_mode == "idle" else finish + 300)
    _deadline(svc, 42, expected)
    svc._schedule_due_accounts(finish)
    assert _drain(svc) == []
    assert len(matching_transport.calls) == 1


@pytest.mark.parametrize("status_code", [403, 503])
def test_real_fallback_key_result_prevents_refresh_reprobe(
    setup_target, fake_clock, matching_transport, status_code,
):
    svc, _ = setup_target(mode="active", age=None, clustered=True,
                          transport=matching_transport)
    original_run = matching_transport.run

    def fail_first_key(**kwargs):
        result = original_run(**kwargs)
        if kwargs["account_id"] == 42:
            return ProbeResult(text=None, status_code=status_code,
                               error_code="account_unavailable", transport_detail={})
        return result

    matching_transport.run = fail_first_key
    svc._refresh_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert len(_drain(svc)) == 1
    assert [call["account_id"] for call in matching_transport.calls] == [42, 43]
    assert svc.db.get_account_rounds(42) == []
    rounds = svc.db.get_account_rounds(43)
    assert len(rounds) == 1 and rounds[0]["status"] == "match"
    finish = fake_clock()
    for elapsed in (0, 1, 30, 299.999):
        fake_clock.value = finish + elapsed
        svc._refresh_accounts(fake_clock())
        svc._schedule_due_accounts(fake_clock())
        assert _drain(svc) == []
        assert len(matching_transport.calls) == 2
        _deadline(svc, 42, finish + 300)
        assert svc.db.get_account(43)["next_run_at"] is None


@pytest.mark.parametrize("due_in", [-1, 30, 2400])
def test_disabled_representative_transfers_deadline_and_model_cursor(
    setup_target, fake_clock, due_in,
):
    svc, host = setup_target(mode="active", clustered=True, due_in=due_in)
    now = fake_clock()
    svc.db.set_account_last_model_index(42, 7, now=now)
    host.accounts_data[0]["schedulable"] = False
    svc._refresh_accounts(now)
    expected = now + min(due_in, 240)
    _deadline(svc, 43, expected)
    assert svc.db.get_account(42)["next_run_at"] is None
    assert svc.db.get_account(43)["last_model_index"] == 7
    for aid in (42, 43):
        assert svc.account_snapshot(aid)["next_run_at"] == utc_iso(expected)
    fake_clock.advance(1)
    svc._refresh_accounts(fake_clock())
    _deadline(svc, 43, expected)


@pytest.mark.parametrize("handoff", ["disabled", "removed", "new_lower_id"])
def test_representative_handoff_does_not_reprobe_recent_member(
    setup_target, fake_clock, matching_transport, handoff,
):
    svc, host = setup_target(mode="active", clustered=True, age=60,
                          transport=matching_transport)
    now = fake_clock()
    svc._refresh_accounts(now)
    if handoff == "disabled":
        host.accounts_data[0]["schedulable"] = False
        rep_id = 43
    elif handoff == "removed":
        host.accounts_data.pop(0)
        rep_id = 43
    else:
        host.accounts_data.append({**host.accounts_data[0], "account_id": 41, "name": "41"})
        rep_id = 41
    svc._refresh_accounts(now)
    svc._schedule_due_accounts(now)
    _drain(svc)
    assert matching_transport.calls == [], "a new representative must not re-probe the same recent round"
    _deadline(svc, rep_id, now + 240)
    for account in svc.db.get_all_accounts():
        if account["account_id"] != rep_id:
            assert account["next_run_at"] is None


@pytest.mark.parametrize("age", [1, 60])
def test_cluster_missing_deadline_waits_for_latest_member_interval(
    setup_target, fake_clock, matching_transport, age,
):
    svc, _ = setup_target(mode="active", clustered=True, age=age, due_in=None,
                          transport=matching_transport)
    now = fake_clock()
    svc._refresh_accounts(now)
    svc._schedule_due_accounts(now)
    _drain(svc)
    assert matching_transport.calls == [], "NULL schedule recovery must not send an early paid probe"
    _deadline(svc, 42, now - age + 300)


@pytest.mark.parametrize("handoff", ["disabled", "removed"])
@pytest.mark.parametrize("trigger", ["manual", "scheduled"])
@pytest.mark.parametrize("state", ["queued", "running"])
def test_representative_change_with_pending_job_sends_only_one_probe(
    setup_target, fake_clock, matching_transport, handoff, trigger, state,
):
    svc, host = setup_target(mode="active", clustered=True, age=600, due_in=0,
                          transport=matching_transport)
    now = fake_clock()
    svc._refresh_accounts(now)
    if trigger == "manual":
        svc.enqueue_manual_account(42, model=MODEL)
    else:
        svc._schedule_due_accounts(now)
    job = svc.db.claim_next_account(now=now) if state == "running" else None
    if handoff == "disabled":
        host.accounts_data[0]["schedulable"] = False
    else:
        host.accounts_data.pop(0)
    svc._refresh_accounts(now)
    svc._schedule_due_accounts(now)
    if job is not None:
        svc._run_account_job(job)
    _drain(svc)
    assert [call["account_id"] for call in matching_transport.calls] == [43], (
        "pending work on the former representative must suppress a second cluster probe"
    )
    assert len(svc.db.get_account_rounds(43)) == 2  # one historical round, one actual check
    assert _pending(svc) == []


@pytest.mark.parametrize("state", ["queued", "running"])
def test_manual_request_after_representative_change_recognizes_member_queue(
    setup_target, fake_clock, matching_transport, state,
):
    svc, host = setup_target(mode="active", clustered=True, age=600, due_in=0,
                          transport=matching_transport)
    svc._refresh_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    if state == "running":
        assert svc.db.claim_next_account(now=fake_clock()) is not None
    host.accounts_data[0]["schedulable"] = False
    svc._refresh_accounts(fake_clock())
    with pytest.raises(DuplicateQueue):
        svc.enqueue_manual_account(43, model=MODEL)
    assert len(_pending(svc)) == 1
    assert matching_transport.calls == []


@pytest.mark.parametrize("trigger", ["manual", "scheduled"])
def test_queued_account_disabled_before_execution_never_sends_probe(
    setup_target, fake_clock, matching_transport, trigger,
):
    svc, host = setup_target(mode="active", age=600, due_in=0, transport=matching_transport)
    svc._refresh_accounts(fake_clock())
    if trigger == "manual":
        svc.enqueue_manual_account(42, model=MODEL)
    else:
        svc._schedule_due_accounts(fake_clock())
    assert len(_pending(svc)) == 1
    host.accounts_data[0]["schedulable"] = False
    svc._refresh_accounts(fake_clock())
    _drain(svc)
    assert matching_transport.calls == [], "queued jobs must revalidate participation before sending"
    assert len(svc.db.get_account_rounds(42)) == 1  # do not overwrite real history


@pytest.mark.parametrize("trigger", ["manual", "scheduled"])
def test_queued_removed_model_never_sends_unsupported_probe(
    setup_target, fake_clock, matching_transport, trigger,
):
    svc, host = setup_target(mode="active", age=600, due_in=0, transport=matching_transport)
    svc._refresh_accounts(fake_clock())
    if trigger == "manual":
        svc.enqueue_manual_account(42, model=MODEL)
    else:
        svc._schedule_due_accounts(fake_clock())
    assert len(_pending(svc)) == 1
    host.accounts_data[0]["models"] = ["gpt-5.6-sol"]
    host.accounts_data[0]["real_models_10m"] = ["gpt-5.6-sol"]
    svc._refresh_accounts(fake_clock())
    _drain(svc)
    assert matching_transport.calls == [], "stale queue model must not bypass the current whitelist"
    assert len(svc.db.get_account_rounds(42)) == 1


@pytest.mark.parametrize("clustered", [False, True])
def test_first_target_without_history_runs_once_immediately(
    setup_target, fake_clock, matching_transport, clustered,
):
    svc, _ = setup_target(mode="active", clustered=clustered, age=None, due_in=None,
                          transport=matching_transport)
    svc._refresh_accounts(fake_clock())
    _deadline(svc, 42, fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert len(_drain(svc)) == 1
    svc._refresh_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert _drain(svc) == []
    assert len(matching_transport.calls) == 1


@pytest.mark.parametrize("clustered", [False, True])
def test_failed_host_fetch_heals_locally_without_retiring_cached_accounts(
    setup_target, fake_clock, clustered,
):
    svc, host = setup_target(mode="active", clustered=clustered)
    fetch_accounts = host.fetch_accounts
    host.fetch_accounts = lambda models: ([], False)
    svc._refresh_accounts(fake_clock())
    # Reject the failed remote payload, but heal using the committed local state.
    _deadline(svc, 42, fake_clock() + 240)
    for aid in (42, 43) if clustered else (42,):
        assert svc.db.get_account(aid)["mode"] == "active"
        assert svc.db.get_account(aid)["schedulable"]
    host.fetch_accounts = fetch_accounts
    svc._refresh_accounts(fake_clock())
    _deadline(svc, 42, fake_clock() + 240)


@pytest.mark.parametrize("departure", ["removed", "different_cluster"])
def test_departed_member_history_does_not_delay_remaining_target(
    setup_target, fake_clock, departure,
):
    svc, host = setup_target(mode="active", clustered=True, age=1)
    now = fake_clock()
    if departure == "removed":
        host.accounts_data.pop()
    else:
        host.accounts_data[1]["cluster_id"] = "other-pool"
    svc._refresh_accounts(now)
    _deadline(svc, 42, now)
    target, _ = svc._get_target_and_members(42)
    assert target["member_account_ids"] == [42]


@pytest.mark.parametrize("age", [-30, -0.001])
def test_future_checked_time_is_not_treated_as_missing_history(setup_target, fake_clock, age):
    svc, _ = setup_target(mode="active", age=age)
    now = fake_clock()
    svc._refresh_accounts(now)
    _deadline(svc, 42, now - age + 300)
    svc._schedule_due_accounts(now)
    assert _pending(svc) == []


@pytest.mark.parametrize("state", ["queued", "running"])
def test_pending_old_queue_respects_global_disable_without_sending(
    setup_target, fake_clock, matching_transport, state,
):
    svc, _ = setup_target(mode="active", age=600, due_in=0,
                          transport=matching_transport,
                          config_overrides={"enabled": False})
    queued, _ = svc.db.enqueue_account(42, MODEL, now=fake_clock(), trigger="scheduled")
    assert queued
    # A previously running request is not claimed again after disabling, either.
    if state == "running":
        assert svc.db.claim_next_account(now=fake_clock()) is not None
    for _ in range(3):
        svc._refresh_accounts(fake_clock())
        svc._schedule_due_accounts(fake_clock())
        assert svc.db.claim_next_account(now=fake_clock(), allow_scheduled=False) is None
        assert len(_pending(svc)) == 1
        fake_clock.advance(1)
    assert matching_transport.calls == []


@pytest.mark.parametrize("trigger", ["manual", "scheduled"])
def test_future_available_job_is_not_duplicated_or_executed_early(
    setup_target, fake_clock, matching_transport, trigger,
):
    svc, _ = setup_target(mode="active", age=600, due_in=0, transport=matching_transport)
    now = fake_clock()
    queued, queue_id = svc.db.enqueue_account(
        42, MODEL, now=now, available_at=now + 60, trigger=trigger,
    )
    assert queued
    for elapsed in (0, 1, 30, 59.999):
        fake_clock.value = now + elapsed
        svc._refresh_accounts(fake_clock())
        svc._schedule_due_accounts(fake_clock())
        assert [row["id"] for row in _pending(svc)] == [queue_id]
        assert _drain(svc) == []
        assert matching_transport.calls == []
    fake_clock.value = now + 60
    svc._refresh_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert len(_drain(svc)) == 1
    assert len(matching_transport.calls) == 1
    _deadline(svc, 42, now + 360)


@pytest.mark.parametrize("clustered", [False, True])
@pytest.mark.parametrize("elapsed", [3600, 7200])
def test_missed_active_intervals_run_once_without_catchup_burst(
    setup_target, fake_clock, matching_transport, clustered, elapsed,
):
    svc, host = setup_target(mode="active", age=600, due_in=0, clustered=clustered,
                             transport=matching_transport)
    fake_clock.advance(elapsed)
    for account in host.accounts_data:
        account["last_real_request_at"] = utc_iso(fake_clock())
    for _ in range(3):
        svc._refresh_accounts(fake_clock())
        svc._schedule_due_accounts(fake_clock())
        _drain(svc)
        assert len(matching_transport.calls) == 1
        _deadline(svc, 42, fake_clock() + 300)


def test_representative_changes_during_transport_do_not_duplicate_paid_work(
    setup_target, fake_clock, matching_transport,
):
    svc, host = setup_target(mode="active", age=600, due_in=0, clustered=True,
                             transport=matching_transport)
    original_run = matching_transport.run

    def disable_rep_during_first_request(**kwargs):
        result = original_run(**kwargs)
        if len(matching_transport.calls) == 1:
            fake_clock.advance(1)
            host.accounts_data[0]["schedulable"] = False
            svc._refresh_accounts(fake_clock())
            svc._schedule_due_accounts(fake_clock())
        return result

    matching_transport.run = disable_rep_during_first_request
    svc._refresh_accounts(fake_clock())
    svc._schedule_due_accounts(fake_clock())
    _drain(svc)
    assert [call["account_id"] for call in matching_transport.calls] == [42], (
        "a request already in flight may complete, but handoff must not issue another"
    )
    assert _pending(svc) == []
    expected_due = min(svc.db.get_account(43)["next_run_at"], fake_clock() + 300)
    assert expected_due > fake_clock()
    svc._refresh_accounts(fake_clock())
    _deadline(svc, 43, expected_due)
    assert svc.db.get_account(42)["next_run_at"] is None
