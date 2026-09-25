"""Offline regression tests for the temporary 600-second schedule."""
from dataclasses import replace
import json
import socket
import sqlite3

import pytest

from modeltrace.config import ConfigError, load_config
from modeltrace.service import ModelTraceService
from tests.conftest import FakeTransport


MODELS = ('gpt-5.4', 'gpt-5.5', 'gpt-5.6-sol', 'gpt-5.6-terra')


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('schedule tests must not access the network')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect_ex', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setattr(socket, 'getaddrinfo', forbidden)


@pytest.fixture
def config_path(tmp_path):
    path = tmp_path / 'ten-minute.json'
    path.write_text(json.dumps({
        'base_url': 'https://modeltrace.invalid/v1',
        'api_key': 'offline-test-only', 'enabled': True,
        'interval_seconds': 600, 'auto_retests': 0,
        # Slots must follow IDs, not configuration insertion order.
        'monitors': {str(i): {'model': MODELS[i - 1], 'enabled': True}
                     for i in (4, 3, 2, 1)},
    }), encoding='utf-8')
    return path


@pytest.fixture
def service_factory(tmp_path, fake_clock, config_path):
    services = []

    def make(*, config=None, transport=None):
        service = ModelTraceService(
            config or load_config(config_path),
            database_path=tmp_path / 'offline.sqlite3',
            transport=transport or FakeTransport(),
            clock=fake_clock, monotonic=fake_clock, sleeper=lambda _: None,
        )
        services.append(service)
        return service

    yield make
    for service in services:
        service.db.close()


def test_600_second_config_loads(config_path):
    config = load_config(config_path)
    assert config.interval_seconds == 600
    assert config.auto_retests == 0
    assert config.enabled
    assert len(config.monitors) == 4
    assert all(m.enabled and m.configured_supported for m in config.monitors.values())


def test_600_second_config_rejects_auto_retests(config_path):
    raw = json.loads(config_path.read_text())
    raw['auto_retests'] = 1
    config_path.write_text(json.dumps(raw))
    with pytest.raises(ConfigError, match='auto_retests'):
        load_config(config_path)


def test_four_supported_models_150_apart_each_every_600(service_factory, fake_clock, monkeypatch):
    import modeltrace.service as service_module
    monkeypatch.setattr(
        service_module,
        "analyze_outputs",
        lambda outputs, bank: {
            "prediction": "gpt-5.4",
            "results": [{"model": "gpt-5.4", "probability": 0.95}],
            "calibration": {"queries": "1"},
        },
    )
    monkeypatch.setattr(
        service_module,
        "classify_sample",
        lambda result, expected, previous=None: {
            "outcome": "compatible",
            "prediction": expected,
            "closedSetWeight": 0.95,
            "expectedWeight": 0.95,
        },
    )
    service = service_factory()
    anchor = fake_clock()
    assert service._scheduled_monitor_ids() == [1, 2, 3, 4]
    assert all(service.is_supported(m) for m in service.config.monitors.values())
    assert [service.db.get_state(i)['next_run_at'] - anchor for i in range(1, 5)] == [0, 150, 300, 450]
    starts = {i: [] for i in range(1, 5)}
    for slot in range(8):
        mid = slot % 4 + 1
        service._schedule_due(fake_clock())
        job = service.db.claim_next(now=fake_clock())
        assert job is not None and job.monitor_id == mid
        starts[mid].append(fake_clock())
        before = len(service.transport.calls)
        service._run_job_safely(job)
        assert len(service.transport.calls) - before == 1
        assert service.db.get_state(mid)['next_run_at'] == fake_clock() + 600
        assert service.db.claim_next(now=fake_clock()) is None
        fake_clock.advance(150)
    assert all(times[1] - times[0] == 600 for times in starts.values())


@pytest.mark.parametrize('elapsed, offsets, due_id', [
    (150, [600, 750, 300, 450], 2),  # Exact stale-slot boundary skips model 1.
    (1801, [2400, 1950, 2100, 2250], None),  # No backlog burst after downtime.
])
def test_missed_slots_do_not_catch_up(service_factory, fake_clock, elapsed, offsets, due_id, monkeypatch):
    import modeltrace.service as service_module
    monkeypatch.setattr(
        service_module,
        "classify_sample",
        lambda result, expected, previous=None: {
            "outcome": "compatible",
            "prediction": expected,
            "closedSetWeight": 0.95,
            "expectedWeight": 0.95,
        },
    )
    service = service_factory()
    anchor = fake_clock()
    fake_clock.advance(elapsed)
    service._schedule_due(fake_clock())
    job = service.db.claim_next(now=fake_clock())
    if due_id is None:
        assert job is None
    else:
        assert job is not None and job.monitor_id == due_id
        service._run_job_safely(job)
    assert service.db.claim_next(now=fake_clock()) is None
    assert [service.db.get_state(i)['next_run_at'] - anchor for i in range(1, 5)] == offsets
    assert len(service.transport.calls) == (0 if due_id is None else 1)


@pytest.mark.parametrize('failure', ['success', 'transport_error', 'exception'])
def test_single_probe_attempt_cap_no_retry_or_replay(service_factory, fake_clock, failure, monkeypatch):
    class Transport(FakeTransport):
        def run(self, **kwargs):
            result = super().run(**kwargs)
            if failure == 'exception':
                raise RuntimeError('offline simulated failure')
            return result

    transport = Transport(error_code='transport_error' if failure == 'transport_error' else None)
    service = service_factory(transport=transport)
    anchor = fake_clock()
    service._schedule_due(anchor)
    service._schedule_due(anchor)
    import modeltrace.service as service_module
    monkeypatch.setattr(
        service_module,
        "classify_sample",
        lambda result, expected, previous=None: {
            "outcome": "compatible",
            "prediction": expected,
            "closedSetWeight": 0.95,
            "expectedWeight": 0.95,
        },
    )
    job = service.db.claim_next(now=anchor)
    assert job is not None
    service._run_job_safely(job)
    assert len(transport.calls) == 1
    attempts = service.db._conn.execute(
        'SELECT ordinal FROM detection_attempts WHERE queue_id = ? ORDER BY ordinal', (job.queue_id,)
    ).fetchall()
    assert [row['ordinal'] for row in attempts] == [0]
    assert len({call['user_agent'] for call in transport.calls}) == 1
    if failure != 'success':
        assert service.snapshot(1)['latest']['status'] == 'error'
    service._run_job_safely(job)
    fake_clock.advance(149)
    service._schedule_due(fake_clock())
    assert service.db.claim_next(now=fake_clock()) is None
    assert len(transport.calls) == 1
    assert service.db.get_state(1)['next_run_at'] == anchor + 600
    assert service.db._conn.execute('SELECT COUNT(*) FROM queue').fetchone()[0] == 1


def test_interval_change_retains_old_future_slots_until_offline_reset(service_factory, config_path, fake_clock):
    config = load_config(config_path)
    anchor = fake_clock()
    hourly = service_factory(config=replace(config, interval_seconds=3600))
    old_slots = [anchor + offset for offset in (3600, 4500, 5400, 6300)]
    for mid, due in enumerate(old_slots, 1):
        hourly.db.set_next_run(mid, due, now=anchor)
    hourly.db.close()

    ten_minute = service_factory(config=config)
    assert [ten_minute.db.get_state(i)['next_run_at'] for i in range(1, 5)] == old_slots
    ten_minute._schedule_due(anchor)
    assert ten_minute.db.claim_next(now=anchor) is None
    ten_minute.db.close()

    # Operator-only migration with the service stopped; never change runtime code.
    with sqlite3.connect(config_path.parent / 'offline.sqlite3') as conn:
        conn.execute('BEGIN IMMEDIATE')
        assert conn.execute("SELECT COUNT(*) FROM queue WHERE state IN ('queued', 'running')").fetchone()[0] == 0
        changed = conn.execute(
            'UPDATE monitor_state SET next_run_at = NULL, updated_at = ? '
            'WHERE monitor_id IN (1, 2, 3, 4) AND next_run_at > ?',
            (anchor, anchor),
        ).rowcount
        assert changed == 4
    restarted = service_factory(config=config)
    assert [restarted.db.get_state(i)['next_run_at'] - anchor for i in range(1, 5)] == [0, 150, 300, 450]
