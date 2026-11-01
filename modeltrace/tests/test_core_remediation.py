"""Remediation regressions use real temporary SQLite and mocked probe/host transport."""
import json
from dataclasses import replace
import pytest
from app import create_app
from modeltrace.transport import ProbeResult
from tests.conftest import build_service
from tests.test_per_account_d2 import make_service, MockHostClient, MockProbeTransport
MODEL = 'gpt-5.6-sol'
OTHER = 'gpt-5.6-luna'

def analysis(model=MODEL, kind='negative'):
    top, prob, expected = (OTHER, .90, .05) if kind == 'negative' else ((model,.90,.90) if kind == 'compatible' else (OTHER,.45,.30))
    results = [{'model': top, 'probability': prob}]
    if top != model:
        results.append({'model': model, 'probability': expected})
    return {'prediction': top, 'results': results, 'calibration': {'queries': 1}}

def account(aid, cluster=None):
    return {'account_id': aid, 'name': f'fake-{aid}', 'platform': 'openai', 'type': 'apikey',
            'schedulable': True, 'models': [MODEL], 'cluster_id': cluster,
            'last_real_request_at': None, 'real_requests_10m': 0, 'paused_models': []}

def account_service(tmp_path, clock, monkeypatch, accounts):
    host = MockHostClient(accounts)
    monkeypatch.setattr('modeltrace.service.HostClient', lambda *a, **kw: host)
    calibrations = [{'account_id': a['account_id'], 'model': MODEL, 'reasoning_effort': 'none', 'reference': 'offline-review-calibration'} for a in accounts]
    svc = make_service(tmp_path, clock, host_client=host,
                       config_overrides={'auto_pause_calibrations': calibrations})
    return svc, host

def test_review_manual_monitor_loses_continuation(tmp_path, fake_clock, monkeypatch):
    svc, transport, _ = build_service(tmp_path, fake_clock)
    svc.config = replace(svc.config, per_account_enabled=False)
    monkeypatch.setattr('modeltrace.service.analyze_outputs', lambda *a: analysis('gpt-5.4'))
    app = create_app(svc, secret='offline-review-secret', start_worker=False)
    response = app.test_client().post('/v1/monitors/1/run', json={'expected_model': 'gpt-5.4'},
                                      headers={'Authorization': 'Bearer offline-review-secret'})
    assert response.status_code == 202
    job = svc.db.claim_next(now=fake_clock(), allow_scheduled=False)
    svc._run_job_safely(job)
    snap = svc.snapshot(1)
    assert len(transport.calls) == 1 and snap['latest']['status'] == 'uncertain'
    assert snap['retest_progress'] == {'done': 0, 'total': 3}
    pending = svc.db.get_pending_job(1)
    assert pending is not None and pending['trigger'] == 'manual'
    assert pending['retest_index'] == 1
    # Completion replay is idempotent, including successor queue creation.
    assert svc.db.record_round_and_finish(job, status='uncertain', target_probability=None,
        best_model=None, checked_at=fake_clock(), message_code='test', ranking=[], diagnostics={},
        next_run_at=fake_clock(), next_run_after=None, upstream_statuses=[], receipt_count=0,
        receipt_consistent=None, actual_cost_usd=None, reserved_usd=None,
        continuation_retest_index=1) == 0
    assert svc.db.get_pending_job(1)['id'] == pending['id']
    # Preserve the legacy rule: initial + THREE retests, not the account 3/5 rule.
    for index in range(1, 4):
        next_job = svc.db.claim_next(now=fake_clock(), allow_scheduled=False)
        assert next_job is not None and next_job.retest_index == index
        svc._run_job_safely(next_job)
    assert svc.db.get_pending_job(1) is None
    assert len(transport.calls) == 4
    assert svc.snapshot(1)['latest']['status'] == 'suspect'
    svc.db.close()

def test_review_cluster_move_applies_old_evidence_to_unprobed_peer(tmp_path, fake_clock, monkeypatch):
    # Only grouping metadata changes: routes/models/credentials/calibrations do not.
    rows = [account(101, 'old-cluster'), account(102, 'old-cluster'), account(201, 'new-cluster')]
    svc, host = account_service(tmp_path, fake_clock, monkeypatch, rows)
    monkeypatch.setattr('modeltrace.service.analyze_outputs', lambda *a: analysis())
    class MoveOnFifth(MockProbeTransport):
        def run(self, **kwargs):
            result = super().run(**kwargs)
            if kwargs['account_id'] == 101:
                return ProbeResult(None, 401, 'upstream_auth')
            if len(self.calls) == 5:
                host.accounts_data = [account(101, 'new-cluster'), account(102, 'old-cluster'), account(201, 'new-cluster')]
                svc._refresh_accounts(fake_clock())
            return result
    transport = MoveOnFifth()
    svc.transport = transport
    svc.enqueue_manual_account(101, model=MODEL)
    svc._run_account_job_safely(svc.db.claim_next_account(now=fake_clock()))
    assert [c['account_id'] for c in transport.calls] == [101, 102, 102, 101, 102]
    assert [c['account_id'] for c in host.pause_calls] == [102]
    assert svc.db.get_account_latest_round(102)['status'] == 'suspect'
    assert svc.db.get_account_latest_round(201) is None
    assert svc.db.get_account(102)['cluster_id'] == 'old-cluster'
    print('CROSS_CLUSTER: all three valid negative votes from 102 (still old-cluster); paused=', [c['account_id'] for c in host.pause_calls])
    svc.db.close()

def test_review_disabled_restart_leaves_manual_run_permanently_blocked(tmp_path, fake_clock, monkeypatch):
    svc, host = account_service(tmp_path, fake_clock, monkeypatch, [account(101)])
    svc.db.set_account_next_run(101, fake_clock(), now=fake_clock())
    svc._schedule_due_accounts(fake_clock())
    assert svc.db.get_pending_account_job(101)['trigger'] == 'scheduled'
    svc.db.close()
    # Restart with automatic detection disabled; keep the real persisted queue.
    svc = make_service(tmp_path, fake_clock, host_client=host, config_overrides={'enabled': False})
    app = create_app(svc, secret='offline-review-secret', start_worker=False)
    response = app.test_client().post('/v1/accounts/101/run', json={'model': MODEL},
                                     headers={'Authorization': 'Bearer offline-review-secret'})
    assert response.status_code == 202
    pending = svc.db.get_pending_account_job(101)
    assert pending['trigger'] == 'manual'
    job = svc.db.claim_next_account(now=fake_clock(), allow_scheduled=False)
    assert job is not None and job.trigger == 'manual'
    # A manual request must never replace a RUNNING job.
    duplicate = app.test_client().post('/v1/accounts/101/run', json={'model': MODEL},
        headers={'Authorization': 'Bearer offline-review-secret'})
    assert duplicate.status_code == 409
    assert len(svc.transport.calls) == 0
    svc.db.close()



def test_disappearing_sampled_lineage_does_not_pause_or_schedule_new_cluster(tmp_path, fake_clock, monkeypatch):
    rows = [account(101, 'old-cluster'), account(102, 'old-cluster'), account(201, 'new-cluster')]
    svc, host = account_service(tmp_path, fake_clock, monkeypatch, rows)
    monkeypatch.setattr('modeltrace.service.analyze_outputs', lambda *a: analysis())
    preserved_schedule = {}
    class MoveAll(MockProbeTransport):
        def run(self, **kwargs):
            result = super().run(**kwargs)
            if len(self.calls) == 3:
                host.accounts_data = [account(aid, 'new-cluster') for aid in [101,102,201]]
                svc._refresh_accounts(fake_clock())
                for aid in [101,102,201]:
                    preserved_schedule[aid] = svc.db.get_account(aid)['next_run_at']
            return result
    svc.transport = MoveAll()
    svc.enqueue_manual_account(101, model=MODEL)
    svc._run_account_job_safely(svc.db.claim_next_account(now=fake_clock()))
    assert len(svc.transport.calls) == 3
    assert not host.pause_calls and not host.resume_calls
    assert all(svc.db.get_account(aid)['next_run_at'] == value for aid,value in preserved_schedule.items())
    svc.db.close()


def test_scheduled_monitor_continuation_does_not_gain_manual_privileges(tmp_path, fake_clock, monkeypatch):
    svc, transport, _ = build_service(tmp_path, fake_clock, enabled=True)
    svc.config = replace(svc.config, per_account_enabled=False)
    monkeypatch.setattr('modeltrace.service.analyze_outputs', lambda *a: analysis('gpt-5.4'))
    assert svc.db.enqueue(1, now=fake_clock(), trigger='scheduled')[0]
    job = svc.db.claim_next(now=fake_clock())
    svc._run_job_safely(job)
    pending = svc.db.get_pending_job(1)
    assert pending is not None and pending['trigger'] == 'auto_retest'
    assert svc.db.claim_next(now=fake_clock(), allow_scheduled=False) is None
    assert len(transport.calls) == 1
    assert svc.db.claim_next(now=fake_clock(), allow_scheduled=True).retest_index == 1
    svc.db.close()


def test_manual_continuation_is_not_replayed_after_batch_state_is_lost(tmp_path, fake_clock, monkeypatch):
    svc, transport, _ = build_service(tmp_path, fake_clock)
    svc.config = replace(svc.config, per_account_enabled=False)
    monkeypatch.setattr('modeltrace.service.analyze_outputs', lambda *a: analysis('gpt-5.4'))
    assert svc.db.enqueue(1, now=fake_clock(), trigger='manual')[0]
    svc._run_job_safely(svc.db.claim_next(now=fake_clock(), allow_scheduled=False))
    pending = svc.db.get_pending_job(1)
    assert pending['trigger'] == 'manual' and pending['retest_index'] == 1
    svc.db.recover_running_jobs(now=fake_clock(), interval_seconds=svc.config.interval_seconds)
    assert svc.db.claim_next(now=fake_clock(), allow_scheduled=False) is None
    assert len(transport.calls) == 1
    svc.db.close()


def test_expiry_preserves_manual_and_running_continuations(tmp_path, fake_clock, monkeypatch):
    svc, _, _ = build_service(tmp_path, fake_clock)
    svc.config = replace(svc.config, per_account_enabled=False)
    monkeypatch.setattr('modeltrace.service.analyze_outputs', lambda *a: analysis('gpt-5.4'))
    assert svc.db.enqueue(1, now=fake_clock(), trigger='manual')[0]
    svc._run_job_safely(svc.db.claim_next(now=fake_clock(), allow_scheduled=False))
    fake_clock.advance(svc.config.interval_seconds * 3)
    assert not svc.db.expire_scheduled_monitor_continuation(
        1, now=fake_clock(), max_idle_seconds=svc.config.interval_seconds)
    continuation = svc.db.claim_next(now=fake_clock(), allow_scheduled=False)
    assert continuation.retest_index == 1
    # The expiry primitive cannot terminate an in-flight job even if its
    # provenance is automatic, nor may it create a replacement queue entry.
    svc.db._conn.execute("UPDATE queue SET trigger='auto_retest' WHERE id=?", (continuation.queue_id,))
    assert not svc.db.expire_scheduled_monitor_continuation(
        1, now=fake_clock(), max_idle_seconds=svc.config.interval_seconds)
    assert svc.db.get_pending_job(1)['state'] == 'running'
    svc.db.close()
