from dataclasses import replace
import sqlite3
import pytest
from modeltrace.config import Pricing
from modeltrace.service import TooSoon
from tests.test_reconciliation_service import BatchReceiptStub, RecordingTransport, run_one, MODEL
from tests.conftest import build_service

pytestmark = pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")


def held_service(tmp_path, fake_clock):
    r = BatchReceiptStub(); t = RecordingTransport()
    s, _, _ = build_service(tmp_path, fake_clock, receipts=r, transport=t)
    s.config = replace(s.config, budget_basis='lei_group4_v1',
                       billing_prices={MODEL: Pricing(0.00001, 0.00001, 0.00001)})
    run_one(s)
    fake_clock.advance(3600)
    return s, r, t


def test_held_round_blocks_manual_scheduled_and_already_queued_requests(tmp_path, fake_clock):
    s, r, t = held_service(tmp_path, fake_clock)
    assert len(t.calls) == 3
    assert s.snapshot(1)['paused_reconciliation']
    with pytest.raises(TooSoon): s.enqueue_manual(1, expected_model=MODEL)
    s._schedule_due(fake_clock())
    assert not s.db.has_pending(1)
    run_one(s)
    assert len(t.calls) == 3
    assert not s.db.has_pending(1)


def proofs(agents, model=MODEL):
    return {ua:dict(evidence_id=i+1,model=model,created_at='2026-09-20',
                   evidence_kind='gateway_routing_not_dispatched') for i,ua in enumerate(agents)}


def test_verified_gateway_rejection_releases_hold_with_immutable_audit(tmp_path, fake_clock):
    s, r, t = held_service(tmp_path, fake_clock)
    r.read_nonbillable_many = proofs
    assert s.reconcile_pending()['settled'] == 1
    assert s.db.budget_snapshot(now=fake_clock()).reserved_usd == 0
    assert not s.snapshot(1)['paused_reconciliation']
    assert s.db._conn.execute('select count(*) from nonbillable_probe_evidence').fetchone()[0] == 3
    with pytest.raises(sqlite3.IntegrityError):
        s.db._conn.execute('delete from nonbillable_probe_evidence')
    assert len(t.calls) == 3


@pytest.mark.parametrize('kind', ['wrong_model', 'query_failure', 'missing'])
def test_unknown_cost_cannot_be_released_by_proof_fallback(tmp_path, fake_clock, kind):
    s, r, t = held_service(tmp_path, fake_clock)
    r.read_nonbillable_many = lambda agents: proofs(agents, 'wrong' if kind=='wrong_model' else MODEL)
    if kind == 'query_failure':
        from modeltrace.receipts import ReceiptResult
        r.read_many=lambda agents:{ua:ReceiptResult([], 'receipt_query_failed') for ua in agents}
    if kind == 'missing': r.read_nonbillable_many=lambda agents:{}
    assert s.reconcile_pending()['settled'] == 0
    assert s.db.budget_snapshot(now=fake_clock()).reserved_usd > 0
