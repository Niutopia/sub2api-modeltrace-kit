import pytest
import sqlite3
import uuid
from modeltrace.db import ModelTraceDB
from tests.conftest import build_service,run_one,FakeReceipts


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_additive_schema_keeps_legacy_reservations_held(tmp_path,fake_clock):
    s,_,_=build_service(tmp_path,fake_clock,receipts=FakeReceipts(missing=True))
    run_one(s)
    before=s.db.budget_snapshot(now=fake_clock())
    path=s.db.path;s.db.close()
    # Simulate v0.1.2 absence of the additive ledger, retaining reservation/queue.
    with sqlite3.connect(path) as db:
        db.execute('DROP TABLE reconciliation_probes')
        db.execute('DROP TABLE reconciliation_rounds')
        db.execute('DROP TABLE reservation_cost_floors')
    db=ModelTraceDB(path)
    fake_clock.advance(600)
    assert db.list_reconciliation_due(now=fake_clock())==[]
    assert db.budget_snapshot(now=fake_clock()).reserved_usd==before.reserved_usd
    assert db.reconciliation_summary()['legacy_unlinked']==1
    assert db._conn.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
    db.close()


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_durable_ids_are_committed_before_transport(tmp_path,fake_clock):
    from tests.conftest import FakeTransport
    class InspectTransport(FakeTransport):
        def run(self,**kw):
            with sqlite3.connect(s.db.path) as conn:
                rows=conn.execute('SELECT user_agent,status FROM reconciliation_probes ORDER BY ordinal').fetchall()
            assert len(rows)==3
            assert dict(rows)[kw['user_agent']]=='attempted'
            return super().run(**kw)
    s,t,_=build_service(tmp_path,fake_clock,transport=InspectTransport())
    run_one(s)
    assert len(t.calls)==3
    s.db.close()


def test_invalid_present_account_cost_never_falls_back_in_reconciliation():
    from tests.test_receipts_reconciliation import reader_for,FakeConnection,row,ua
    reader,_=reader_for(FakeConnection([row(ua(),total='0.01',account='NaN')]))
    assert reader.read_many([ua()])[ua()].receipts[0].charge_usd is None


def test_cannot_cancel_planned_probe_while_queue_running(tmp_path,fake_clock):
    s,_,_=build_service(tmp_path,fake_clock)
    _,qid=s.db.enqueue(1,now=fake_clock(),trigger='test')
    job=s.db.claim_next(now=fake_clock())
    r=s.db.reserve_budget(1,qid,0.1,daily_budget_usd=5,now=fake_clock())
    agents=['ModelTraceProbe/'+str(uuid.uuid4()) for _ in range(3)]
    s.db.register_reconciliation(r.reservation_id,'gpt-5.4',agents,now=fake_clock())
    assert not s.db.cancel_probe(r.reservation_id,0,now=fake_clock())
    assert s.db.mark_probe_attempted(r.reservation_id,0,now=fake_clock())
    assert not s.db.mark_probe_attempted(r.reservation_id,0,now=fake_clock())
    s.db.close()


def test_orphan_reserved_rows_are_exposed_but_never_released(tmp_path, fake_clock):
    service, _, _ = build_service(tmp_path, fake_clock)
    _, queue_id = service.db.enqueue(1, now=fake_clock(), trigger="test")
    service.db.claim_next(now=fake_clock())
    reservation = service.db.reserve_budget(1, queue_id, 0.25, daily_budget_usd=5, now=fake_clock())
    assert reservation is not None

    summary = service.db.orphan_summary()
    assert summary["count"] == 1
    assert summary["reserved_usd"] == 0.25
    assert summary["reservation_ids"] == [reservation.reservation_id]
    assert summary["release_automatic"] is False
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0.25
