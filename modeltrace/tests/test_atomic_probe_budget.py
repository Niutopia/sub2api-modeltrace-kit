import pytest
from tests.conftest import build_service,run_one
from modeltrace.receipts import ReceiptReader


@pytest.mark.skip(reason="v0.1.12 单题检测，新规则见 tests/test_single_probe_v0112.py")
def test_probe_ledger_failure_rolls_back_budget_and_never_sends(tmp_path,fake_clock):
    service,transport,_=build_service(tmp_path,fake_clock)
    service.db._conn.execute("CREATE TRIGGER inject_probe_failure BEFORE INSERT ON reconciliation_probes BEGIN SELECT RAISE(ABORT,'injected'); END")
    run_one(service)
    assert service.snapshot(1)["latest"]["message_code"]=="internal_error"
    assert not transport.calls
    assert service.db._conn.execute('SELECT count(*) FROM budget_reservations').fetchone()[0]==0
    assert service.db._conn.execute('SELECT count(*) FROM reconciliation_rounds').fetchone()[0]==0
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd==0


@pytest.mark.skip(reason="v0.1.12 单题检测，新规则见 tests/test_single_probe_v0112.py")
def test_probe_ids_already_committed_before_transport(tmp_path,fake_clock):
    service,transport,_=build_service(tmp_path,fake_clock)
    original=transport.run
    def run(**kwargs):
        r=service.db._conn.execute('SELECT status FROM reconciliation_probes WHERE user_agent=?',(kwargs['user_agent'],)).fetchone()
        assert r and r[0]=='attempted'
        assert service.db._conn.execute('SELECT count(*) FROM reconciliation_probes').fetchone()[0]==3
        return original(**kwargs)
    transport.run=run
    run_one(service)
    assert len(transport.calls)==3


@pytest.mark.parametrize('bad',[float('nan'),float('inf'),-1,True,'invalid'])
def test_invalid_present_upstream_cost_never_falls_back(bad):
    r=ReceiptReader._row_to_receipt(['acct','model',1,2,0,.01,bad,'default',None])
    assert r.charge_usd is None
