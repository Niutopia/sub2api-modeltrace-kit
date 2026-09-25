"""Offline release regressions; no real transport."""
from dataclasses import replace
import json
import pytest
from modeltrace.config import Pricing
from tests.conftest import build_service
from tests.test_reconciliation_service import BatchReceiptStub, RecordingTransport, MODEL, run_one
from tests.test_receipts_reconciliation import FakeConnection, reader_for, ua
from tests.test_nonbillable_receipts import proof


def held(tmp_path, clock):
    reader, transport = BatchReceiptStub(), RecordingTransport()
    s, _, _ = build_service(tmp_path, clock, receipts=reader, transport=transport)
    s.config = replace(s.config, budget_basis='lei_group4_v1',
        pricing_upper_bound={MODEL: Pricing(2.5, 1.25, 15)},
        billing_prices={MODEL: Pricing(2.5, 1.25, 15)}, reconciliation_max_checks=1)
    run_one(s); clock.advance(300)
    return s, reader, transport


@pytest.mark.parametrize('damage', ['zero_billing', 'zero_reserve', 'low_reserve',
    'cancelled_sent', 'attempted_no_time', 'attempted_after_finish', 'invalid_uuid',
    'queue_monitor', 'queue_identity_error', 'prior_identity_error', 'prior_db_error',
    'nonbillable_exception', 'nonbillable_missing_method'])
@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_corrupt_or_unavailable_evidence_cannot_estimate(tmp_path, fake_clock, damage):
    s, reader, transport = held(tmp_path, fake_clock); db = s.db._conn
    if damage in ('zero_billing', 'zero_reserve', 'low_reserve'):
        column = 'billing_prices_json' if damage == 'zero_billing' else 'reserve_prices_json'
        prices = Pricing(0, 0, 0) if damage.startswith('zero') else Pricing(0.1, 0.1, 0.1)
        db.execute(f'update budget_reservations set {column}=?', (json.dumps(prices.as_dict()),))
    elif damage == 'cancelled_sent': db.execute("update reconciliation_probes set status='cancelled' where ordinal=2")
    elif damage == 'attempted_no_time': db.execute('update reconciliation_probes set started_at=NULL where ordinal=0')
    elif damage == 'attempted_after_finish': db.execute('update reconciliation_probes set started_at=? where ordinal=0', (fake_clock()+1,))
    elif damage == 'invalid_uuid': db.execute("update reconciliation_probes set user_agent='ModelTraceProbe/bad' where ordinal=0")
    elif damage == 'queue_monitor': db.execute('update queue set monitor_id=999')
    elif damage == 'queue_identity_error': db.execute("update queue set error_code='upstream_model_mismatch'")
    elif damage.startswith('prior_'):
        db.execute('update reconciliation_rounds set error_code=?',
            ('receipt_identity_mismatch' if damage == 'prior_identity_error' else 'receipt_query_failed',))
    elif damage == 'nonbillable_exception':
        def fail(agents): raise RuntimeError('database unavailable')
        reader.read_nonbillable_many = fail
    else: reader.read_nonbillable_many = None
    db.commit()
    for _ in range(2):
        assert s.reconcile_pending() == {'checked':1, 'held':1, 'settled':0, 'estimated':0}
        fake_clock.advance(300)
    assert db.execute('select count(*) from reservation_conservative_resolutions').fetchone()[0] == 0
    assert db.execute('select state from budget_reservations').fetchone()[0] == 'reserved'
    assert len(transport.calls) == 3


@pytest.mark.parametrize('stage', ['autocommit', 'enter', 'cursor', 'execute', 'fetchall', 'exit'])
def test_strict_nonbillable_query_errors_are_not_absence(stage):
    conn = FakeConnection([proof()], fail=stage); reader, _ = reader_for(conn)
    with pytest.raises(ValueError, match='nonbillable_query_failed'):
        reader.read_nonbillable_many([ua()], strict=True)
    assert conn.closed


@pytest.mark.parametrize('rows', [[proof(), proof()], [proof(evidence_id=True)],
    [proof(model='')], [proof(created_at=None)], [proof(ua(2))], [('wrong',)]])
def test_strict_nonbillable_malformed_rows_are_not_absence(rows):
    reader, _ = reader_for(FakeConnection(rows))
    with pytest.raises(ValueError, match='nonbillable_query_failed'):
        reader.read_nonbillable_many([ua()], strict=True)


def test_strict_nonbillable_successful_empty_query_is_absence():
    reader, _ = reader_for(FakeConnection([]))
    assert reader.read_nonbillable_many([ua()], strict=True) == {}


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_estimate_idempotence_restart_and_five_dollar_admission(tmp_path, fake_clock):
    s, reader, transport = held(tmp_path, fake_clock); db = s.db._conn
    amount = db.execute('select amount_usd from budget_reservations').fetchone()[0]
    db.execute('update budget_days set spent_usd=?', (5-amount,)); db.commit()
    assert s.reconcile_pending()['estimated'] == 1
    assert s.reconcile_pending()['checked'] == 0
    s.db.recover_running_jobs(now=fake_clock(), interval_seconds=s.config.interval_seconds)
    assert s.reconcile_pending()['checked'] == 0
    run_one(s)
    assert len(transport.calls) == 3
    b = s.db.budget_snapshot(now=fake_clock())
    assert b.spent_usd == pytest.approx(5) and b.reserved_usd == pytest.approx(0)
    assert db.execute('select count(*) from reservation_conservative_resolutions').fetchone()[0] == 1


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_cross_account_receipts_remain_confirmed_actual(tmp_path, fake_clock):
    from tests.test_reconciliation_service import receipt
    from modeltrace.receipts import ReceiptResult
    s, reader, transport = held(tmp_path, fake_clock)
    reader.queue_batch(lambda agents: {agent: ReceiptResult([
        replace(receipt(), account_id=str(23 if i == 0 else 7))], None)
        for i, agent in enumerate(agents)})
    assert s.reconcile_pending()['settled'] == 1
    row = s.db._conn.execute('select settlement_basis,settled_amount_usd from budget_reservations').fetchone()
    assert row['settlement_basis'] == 'confirmed_actual'
    assert row['settled_amount_usd'] == pytest.approx(3 * (2.5 + 15) / 1_000_000)
    assert s.db._conn.execute('select count(*) from reservation_conservative_resolutions').fetchone()[0] == 0
    assert len(transport.calls) == 3


# Reuse the complete stopped-baseline fixture without executing cutover's main.
from tests.test_detector_cutover_v019 import auto_transition


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_auto_increment_refuses_conserved_but_over_budget_day(auto_transition):
    before, after, evidence, verify = auto_transition
    before['budget_days']['2026-09-20']['spent_usd'] = 4.95
    after['budget_days']['2026-09-20']['spent_usd'] = 5.05
    with pytest.raises(AssertionError, match='daily budget exceeds'):
        verify(before, after, max_checks=12, receipt_evidence=evidence)


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_cutover_pins_reviewed_digest_before_any_mutation():
    from pathlib import Path
    script = (Path(__file__).parents[2] / 'deploy/cutover-detector-v019.py').read_text()
    assert script.index("assert image['Id']==expected_image_id") < script.index('backup.mkdir')
    assert "if not __debug__:" in script
    assert "assert identity('sub2api-modeltrace')['image_id']==expected_image_id" in script
    assert "(backup/'dependencies.json').write_text" in script


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_cutover_allows_unchanged_ledger_with_no_estimates():
    import copy
    from tests.test_detector_cutover_v019 import _load_cutover_ledger_helpers, _settlement_transition
    names, _, verify = _load_cutover_ledger_helpers()
    before, _ = _settlement_transition(names)
    assert verify(before, copy.deepcopy(before))['auto_estimated_reservations'] == []
