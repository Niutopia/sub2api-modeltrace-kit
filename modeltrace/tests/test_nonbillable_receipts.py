import pytest
from tests.test_receipts_reconciliation import FakeConnection, reader_for, ua


def proof(key=None, **kw):
    values = dict(user_agent=key or ua(), evidence_id=123, model='gpt-5.6-luna',
                  created_at='2026-09-20T01:00:00Z', evidence_kind='gateway_routing_not_dispatched')
    values.update(kw)
    return tuple(values.values())


def test_exact_proof_query_is_restricted_and_parameterized():
    c = FakeConnection([proof()]); r, calls = reader_for(c)
    result = r.read_nonbillable_many([ua()])
    assert result[ua()]['evidence_id'] == 123
    assert 'modeltrace_monitoring.nonbillable_probe_receipts' in c.calls[0][0]
    assert c.calls[0][1] == ([ua()],)
    assert ua() not in c.calls[0][0]
    assert c.closed


@pytest.mark.parametrize('stage', ['autocommit', 'enter', 'cursor', 'execute', 'fetchall', 'exit'])
def test_query_failure_never_becomes_zero_cost_evidence(stage):
    c = FakeConnection([proof()], fail=stage); r, _ = reader_for(c)
    assert r.read_nonbillable_many([ua()]) == {}
    assert c.closed


@pytest.mark.parametrize('rows', [[], [proof(), proof()], [proof(evidence_id=True)],
    [proof(evidence_id=0)], [proof(model='')], [proof(created_at=None)],
    [proof(evidence_kind='upstream_503')], [proof(ua(2))], [('wrong',)]])
def test_missing_ambiguous_or_invalid_proofs_fail_closed(rows):
    r, _ = reader_for(FakeConnection(rows))
    assert r.read_nonbillable_many([ua()]) == {}


@pytest.mark.parametrize('keys', [[], ['ModelTraceProbe/not-a-uuid'], [ua(i) for i in range(1, 62)]])
def test_invalid_batch_does_not_connect(keys):
    r, calls = reader_for(FakeConnection())
    assert r.read_nonbillable_many(keys) == {}
    assert calls == []
