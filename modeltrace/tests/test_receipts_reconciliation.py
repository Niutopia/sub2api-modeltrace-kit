from __future__ import annotations

import sys
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest

from modeltrace.config import Pricing
from modeltrace.receipts import RECEIPT_BATCH_QUERY, Receipt, ReceiptReader
from modeltrace.service import ModelTraceService
from tests.conftest import build_service


SECRET = "postgresql://private:password@private.invalid/db raw-private-response"


def ua(index=1):
    return f"ModelTraceProbe/{UUID(int=index)}"


def row(key, *, total="9.0", account="3.0"):
    return (key, "acct", "model", "12", "7", "2", total, account, "default", "timestamp", "0")


class FakeConnection:
    def __init__(self, rows=(), fail=None):
        self.rows = rows
        self.fail = fail
        self.calls = []
        self.closed = False
        self._autocommit = False

    def check(self, stage):
        if self.fail == stage:
            raise RuntimeError(SECRET)

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.check("autocommit")
        self._autocommit = value

    def __enter__(self):
        self.check("enter")
        return self

    def __exit__(self, *args):
        self.check("exit")

    def cursor(self):
        self.check("cursor")
        return self

    def execute(self, query, params):
        self.calls.append((query, params))
        self.check("execute")

    def fetchall(self):
        self.check("fetchall")
        return self.rows

    def close(self):
        self.closed = True
        self.check("close")


def reader_for(connection, *, fail_connect=False):
    connects = []

    def connect(url, timeout):
        connects.append((url, timeout))
        if fail_connect:
            raise RuntimeError(SECRET)
        return connection

    def no_wait(*args):
        pytest.fail("batch must not poll, sleep, or consult a wait deadline")

    return ReceiptReader(SECRET, connect_factory=connect, sleeper=no_wait, clock=no_wait), connects


def test_batch_parameterization_grouping_duplicates_and_missing():
    duplicate = row(ua(2))
    connection = FakeConnection([duplicate, row(ua(1)), duplicate])
    reader, connects = reader_for(connection)
    results = reader.read_many([ua(1), ua(2), ua(1), ua(3)])
    assert connects == [(SECRET, 2.0)]
    assert connection.calls == [(RECEIPT_BATCH_QUERY, ([ua(1), ua(2), ua(3)],))]
    assert " ".join(RECEIPT_BATCH_QUERY.split()) == (
        "SELECT user_agent, account_id, model, input_tokens,output_tokens,cache_read_tokens,"
        "total_cost,account_stats_cost,service_tier,created_at,cache_creation_tokens "
        "FROM modeltrace_monitoring.probe_receipts WHERE user_agent = ANY(%s)"
    )
    assert all(key not in RECEIPT_BATCH_QUERY for key in results)
    assert list(results) == [ua(1), ua(2), ua(3)]
    assert results[ua(1)].receipts == [Receipt("acct", "model", 12, 7, 2, 9.0, 3.0, "default", "timestamp")]
    assert results[ua(1)].error_code is None
    assert len(results[ua(2)].receipts) == 2
    assert sum(receipt.charge_usd for receipt in results[ua(2)].receipts) == 6
    assert results[ua(3)].receipts == []
    assert results[ua(3)].error_code == "receipt_missing"
    assert connection.autocommit and connection.closed




@pytest.mark.parametrize("value", [True, False, 1.5, "1.5", "abc", " 1", "1 "])
def test_safe_int_rejects_bool_decimal_and_non_integer_text(value):
    assert ReceiptReader._row_to_receipt(
        ["acct", "model", value, 2, 0, 1, 1, "default", None, 0]
    ).input_tokens is None


@pytest.mark.parametrize("value", [True, False])
def test_safe_float_rejects_bool(value):
    receipt = ReceiptReader._row_to_receipt(
        ["acct", "model", 1, 2, 0, 1, value, "default", None, 0]
    )
    assert receipt.charge_usd is None


@pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")
def test_reader_always_reads_cache_creation_after_legacy_config_restart_semantics(tmp_path, fake_clock):
    # Create a real Lei reservation with its durable three-UUID reconciliation
    # rows, then simulate a restart that loads the legacy config.  The reader
    # flag is deliberately false, but the SQL contract remains complete and
    # Lei settlement still sees non-zero cache creation and holds the money.
    service, _, _ = build_service(tmp_path, fake_clock)
    agents = [ua(7), ua(8), ua(9)]
    _, queue_id = service.db.enqueue(1, now=fake_clock(), trigger="test")
    job = service.db.claim_next(now=fake_clock())
    assert job is not None
    reservation = service.db.reserve_budget(
        1,
        queue_id,
        0.25,
        daily_budget_usd=5,
        now=fake_clock(),
        budget_basis="lei_group4_v1",
        billing_prices={
            "input_per_million_usd": 1,
            "cache_read_per_million_usd": 2,
            "output_per_million_usd": 3,
        },
        reserve_prices={
            "input_per_million_usd": 1,
            "cache_read_per_million_usd": 2,
            "output_per_million_usd": 3,
        },
        probe_model="gpt-5.4",
        probe_user_agents=agents,
    )
    assert reservation is not None
    assert service.db._conn.execute(
        "SELECT count(*) FROM reconciliation_probes WHERE reservation_id = ?",
        (reservation.reservation_id,),
    ).fetchone()[0] == 3

    key = agents[0]
    connection = FakeConnection([
        (agent, "acct", "gpt-5.4", 100, 20, 3, 999, 999, "default", None, 1)
        for agent in agents
    ])
    connects = []

    def connect(url, timeout):
        connects.append((url, timeout))
        return connection

    reader = ReceiptReader(
        SECRET,
        connect_factory=connect,
        include_cache_creation_tokens=False,
        sleeper=lambda *_: None,
        clock=lambda: 0.0,
    )
    # The restarted process has the old global config, but the persisted
    # reservation still selects the Lei formula.
    service.config = replace(service.config, budget_basis=None, billing_prices={})
    service.receipt_reader = reader
    for ordinal in range(3):
        assert service.db.mark_probe_attempted(reservation.reservation_id, ordinal, now=fake_clock())
    service.db.record_round_and_finish(
        job,
        status="error",
        target_probability=None,
        best_model=None,
        checked_at=fake_clock(),
        message_code="transport_error",
        ranking=[],
        diagnostics={},
        next_run_at=None,
        next_run_after=None,
        upstream_statuses=[],
        receipt_count=0,
        receipt_consistent=None,
        actual_cost_usd=None,
        reserved_usd=reservation.amount_usd,
    )
    fake_clock.advance(301)
    result = service.reconcile_pending()

    assert result == {"checked": 1, "settled": 0, "estimated": 0, "held": 1}
    assert connection.calls == [(RECEIPT_BATCH_QUERY, (agents,))]
    assert all(receipt.cache_creation_tokens == 1 for receipt in [
        ReceiptReader._row_to_receipt(row[1:]) for row in connection.rows
    ])
    assert service.db.budget_snapshot(now=fake_clock()).spent_usd == 0
    assert service.db.budget_snapshot(now=fake_clock()).reserved_usd == 0.25


@pytest.mark.parametrize("invalid", [
    "", "ModelTraceProbe/local-test", "modeltraceprobe/" + str(UUID(int=1)),
    ua() + "\n", " " + ua(), ua() + "' OR 1=1 --", str(UUID(int=1)),
    "ModelTraceProbe/" + UUID(int=1).hex,
    "ModelTraceProbe/{" + str(UUID(int=1)) + "}",
])
def test_invalid_identifiers_never_reach_database(invalid):
    connection = FakeConnection()
    reader, connects = reader_for(connection)
    result = reader.read_many([invalid])
    assert result[invalid].error_code == "receipt_query_failed"
    assert result[invalid].receipts == []
    assert not connects
    mixed = reader.read_many([invalid, ua()])
    assert mixed[invalid].error_code == "receipt_query_failed"
    assert mixed[ua()].error_code == "receipt_missing"
    assert connection.calls == [(RECEIPT_BATCH_QUERY, ([ua()],))]


def test_exact_case_is_preserved_not_uuid_normalized():
    upper = "ModelTraceProbe/ABCDEFAB-1234-1234-1234-ABCDEFABCDEF"
    lower = upper.replace("ABCDEF", "abcdef")
    connection = FakeConnection([row(upper)])
    reader, _ = reader_for(connection)
    results = reader.read_many([upper, lower])
    assert connection.calls == [(RECEIPT_BATCH_QUERY, ([upper, lower],))]
    assert results[upper].error_code is None
    assert results[lower].error_code == "receipt_missing"


def test_empty_input_does_not_connect():
    reader, connects = reader_for(FakeConnection())
    assert reader.read_many([]) == {}
    assert connects == []


def test_limit_applies_after_deduplication_and_never_truncates():
    reader, connects = reader_for(FakeConnection())
    keys = [ua(i) for i in range(60)]
    results = reader.read_many(keys * 2)
    assert len(results) == 60
    assert all(result.error_code == "receipt_missing" for result in results.values())
    assert len(connects) == 1
    results = reader.read_many(keys + [ua(60)])
    assert len(results) == 61
    assert all(result.error_code == "receipt_query_failed" for result in results.values())
    assert len(connects) == 1


def test_unconfigured_database_is_explicit():
    results = ReceiptReader().read_many([ua(), "invalid"])
    assert results[ua()].error_code == "receipt_db_unconfigured"
    assert results["invalid"].error_code == "receipt_query_failed"


@pytest.mark.parametrize("stage", ["connect", "autocommit", "enter", "cursor", "execute", "fetchall", "exit"])
def test_failures_are_closed_and_do_not_expose_exception_text(stage, capsys, caplog):
    connection = FakeConnection([row(ua())], fail=stage)
    reader, connects = reader_for(connection, fail_connect=stage == "connect")
    results = reader.read_many([ua(), ua(2)])
    assert all(result.error_code == "receipt_query_failed" and not result.receipts for result in results.values())
    assert len(connects) == 1
    assert connection.closed == (stage != "connect")
    captured = capsys.readouterr()
    assert SECRET not in repr(results) + captured.out + captured.err + caplog.text


@pytest.mark.parametrize("bad_row", [(), row(ua(99)), row(ua())[:-1], row(ua()) + (SECRET,)])
def test_malformed_or_unrequested_row_discards_partial_success(bad_row):
    reader, _ = reader_for(FakeConnection([row(ua()), bad_row]))
    result = reader.read_many([ua()])[ua()]
    assert result.error_code == "receipt_query_failed"
    assert result.receipts == []


@pytest.mark.parametrize("invalid", ["NaN", float("nan"), "Infinity", -1, "-0.01", "invalid", None])
def test_invalid_costs_use_existing_safe_parser(invalid):
    reader, _ = reader_for(FakeConnection([row(ua(), total=invalid, account=invalid)]))
    receipt = reader.read_many([ua()])[ua()].receipts[0]
    assert receipt.total_cost is None
    assert receipt.account_stats_cost is None
    assert receipt.charge_usd is None


def test_total_cost_fallback_and_zero_cost():
    reader, _ = reader_for(FakeConnection([row(ua(), total="0", account=None)]))
    assert reader.read_many([ua()])[ua()].receipts[0].charge_usd == 0


def test_production_connector_timeout_configuration_without_network(monkeypatch):
    calls = []
    connection = FakeConnection()

    def connect(*args, **kwargs):
        calls.append((args, kwargs))
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))
    result = ReceiptReader(SECRET, wait_seconds=999).read_many([ua()])
    assert calls == [((SECRET,), {"connect_timeout": 2, "options": "-c statement_timeout=2000"})]
    assert result[ua()].error_code == "receipt_missing"
    assert len(connection.calls) == 1
    assert connection.closed


def test_close_failure_does_not_expose_exception(capsys):
    reader, _ = reader_for(FakeConnection([row(ua())], fail="close"))
    assert reader.read_many([ua()])[ua()].error_code is None
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
