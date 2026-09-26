"""Accounts outside the host pool stay visible but are never probed, and the
channel view follows the same per-account rounds the account page shows."""

import pytest

from modeltrace.service import NotParticipating, utc_iso
from tests.test_per_account_d2 import MockHostClient, make_service

ASTRA = "gpt-5.6-sol"  # bank model configured for monitor 1 in make_service


def key(account_id, *, on=True, reason=None, cluster="c1"):
    row = {
        "account_id": account_id, "name": f"key-{account_id}", "platform": "openai",
        "type": "apikey", "schedulable": on, "models": [ASTRA],
        "cluster_id": cluster, "cluster_name": "Upstream",
    }
    if reason:
        row["inactive_reason"] = reason
    return row


@pytest.fixture
def host():
    return MockHostClient([key(5), key(7), key(23)])


@pytest.fixture
def service(tmp_path, fake_clock, host):
    svc = make_service(tmp_path, fake_clock, host_client=host)
    svc._refresh_accounts(fake_clock())
    yield svc
    svc.db.close()


def record(service, account_id, status, at):
    service.db.insert_account_round(
        account_id=account_id, model=ASTRA, status=status,
        message_code="compatible" if status == "match" else "upstream_error",
        checked_at=at,
    )


def test_switched_off_key_keeps_last_result_and_is_not_probed(service, host, fake_clock):
    record(service, 5, "match", fake_clock() - 60)
    host.accounts_data = [key(5, on=False, reason="unschedulable"), key(7), key(23)]
    service._refresh_accounts(fake_clock())

    snap = service.account_snapshot(7)
    assert snap["participating"] is True
    members = {m["account_id"]: m for m in snap["members"]}
    assert set(members) == {5, 7, 23}
    assert members[5]["participating"] is False
    assert members[5]["inactive_reason"] == "unschedulable"
    assert members[5]["latest_by_model"][ASTRA]["status"] == "match"
    assert members[7]["latest_by_model"][ASTRA] is None

    # Representative moves to the lowest key still in the pool.
    service.db.set_account_next_run(7, fake_clock(), now=fake_clock())
    service._schedule_due_accounts(fake_clock())
    queued = service.db._conn.execute(
        "SELECT account_id FROM account_queue WHERE state = 'queued'"
    ).fetchall()
    assert [int(r[0]) for r in queued] == [7]


def test_all_keys_off_shows_last_result_without_schedule(service, host, fake_clock):
    record(service, 7, "match", fake_clock() - 60)
    host.accounts_data = [key(i, on=False, reason="unschedulable") for i in (5, 7, 23)]
    service._refresh_accounts(fake_clock())

    listed = service.all_accounts_snapshot()["accounts"]
    assert len(listed) == 1, "the cluster stays listed instead of being retired"
    snap = listed[0]
    assert snap["participating"] is False
    assert snap["inactive_reason"] == "unschedulable"
    assert snap["schedule_mode"] == "manual"
    assert snap["next_run_at"] is None
    assert snap["latest"]["status"] == "match"

    with pytest.raises(NotParticipating):
        service.enqueue_manual_account(5, model=ASTRA)
    assert service._enqueue_model_on_all_accounts(ASTRA, now=fake_clock()) == 0
    summary = service.accounts_summary_for_model(ASTRA)
    assert summary["total"] == 0, "switched-off keys do not count for the channel"

    service.db.set_account_next_run(5, fake_clock(), now=fake_clock())
    service._schedule_due_accounts(fake_clock())
    assert not service.db.has_pending_account(5)


def test_removed_account_is_still_retired(service, host, fake_clock):
    host.accounts_data = [key(7), key(23)]
    service._refresh_accounts(fake_clock())
    assert service.db.get_account(5)["mode"] == "retired"
    snap = service.account_snapshot(7)
    assert 5 not in snap["member_account_ids"]


def test_switching_key_back_on_resumes_probing(service, host, fake_clock):
    host.accounts_data = [key(i, on=False, reason="unschedulable") for i in (5, 7, 23)]
    service._refresh_accounts(fake_clock())
    host.accounts_data = [key(5), key(7), key(23)]
    service._refresh_accounts(fake_clock())
    assert service.account_snapshot(5)["participating"] is True
    assert service.enqueue_manual_account(5, model=ASTRA).queued is True


def test_channel_view_follows_newest_account_round(service, fake_clock):
    old = fake_clock() - 5 * 3600
    service.db._conn.execute(
        "INSERT INTO rounds (monitor_id, status, checked_at, message_code, ranking_json, diagnostics_json) "
        "VALUES (1, 'error', ?, 'upstream_http_error', '[]', '{}')", (old,),
    )
    service.db._conn.execute(
        "UPDATE monitor_state SET last_checked_at = ? WHERE monitor_id = 1", (old,),
    )
    stale = service.snapshot(1)
    assert stale["latest"]["status"] == "error"
    assert stale["stale"] is True

    record(service, 7, "match", fake_clock() - 30)
    fresh = service.snapshot(1)
    assert fresh["latest"]["status"] == "match"
    assert fresh["last_checked_at"] == utc_iso(fake_clock() - 30)
    assert fresh["stale"] is False
    assert [r["status"] for r in fresh["history"]][-2:] == ["error", "match"]
    assert fresh["accounts_summary"]["match"] == 1


def test_channel_view_ignores_switched_off_keys(service, host, fake_clock):
    record(service, 5, "suspect", fake_clock() - 30)
    host.accounts_data = [key(5, on=False, reason="unschedulable"), key(7), key(23)]
    service._refresh_accounts(fake_clock())
    snap = service.snapshot(1)
    assert snap["latest"]["status"] != "suspect"
    assert snap["accounts_summary"]["suspect"] == 0
