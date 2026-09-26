"""Offline regressions for authoritative host account/model synchronization."""
from copy import deepcopy
from dataclasses import replace
import json
import socket

import httpx
import pytest

from modeltrace.db import ModelTraceDB
from modeltrace.service import HostClient, ModelNotSupported, utc_iso
from tests.conftest import build_service


ASTRA = "gpt-6-astra"
LUNA = "gpt-5.6-luna"
WHITELIST = [
    "codex-auto-review", "gpt-5.5", LUNA, "gpt-5.6-sol", "gpt-5.6-terra",
    ASTRA, "gpt-reserve",
]


class OfflineHost:
    def __init__(self, accounts):
        self.accounts = accounts
        self.calls = []
        self.ok = True

    def fetch_accounts(self, models):
        self.calls.append(list(models))
        return deepcopy(self.accounts), self.ok


def account(account_id=42, models=None, **extra):
    return {
        "account_id": account_id, "name": "231", "platform": "openai",
        "type": "apikey", "schedulable": True,
        "models": [ASTRA] if models is None else models,
        "real_models_10m": [ASTRA], "paused_models": [], **extra,
    }


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("account sync regression tests must not access the network")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.delenv("HOST_API_BASE", raising=False)


@pytest.fixture
def service(tmp_path, fake_clock):
    svc, _, _ = build_service(tmp_path, fake_clock, enabled=True)
    svc.host_client = OfflineHost([account()])
    svc._refresh_accounts(fake_clock())
    yield svc
    svc.db.close()


def test_authoritative_astra_only_does_not_invent_other_bank_models(service):
    snapshot = service.account_snapshot(42)
    assert json.loads(service.db.get_account(42)["models_json"]) == [ASTRA]
    assert snapshot["models"] == [ASTRA]
    assert [row["model"] for row in snapshot["per_model"]] == [ASTRA]
    assert {ASTRA, LUNA, "gpt-5.5", "gpt-5.6-sol", "gpt-5.6-terra"} <= set(service.host_client.calls[-1])
    assert service.transport.calls == []


def test_whitelist_filters_only_missing_baselines_not_recent_activity(service, fake_clock):
    service.host_client.accounts = [account(models=WHITELIST)]
    service._refresh_accounts(fake_clock())
    expected = [m for m in WHITELIST if m in service.bank_models]
    assert len(expected) == 5
    assert service.account_snapshot(42)["models"] == expected
    assert json.loads(service.db.get_account(42)["models_json"]) == expected
    for unsupported in ("codex-auto-review", "gpt-reserve"):
        with pytest.raises(ModelNotSupported):
            service.enqueue_manual_account(42, model=unsupported)
    assert service.transport.calls == []


def test_upsert_replaces_removed_and_empty_models(service, fake_clock):
    for models in ([ASTRA, LUNA], [LUNA], []):
        service.host_client.accounts = [account(models=models)]
        service._refresh_accounts(fake_clock())
        assert json.loads(service.db.get_account(42)["models_json"]) == models
        assert service.account_snapshot(42)["models"] == models
    assert service.account_snapshot(42)["next_model"] is None


def test_explicit_alias_preserves_route_identity(service, fake_clock):
    service.config = replace(service.config, model_aliases={"luna-route": LUNA})
    service.host_client.accounts = [account(models=[ASTRA, "luna-route", "gpt-reserve"])]
    service._refresh_accounts(fake_clock())
    snapshot = service.account_snapshot(42)
    assert snapshot["models"] == [ASTRA, "luna-route"]
    assert snapshot["per_model"][1]["calibration_model"] == LUNA
    assert "luna-route" in service.host_client.calls[-1]


def test_host_query_encodes_model_aliases_without_changing_identity():
    models = [ASTRA, "luna+canary", "route&preview=1", "sol/review"]

    def respond(request):
        assert request.url.params["models"].split(",") == models
        assert request.url.params["include_inactive"] == "1"
        assert set(request.url.params) == {"models", "include_inactive"}
        assert request.headers["Authorization"] == "Bearer offline-secret"
        return httpx.Response(200, json={"accounts": [account()]})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        fetched, ok = HostClient("https://host.invalid", "offline-secret", client).fetch_accounts(models)
    assert ok is True
    assert fetched[0]["account_id"] == 42


def test_failed_fetch_does_not_replay_cached_models_or_local_pause(service, fake_clock):
    # A successful pause is stored locally before the next host refresh. A
    # failed HTTP fetch must not replay HostClient's older unpaused payload.
    service._update_member_paused_model(42, ASTRA, utc_iso(fake_clock() + 3600))
    service.db._conn.execute("UPDATE accounts SET models_json = ? WHERE account_id = 42", (json.dumps([ASTRA, LUNA]),))
    before = dict(service.db.get_account(42))
    service.host_client.ok = False
    fake_clock.advance(60)
    service._refresh_accounts(fake_clock())
    assert dict(service.db.get_account(42)) == before


@pytest.mark.parametrize("read", ["single", "all", "summary"])
def test_stale_reads_refresh_even_while_probe_worker_is_busy(service, fake_clock, read):
    service.host_client.accounts = [account(models=[ASTRA, LUNA])]
    fake_clock.advance(61)
    if read == "single":
        snapshot = service.account_snapshot(42, force_refresh=True)
    elif read == "all":
        snapshot = service.all_accounts_snapshot(force_refresh=True)["accounts"][0]
    else:
        summary = service.accounts_summary_for_model(LUNA)
        assert summary["total"] == 1
        snapshot = service._account_snapshot(42)
    assert snapshot["models"] == [ASTRA, LUNA]
    assert service.host_client.calls and len(service.host_client.calls) == 2
    assert service.transport.calls == []


def test_manual_admission_refreshes_recently_changed_whitelist(service, fake_clock):
    # A change immediately after a background sync must not cause a spurious
    # unsupported-model error when the user explicitly requests a check.
    service.host_client.accounts = [account(models=[ASTRA, LUNA])]
    result = service.enqueue_manual_account(42, model=LUNA, force_refresh=True)
    assert result.model == LUNA
    assert service.db.get_pending_account_job(42)["model"] == LUNA
    assert service.transport.calls == []


def test_manual_admission_rejects_just_removed_model(service):
    service.host_client.accounts = [account(models=[LUNA])]
    with pytest.raises(ModelNotSupported):
        service.enqueue_manual_account(42, model=ASTRA, force_refresh=True)
    assert service.db.get_pending_account_job(42) is None


def test_sync_batch_is_atomic_for_other_sqlite_readers(service, fake_clock, monkeypatch):
    service.host_client.accounts = [account(cluster_id="upstream"), account(43, cluster_id="upstream")]
    service._refresh_accounts(fake_clock())
    other = ModelTraceDB(service.db.path)
    observations = []
    original = service.db.upsert_account_sync

    def observe(**kwargs):
        original(**kwargs)
        observations.append([json.loads(row["models_json"]) for row in other.get_all_accounts()])

    monkeypatch.setattr(service.db, "upsert_account_sync", observe)
    service.host_client.accounts = [account(models=[LUNA], cluster_id="upstream"), account(43, models=[LUNA], cluster_id="upstream")]
    try:
        service._refresh_accounts(fake_clock())
        assert observations == [[[ASTRA], [ASTRA]], [[ASTRA], [ASTRA]]]
        assert [json.loads(row["models_json"]) for row in other.get_all_accounts()] == [[LUNA], [LUNA]]
    finally:
        other.close()


@pytest.mark.parametrize("read", ["single", "all"])
def test_save_then_get_reflects_models_immediately_without_clock_advance(service, read):
    # Same timestamp as the preceding background fetch: neither a 30-second
    # frontend poll nor the worker's 60-second interval may delay this read.
    service.host_client.accounts = [account(models=[ASTRA, LUNA])]
    snapshot = (service.account_snapshot(42, force_refresh=True) if read == "single"
                else service.all_accounts_snapshot(force_refresh=True)["accounts"][0])
    assert snapshot["models"] == [ASTRA, LUNA]
    assert [row["model"] for row in snapshot["per_model"]] == [ASTRA, LUNA]
    assert len(service.host_client.calls) == 2
    assert service.transport.calls == []


@pytest.mark.parametrize("read", ["single", "all"])
def test_cluster_save_then_get_refreshes_members_and_union_immediately(service, read):
    service.host_client.accounts = [
        account(models=[ASTRA], cluster_id="upstream"),
        account(43, models=[LUNA], cluster_id="upstream"),
    ]
    snapshot = (service.account_snapshot(43, force_refresh=True) if read == "single"
                else service.all_accounts_snapshot(force_refresh=True)["accounts"][0])
    assert snapshot["models"] == sorted([ASTRA, LUNA])
    assert snapshot["member_account_ids"] == [42, 43]
    assert {m["account_id"]: m["models"] for m in snapshot["members"]} == {
        42: [ASTRA], 43: [LUNA],
    }
    assert len(service.host_client.calls) == 2
    assert service.transport.calls == []


def test_invalid_sync_rolls_back_entire_batch(service, fake_clock):
    before = dict(service.db.get_account(42))
    service.host_client.accounts = [
        account(models=[LUNA]),
        account(43, real_requests_10m="malformed-count"),
    ]
    service._refresh_accounts(fake_clock())
    assert dict(service.db.get_account(42)) == before
    assert service.db.get_account(43) is None
    assert service.db._conn.in_transaction is False


def test_failed_force_refresh_keeps_local_pause_with_real_host_cache(service, fake_clock):
    replies = [httpx.Response(200, json={"accounts": [account()]}), httpx.Response(503)]
    with httpx.Client(transport=httpx.MockTransport(lambda request: replies.pop(0))) as client:
        service.host_client = HostClient("https://host.invalid", client=client)
        service._refresh_accounts(fake_clock())
        until = utc_iso(fake_clock() + 3600)
        service._update_member_paused_model(42, ASTRA, until)
        snapshot = service.account_snapshot(42, force_refresh=True)
    assert snapshot["models"] == [ASTRA]
    assert snapshot["per_model"][0]["paused_until"] == until
    assert service.transport.calls == []


def test_monitor_summary_refresh_commits_before_entering_read_transaction(service, fake_clock):
    service.host_client.accounts = [account(models=[ASTRA, "gpt-5.4"])]
    fake_clock.advance(61)
    snapshot = service.snapshot(1)
    assert snapshot["accounts_summary"]["total"] == 1
    assert json.loads(service.db.get_account(42)["models_json"]) == [ASTRA, "gpt-5.4"]
    assert service.db._conn.in_transaction is False


@pytest.mark.parametrize("path", ["/accounts", "/accounts/42", "/v1/accounts", "/v1/accounts/42"])
def test_existing_http_get_immediately_reads_saved_whitelist_without_query(service, path):
    from app import create_app

    client = create_app(service, secret="offline-secret", start_worker=False).test_client()
    service.host_client.accounts = [account(models=[ASTRA, LUNA])]
    response = client.get(path, headers={"Authorization": "Bearer offline-secret"})
    assert response.status_code == 200
    payload = response.get_json()
    snapshot = payload["accounts"][0] if "accounts" in payload else payload
    assert snapshot["models"] == [ASTRA, LUNA]
    assert len(service.host_client.calls) == 2
    assert service.transport.calls == []


def test_slow_host_is_bounded_and_late_result_is_never_applied(service, monkeypatch):
    import threading
    import time

    release = threading.Event()
    started = threading.Event()

    def slow_fetch(models):
        started.set()
        release.wait(2)
        return [account(models=[LUNA])], True

    service.host_client.fetch_accounts = slow_fetch
    monkeypatch.setattr("modeltrace.service.ACCOUNT_SYNC_TIMEOUT_SECONDS", 0.05)
    before = dict(service.db.get_account(42))
    start = time.monotonic()
    try:
        snapshot = service.account_snapshot(42, force_refresh=True)
        assert time.monotonic() - start < 0.4
        assert started.is_set()
        assert snapshot["models"] == [ASTRA]
        assert dict(service.db.get_account(42)) == before
    finally:
        release.set()
        service._account_fetch_thread.join(1)
    assert dict(service.db.get_account(42)) == before
    service.host_client.fetch_accounts = lambda models: ([account(models=[ASTRA, LUNA])], True)
    assert service.account_snapshot(42, force_refresh=True)["models"] == [ASTRA, LUNA]


def test_busy_refresh_lock_does_not_exceed_get_budget(service, monkeypatch):
    import time

    monkeypatch.setattr("modeltrace.service.ACCOUNT_SYNC_TIMEOUT_SECONDS", 0.05)
    service._account_refresh_lock.acquire()
    started = time.monotonic()
    try:
        snapshot = service.account_snapshot(42, force_refresh=True)
        assert time.monotonic() - started < 0.4
        assert snapshot["models"] == [ASTRA]
        assert len(service.host_client.calls) == 1
    finally:
        service._account_refresh_lock.release()


def test_busy_sqlite_writer_does_not_exceed_get_budget(service, monkeypatch):
    import time

    monkeypatch.setattr("modeltrace.service.ACCOUNT_SYNC_TIMEOUT_SECONDS", 0.05)
    service.host_client.accounts = [account(models=[LUNA])]
    other = ModelTraceDB(service.db.path)
    other._conn.execute("BEGIN IMMEDIATE")
    start = time.monotonic()
    try:
        snapshot = service.account_snapshot(42, force_refresh=True)
        assert time.monotonic() - start < 0.4
        assert snapshot["models"] == [ASTRA]
        assert service.db._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
    finally:
        other._conn.execute("ROLLBACK")
        other.close()
    assert service.account_snapshot(42, force_refresh=True)["models"] == [LUNA]


def test_cluster_admission_is_atomic_across_sqlite_connections(service, fake_clock):
    from concurrent.futures import ThreadPoolExecutor
    import threading

    service.host_client.accounts = [account(cluster_id="upstream"), account(43, cluster_id="upstream")]
    service._refresh_accounts(fake_clock())
    other = ModelTraceDB(service.db.path)
    ready = threading.Barrier(2)

    def enqueue(db, account_id):
        ready.wait(timeout=2)
        return db.enqueue_account(account_id, ASTRA, now=fake_clock(), trigger="manual")

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(enqueue, service.db, 42)
            b = pool.submit(enqueue, other, 43)
            results = [a.result(timeout=3), b.result(timeout=3)]
        assert sorted(ok for ok, _ in results) == [False, True]
        assert results[0][1] == results[1][1]
    finally:
        other.close()


@pytest.mark.parametrize("path", ["/accounts/42/run", "/v1/accounts/42/run"])
@pytest.mark.parametrize("saved_models,requested_model,status", [
    ([ASTRA, LUNA], LUNA, 202),
    ([LUNA], ASTRA, 400),
    ([ASTRA, "gpt-reserve"], "gpt-reserve", 400),
])
def test_http_manual_post_uses_saved_whitelist_without_query(service, path, saved_models, requested_model, status):
    from app import create_app

    client = create_app(service, secret="offline-secret", start_worker=False).test_client()
    service.host_client.accounts = [account(models=saved_models)]
    response = client.post(path, json={"model": requested_model}, headers={"Authorization": "Bearer offline-secret"})
    assert response.status_code == status
    if status == 202:
        assert response.get_json()["model"] == requested_model
    else:
        assert response.get_json()["error"] == "model_not_supported"
        assert service.db.get_pending_account_job(42) is None
    assert len(service.host_client.calls) == 2
    assert service.transport.calls == []


def test_http_empty_successful_snapshot_is_authoritative_deletion(service):
    from app import create_app

    client = create_app(service, secret="offline-secret", start_worker=False).test_client()
    service.host_client.accounts = []
    response = client.get("/accounts", headers={"Authorization": "Bearer offline-secret"})
    assert response.status_code == 200
    assert response.get_json()["accounts"] == []
    assert service.db.get_account(42)["mode"] == "retired"


def test_internal_snapshot_does_not_overwrite_local_pause_or_fetch_host(service, fake_clock):
    until = utc_iso(fake_clock() + 3600)
    service._update_member_paused_model(42, ASTRA, until)
    snapshot = service.account_snapshot(42)
    assert snapshot["per_model"][0]["paused_until"] == until
    assert len(service.host_client.calls) == 1
