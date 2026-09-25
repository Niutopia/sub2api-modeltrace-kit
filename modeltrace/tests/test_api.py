from __future__ import annotations

import pytest

from app import create_app
from modeltrace.config import ConfigError


def test_all_routes_require_bearer(tmp_path, fake_clock):
    from tests.conftest import build_service

    service, _, _ = build_service(tmp_path, fake_clock)
    app = create_app(service, secret="secret", start_worker=False)
    client = app.test_client()

    assert client.get("/health").status_code == 401
    assert client.get("/v1/monitors/1").status_code == 401
    assert client.post("/v1/monitors/1/run").status_code == 401
    assert client.get("/v1/admin/monitors/1").status_code == 401


def test_missing_auth_secret_fails_before_worker_start(monkeypatch, tmp_path, fake_clock):
    from tests.conftest import build_service

    service, _, _ = build_service(tmp_path, fake_clock, enabled=True)
    started = False

    def mark_started():
        nonlocal started
        started = True

    service.start = mark_started
    monkeypatch.delenv("MODELTRACE_SECRET", raising=False)

    with pytest.raises(ConfigError, match="MODELTRACE_SECRET"):
        create_app(service, start_worker=True)

    assert started is False


def test_missing_auth_secret_fails_before_service_construction(monkeypatch):
    def should_not_construct(*args, **kwargs):
        raise AssertionError("service construction must not run without auth secret")

    monkeypatch.setattr("app.ModelTraceService", should_not_construct)
    monkeypatch.delenv("MODELTRACE_SECRET", raising=False)

    with pytest.raises(ConfigError, match="MODELTRACE_SECRET"):
        create_app(start_worker=False)


def test_public_shape_and_queue_duplicate(tmp_path, fake_clock):
    from tests.conftest import build_service

    service, _, _ = build_service(tmp_path, fake_clock)
    app = create_app(service, secret="secret", start_worker=False)
    client = app.test_client()
    headers = {"Authorization": "Bearer secret"}

    response = client.get("/v1/monitors/1", headers=headers)
    assert response.status_code == 200
    body = response.get_json()
    assert body["monitor_id"] == 1
    assert body["model"] == "gpt-5.4"
    assert body["protocol"] == "responses"
    assert body["reasoning_effort"] == "none"
    assert body["service_tier"] == "default"
    assert body["latest"]["status"] == "not_tested"
    assert body["history"] == []
    assert body["next_run_after"] is None

    missing_model = client.post("/v1/monitors/1/run", headers=headers)
    assert missing_model.status_code == 400
    assert missing_model.get_json() == {"queued": False, "message_code": "expected_model_required"}
    mismatched = client.post(
        "/v1/monitors/1/run",
        headers=headers,
        json={"expected_model": "gpt-5.6-luna"},
    )
    assert mismatched.status_code == 409
    assert mismatched.get_json() == {"queued": False, "message_code": "expected_model_mismatch"}
    queued = client.post(
        "/v1/monitors/1/run",
        headers=headers,
        json={"expected_model": "gpt-5.4"},
    )
    assert queued.status_code == 202
    assert queued.get_json() == {"queued": True}
    duplicate = client.post(
        "/v1/monitors/1/run",
        headers=headers,
        json={"expected_model": "gpt-5.4"},
    )
    assert duplicate.status_code == 409
    assert duplicate.get_json()["message_code"] == "duplicate"

    admin = client.get("/v1/admin/monitors/1", headers=headers)
    assert admin.status_code == 200
    admin_body = admin.get_json()
    assert "diagnostics" in admin_body
    assert admin_body["diagnostics"]["cost_controls_enabled"] is False
    assert "daily_budget_usd" not in admin_body["diagnostics"]
