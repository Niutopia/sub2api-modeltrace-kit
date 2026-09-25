from __future__ import annotations

import hmac
import os
from typing import Any

from flask import Flask, jsonify, request

from modeltrace.config import ConfigError, load_config
from modeltrace.service import (
    DuplicateQueue,
    ExpectedModelMismatch,
    ModelNotSupported,
    ModelTraceService,
    TooSoon,
    UnknownMonitor,
    UnknownAccount,
)


def create_app(
    service: ModelTraceService | None = None,
    *,
    secret: str | None = None,
    start_worker: bool = True,
) -> Flask:
    auth_secret = os.environ.get("MODELTRACE_SECRET") if secret is None else secret
    if not isinstance(auth_secret, str) or not auth_secret.strip():
        # Fail before constructing the service or starting its worker.  An
        # enabled deployment without an authentication secret must not be able
        # to reach the paid probe path while every request is merely rejected.
        raise ConfigError("MODELTRACE_SECRET must be set before starting modeltrace-service")

    app = Flask(__name__)
    app.config["JSON_SORT_KEYS"] = False
    modeltrace = service
    if modeltrace is None:
        modeltrace = ModelTraceService(load_config())
    app.extensions["modeltrace_service"] = modeltrace

    @app.before_request
    def require_bearer() -> Any:
        authorization = request.headers.get("Authorization", "")
        supplied = authorization[7:] if authorization.startswith("Bearer ") else ""
        if not auth_secret or not hmac.compare_digest(supplied, auth_secret):
            return jsonify({"error": "unauthorized"}), 401
        return None

    @app.get("/health")
    def health() -> Any:
        return jsonify(modeltrace.health())

    @app.get("/v1/monitors/<int:monitor_id>")
    def public_monitor(monitor_id: int) -> Any:
        try:
            return jsonify(modeltrace.snapshot(monitor_id, admin=False))
        except UnknownMonitor:
            return jsonify({"error": "not_found"}), 404

    @app.post("/v1/monitors/<int:monitor_id>/run")
    def run_monitor(monitor_id: int) -> Any:
        payload = request.get_json(silent=True)
        expected_model = payload.get("expected_model") if isinstance(payload, dict) else None
        if not isinstance(expected_model, str) or not expected_model:
            return jsonify({"queued": False, "message_code": "expected_model_required"}), 400
        try:
            result = modeltrace.enqueue_manual(monitor_id, expected_model=expected_model)
        except UnknownMonitor:
            return jsonify({"error": "not_found"}), 404
        except ExpectedModelMismatch:
            return jsonify({"queued": False, "message_code": "expected_model_mismatch"}), 409
        except DuplicateQueue:
            return jsonify({"queued": False, "message_code": "duplicate"}), 409
        except TooSoon as exc:
            response = jsonify(
                {
                    "queued": False,
                    "message_code": "manual_trigger_too_soon",
                    "retry_after_seconds": exc.retry_after,
                }
            )
            response.status_code = 429
            response.headers["Retry-After"] = str(exc.retry_after)
            return response
        return jsonify({"queued": result.queued}), 202

    @app.get("/v1/admin/monitors/<int:monitor_id>")
    def admin_monitor(monitor_id: int) -> Any:
        try:
            return jsonify(modeltrace.snapshot(monitor_id, admin=True))
        except UnknownMonitor:
            return jsonify({"error": "not_found"}), 404


    @app.get("/accounts")
    @app.get("/v1/accounts")
    def list_accounts() -> Any:
        return jsonify(modeltrace.all_accounts_snapshot())

    @app.get("/accounts/<int:account_id>")
    @app.get("/v1/accounts/<int:account_id>")
    def get_account_detail(account_id: int) -> Any:
        try:
            return jsonify(modeltrace.account_snapshot(account_id))
        except UnknownAccount:
            return jsonify({"error": "not_found"}), 404

    @app.post("/accounts/<int:account_id>/reset")
    @app.post("/v1/accounts/<int:account_id>/reset")
    def reset_account(account_id: int) -> Any:
        payload = request.get_json(silent=True) or {}
        model_req = payload.get("model") if isinstance(payload, dict) else None
        try:
            return jsonify(modeltrace.reset_account_model(account_id, model=model_req)), 200
        except UnknownAccount:
            return jsonify({"error": "not_found"}), 404
        except ModelNotSupported:
            return jsonify({"error": "model_not_supported"}), 400

    @app.post("/accounts/<int:account_id>/run")
    @app.post("/v1/accounts/<int:account_id>/run")
    def run_account(account_id: int) -> Any:
        payload = request.get_json(silent=True) or {}
        model_req = payload.get("model") if isinstance(payload, dict) else None
        try:
            result = modeltrace.enqueue_manual_account(account_id, model=model_req)
            return jsonify({"queued": result.queued, "model": result.model}), 202
        except UnknownAccount:
            return jsonify({"error": "not_found"}), 404
        except ModelNotSupported:
            return jsonify({"error": "model_not_supported"}), 400
        except DuplicateQueue:
            return jsonify({"error": "already_queued", "queued": False, "message_code": "duplicate"}), 409

    if start_worker:
        modeltrace.start()

    return app


if __name__ == "__main__":
    try:
        application = create_app()
    except ConfigError as exc:
        raise SystemExit(f"configuration error: {exc}") from exc
    application.run(host="0.0.0.0", port=8081, threaded=True)
