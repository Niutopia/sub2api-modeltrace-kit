from __future__ import annotations

import hashlib

import json
import logging
import math
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import httpx

from . import __version__
from .config import ConfigError, MonitorConfig, ServiceConfig
from .db import ModelTraceDB, QueueItem
from .fingerprint import analyze_outputs, generate_challenges, load_bank, parse_numbers
from .transport import ProbeResult, ProbeTransport

logger = logging.getLogger(__name__)


PROTOCOL = "responses"
REASONING_EFFORT = "none"
SERVICE_TIER = "default"
HISTORY_LIMIT = 16
MANUAL_MIN_INTERVAL_SECONDS = 60
STALE_AFTER_SECONDS = 3600
ACCOUNT_SYNC_TIMEOUT_SECONDS = 4.0
HOST_ACCOUNT_FETCH_TIMEOUT_SECONDS = 3.0

SAFE_MESSAGE_CODES = frozenset({
    "insufficient_output", "insufficient_sample", "response_truncated",
    "invalid_event_stream", "response_too_large", "upstream_timeout",
    "upstream_unsupported_reasoning_effort", "calibration_unverified_reasoning_effort",
    "upstream_response_failed", "classifier_sample_mismatch", "classifier_error",
    "mixed_upstream_account", "mixed_route", "upstream_error",
    "incomplete_response", "upstream_model_mismatch", "transport_error",
    "upstream_auth", "upstream_rate_limited", "upstream_5xx", "upstream_http_error",
    "redirect_blocked", "upstream_network_error", "invalid_json", "missing_output_text",
    "internal_error", "challenge_generation_failed", "target_not_in_bank",
    "target_top1_high_confidence", "repeated_other_model", "low_or_competing_probability",
    "unsupported_model", "not_tested", "upstream_no_available_account", "output_truncated", "difference_signal", "compatible", "account_unavailable"
})



class UnknownMonitor(Exception):
    pass


class UnknownAccount(Exception):
    pass


class ModelNotSupported(Exception):
    pass


class NotParticipating(Exception):
    """The account exists on the host but is outside its scheduling pool."""


class DuplicateQueue(Exception):
    pass


class TooSoon(Exception):
    def __init__(self, retry_after: int):
        self.retry_after = max(1, retry_after)
        super().__init__("manual_trigger_too_soon")


class ExpectedModelMismatch(Exception):
    """The caller's snapshot is not the detector's current model."""


@dataclass(frozen=True)
class EnqueueResult:
    queued: bool
    queue_id: int | None
    model: str | None = None


def utc_iso(timestamp: float | None, *, timespec: str = "seconds") -> str | None:
    if timestamp is None:
        return None
    return (
        datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
        .isoformat(timespec=timespec)
        .replace("+00:00", "Z")
    )


def _clamp_probability(value: Any) -> float | None:
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if value != value or value in (float("inf"), float("-inf")):
        return None
    return max(0.0, min(1.0, value))



# "Clearly another model": the top candidate must reach this probability (and the
# target stay <= 0.15, with a 0.65 gap). Lowered from 0.8 on 2026-09-25 after an
# account kept answering like gpt-5.5 at 70-76% with the target near 0%.
DIFFERENCE_MIN_PROBABILITY = 0.7


def classify_sample(result: dict[str, Any], expected: str, previous: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """
    Port of state.mjs:
    export function classifySample(result, expected, previous = []) {
      const candidate = result.results.find((item) => item.model === expected);
      const top = result.results[0];
      let outcome = 'inconclusive';
      if (!expected) outcome = 'missing_expected_model';
      else if (!candidate) outcome = 'unknown_expected_model';
      else if (top.model === expected && top.probability >= 0.5) outcome = 'compatible';
      else if (top.model !== expected && top.probability >= 0.7 && candidate.probability <= 0.15 && top.probability - candidate.probability >= 0.65) outcome = 'difference_signal';
      const recent = [...previous.slice(-2), { prediction: top.model, outcome }];
      if (outcome === 'difference_signal' && recent.filter((sample) => sample.prediction === top.model && ['difference_signal', 'repeated_difference'].includes(sample.outcome)).length >= 2) outcome = 'repeated_difference';
      return { outcome, prediction: top.model, closedSetWeight: top.probability, expectedWeight: candidate?.probability ?? null };
    }
    """
    previous = previous or []
    results = result.get("results", [])
    candidate = next((item for item in results if item.get("model") == expected), None)
    top = results[0] if results else None
    outcome = "inconclusive"
    top_model = top.get("model") if top else None
    top_prob = top.get("probability", 0.0) if top else 0.0
    cand_prob = candidate.get("probability") if candidate else None

    if not expected:
        outcome = "missing_expected_model"
    elif candidate is None:
        outcome = "unknown_expected_model"
    elif top_model == expected and top_prob >= 0.5:
        outcome = "compatible"
    elif top_model != expected and top_prob >= DIFFERENCE_MIN_PROBABILITY and cand_prob is not None and cand_prob <= 0.15 and (top_prob - cand_prob) >= 0.65:
        outcome = "difference_signal"

    recent = [*previous[-2:], {"prediction": top_model, "outcome": outcome}]
    if outcome == "difference_signal":
        matching = [s for s in recent if s.get("prediction") == top_model and s.get("outcome") in {"difference_signal", "repeated_difference"}]
        if len(matching) >= 2:
            outcome = "repeated_difference"

    return {
        "outcome": outcome,
        "prediction": top_model,
        "closedSetWeight": top_prob,
        "expectedWeight": cand_prob,
    }


class HostClient:
    """HTTP client for communicating with sub2api internal host API (A1)."""

    def __init__(self, host_api_base: str, secret: str | None = None, client: httpx.Client | None = None):
        self.base_url = host_api_base.rstrip("/")
        self.secret = secret
        self.client = client
        self._last_accounts: list[dict[str, Any]] = []

    def fetch_accounts(self, models: list[str]) -> tuple[list[dict[str, Any]], bool]:
        models_param = quote(",".join(models), safe=",")
        # include_inactive keeps accounts that are switched off (or cooling
        # down) visible with their last result; they are never auto-probed.
        url = f"{self.base_url}/api/v1/internal/modeltrace/accounts?models={models_param}&include_inactive=1"
        headers = {}
        if self.secret:
            headers["Authorization"] = f"Bearer {self.secret}"
        try:
            if self.client is not None:
                resp = self.client.get(url, headers=headers, timeout=HOST_ACCOUNT_FETCH_TIMEOUT_SECONDS)
            else:
                with httpx.Client(timeout=HOST_ACCOUNT_FETCH_TIMEOUT_SECONDS) as cl:
                    resp = cl.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and "accounts" in data and isinstance(data["accounts"], list):
                    self._last_accounts = data["accounts"]
                    return self._last_accounts, True
            logger.warning("host account fetch returned HTTP %s", resp.status_code)
        except Exception as exc:
            logger.warning("host account fetch failed: %s", type(exc).__name__)
        return self._last_accounts, False

    def pause_model(self, account_id: int, model: str, minutes: int, evidence: str) -> bool:
        url = f"{self.base_url}/api/v1/internal/modeltrace/accounts/{account_id}/model-pause"
        headers = {"Content-Type": "application/json"}
        if self.secret:
            headers["Authorization"] = f"Bearer {self.secret}"
        payload = {"model": model, "minutes": minutes, "evidence": evidence}
        try:
            if self.client is not None:
                resp = self.client.post(url, json=payload, headers=headers, timeout=10.0)
            else:
                with httpx.Client(timeout=10.0) as cl:
                    resp = cl.post(url, json=payload, headers=headers)
            if resp.status_code == 200:
                return True
            logger.warning("host pause_model for account %s returned HTTP %s", account_id, resp.status_code)
        except Exception as exc:
            logger.warning("host pause_model for account %s failed: %s", account_id, type(exc).__name__)
        return False

    def resume_model(self, account_id: int, model: str) -> bool:
        url = f"{self.base_url}/api/v1/internal/modeltrace/accounts/{account_id}/model-resume"
        headers = {"Content-Type": "application/json"}
        if self.secret:
            headers["Authorization"] = f"Bearer {self.secret}"
        payload = {"model": model}
        try:
            if self.client is not None:
                resp = self.client.post(url, json=payload, headers=headers, timeout=10.0)
            else:
                with httpx.Client(timeout=10.0) as cl:
                    resp = cl.post(url, json=payload, headers=headers)
            if resp.status_code == 200:
                return True
            logger.warning("host resume_model for account %s returned HTTP %s", account_id, resp.status_code)
        except Exception as exc:
            logger.warning("host resume_model for account %s failed: %s", account_id, type(exc).__name__)
        return False


class ModelTraceService:
    def __init__(
        self,
        config: ServiceConfig,
        *,
        db: ModelTraceDB | None = None,
        database_path: str | Path | None = None,
        transport: ProbeTransport | Any | None = None,
        receipt_reader: Any | None = None,
        bank_path: str | Path | None = None,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.config = config
        self.clock = clock
        self.monotonic = monotonic
        self.sleeper = sleeper
        self.db = db or ModelTraceDB(database_path or os.environ.get("MODELTRACE_DB_PATH", "/data/modeltrace.sqlite3"))
        self.bank_path = Path(bank_path or Path(__file__).with_name("data") / "unified_bank.json")
        self.bank = load_bank(self.bank_path)
        self.bank_models = {str(model["id"]): model for model in self.bank.get("models", [])}
        if any(alias in self.bank_models or canonical not in self.bank_models
               for alias, canonical in config.model_aliases.items()):
            raise ConfigError("model_aliases must point directly to a known bank model and not shadow one")
        self.transport = transport or ProbeTransport(
            endpoint=config.endpoint,
            api_key=config.api_key,
            max_output_tokens=config.max_output_tokens,
            timeout_seconds=config.timeout_seconds,
            max_response_bytes=config.max_response_bytes,
            max_prompt_bytes=config.max_prompt_bytes,
            probe_secret=os.environ.get("MODELTRACE_SECRET"),
            reasoning_effort_overrides=config.reasoning_effort_overrides,
            model_aliases=config.model_aliases,
            idle_timeout_seconds=getattr(config, "idle_timeout_seconds", None),
            total_timeout_seconds=getattr(config, "total_timeout_seconds", None),
        )
        self._retest_states: dict[int, dict[str, Any]] = {}
        self._last_account_refresh_at: float | None = None
        self._account_refresh_lock = threading.Lock()
        self._account_fetch_thread: threading.Thread | None = None
        host_base = config.host_api_base or os.environ.get("HOST_API_BASE")
        self.host_client = HostClient(host_base, secret=os.environ.get("MODELTRACE_SECRET")) if host_base else None
        self._stop = threading.Event()
        self._worker_lock = threading.Lock()
        self._worker_thread: threading.Thread | None = None
        self._worker_started_at: float | None = None
        # A completion timestamp provides bounded evidence that an overdue slot
        # was delayed by this live worker. After a long idle or restart, old
        # slots are advanced instead of replayed. The partial queue index still
        # enforces one queued/running job per monitor.
        self._last_round_completed_at: float | None = None

        now = self.clock()
        try:
            self.db.acquire_worker_ownership()
        except Exception:
            if db is None:
                self.db.close()
            raise
        self.db.seed_monitors(config.monitors, now=now)
        self._schedule_anchor = self.db.schedule_anchor(now=now)
        self.db.recover_running_jobs(
            now=now, interval_seconds=config.interval_seconds,
            next_runs={monitor_id: self._next_scheduled_run(monitor_id, now + config.interval_seconds)
                       for monitor_id in config.monitors},
        )
        interrupted_account_jobs = self.db.recover_running_account_jobs(now=now)
        for rec_acct_id, rec_model in interrupted_account_jobs:
            self.db.enqueue_account(
                rec_acct_id,
                rec_model,
                now=now,
                available_at=now + 60,
                trigger="scheduled",
                retest_index=0,
            )
        self.db.cleanup_old(now=now)
        if self.config.per_account_enabled and self.host_client:
            self._refresh_accounts(now)
        # Keep a startup snapshot for health/diagnostics.  Orphans are only
        # reported; no startup path is allowed to release them.
        self._startup_orphan_summary = self.db.orphan_summary()
        self._initialize_next_runs(now)

    def _scheduled_monitor_ids(self) -> list[int]:
        return sorted(
            monitor.monitor_id for monitor in self.config.monitors.values()
            if monitor.enabled and self.is_supported(monitor)
        )

    def _next_scheduled_run(self, monitor_id: int, not_before: float) -> float | None:
        monitor_ids = self._scheduled_monitor_ids()
        if monitor_id not in monitor_ids:
            return None
        # One durable global phase, including across restarts.
        # Four distinct models at 1800s still occupy 0/450/900/1350s, not four
        # independent model groups or finish-time-dependent drifting schedules.
        window = self.config.interval_seconds
        offset = monitor_ids.index(monitor_id) * window // len(monitor_ids)
        first = self._schedule_anchor + offset
        cycles = max(0, math.ceil((not_before - first) / self.config.interval_seconds))
        return first + cycles * self.config.interval_seconds

    def _initialize_next_runs(self, now: float) -> None:
        for monitor_id in self._scheduled_monitor_ids():
            next_run = self._next_scheduled_run(monitor_id, now)
            assert next_run is not None
            # A not-yet-run monitor already owns its persisted slot. Restarting
            # must neither move it forward indefinitely nor bring it earlier.
            self.db.set_initial_next_run(monitor_id, next_run, now=now)

    def is_supported(self, monitor: MonitorConfig) -> bool:
        return monitor.configured_supported and self.config.calibration_model_for(monitor.model) in self.bank_models

    def support_message_code(self, monitor: MonitorConfig) -> str:
        return "ok" if self.is_supported(monitor) else "unsupported_model"

    def start(self) -> None:
        with self._worker_lock:
            if self._worker_thread and self._worker_thread.is_alive():
                return
            self._stop.clear()
            self._worker_started_at = self.clock()
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="modeltrace-worker",
                daemon=True,
            )
            self._worker_thread.start()

    def stop(self, *, join_timeout: float = 2.0) -> None:
        with self._worker_lock:
            self._stop.set()
            thread = self._worker_thread
        if thread and thread.is_alive():
            thread.join(timeout=join_timeout)
        with self._worker_lock:
            # An in-flight paid request may outlast the join timeout. Keep its
            # handle so start() cannot clear its stop signal and launch a peer.
            if self._worker_thread is thread and (thread is None or not thread.is_alive()):
                self._worker_thread = None

    def worker_running(self) -> bool:
        return bool(self._worker_thread and self._worker_thread.is_alive())

    def health(self) -> dict[str, Any]:
        return {
            "ok": True,
            "service": "modeltrace-service",
            "version": __version__,
            "worker_running": self.worker_running(),
            "protocol": PROTOCOL,
            "orphan_reservations": self.db.orphan_summary(),
            "startup_orphan_reservations": self._startup_orphan_summary,
        }

    def enqueue_manual(self, monitor_id: int, *, expected_model: str) -> EnqueueResult:
        monitor = self.config.monitors.get(monitor_id)
        if monitor is None:
            raise UnknownMonitor(monitor_id)
        if not isinstance(expected_model, str) or not expected_model or expected_model != monitor.model:
            raise ExpectedModelMismatch(monitor_id)
        now = self.clock()
        if self.db.has_pending(monitor_id):
            raise DuplicateQueue(monitor_id)
        last_request = self.db.last_request_at(monitor_id)
        if last_request is not None:
            remaining = MANUAL_MIN_INTERVAL_SECONDS - (now - last_request)
            if remaining > 0:
                raise TooSoon(int(remaining + 0.999))
        queued, queue_id = self.db.enqueue(monitor_id, now=now, trigger="manual")
        if not queued:
            raise DuplicateQueue(monitor_id)
        if self.config.per_account_enabled:
            self._enqueue_model_on_all_accounts(monitor.model, now=now)
        # A manual run owns the queue slot immediately. The regular scheduler
        # cannot add a second run while this one is queued/running.
        return EnqueueResult(True, queue_id)

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            now = self.clock()
            if self.config.per_account_enabled:
                self._refresh_accounts(now, if_due=True)
                self._schedule_due_accounts(now)
                # Monitors are no longer scheduled here, but a manual run from the
                # channel page still lands in the monitor queue and must be served.
                job = self.db.claim_next(now=now, allow_scheduled=self.config.enabled)
                if job is not None:
                    self._run_job_safely(job)
                    continue
                acct_job = self.db.claim_next_account(now=now, allow_scheduled=self.config.enabled)
                if acct_job is not None:
                    self._run_account_job_safely(acct_job)
                    continue
            else:
                self._schedule_due(now)
                job = self.db.claim_next(now=now, allow_scheduled=self.config.enabled)
                if job is not None:
                    self._run_job_safely(job)
                    continue
            self._stop.wait(0.25)

    def _schedule_due(self, now: float) -> None:
        monitor_ids = self._scheduled_monitor_ids()
        if not monitor_ids:
            return
        slot_width = self.config.interval_seconds / len(monitor_ids)
        for monitor_id in monitor_ids:
            state = self.db.get_state(monitor_id)
            due_at = state["next_run_at"]
            if due_at is None or self.db.has_pending(monitor_id):
                continue
            completion_age = (
                None
                if self._last_round_completed_at is None
                else now - self._last_round_completed_at
            )
            live_worker_continuity = (
                completion_age is not None
                and 0 <= completion_age <= self.config.interval_seconds
            )
            if (
                not live_worker_continuity
                and now - float(due_at) >= slot_width
            ):
                # Without a recent completion from this process, an overdue
                # slot may be historical state left by a restart or long idle.
                # Advance it without replaying missed periods. A recent round
                # completion is bounded evidence that the worker was alive and
                # occupied; leave those overdue slots due so one current job is
                # queued below and a slow model cannot be skipped forever.
                self.db.set_next_run(
                    monitor_id,
                    self._next_scheduled_run(monitor_id, now),
                    now=now,
                )
        self.db.enqueue_due_monitors(monitor_ids, now=now)

    def _run_job_safely(self, job: QueueItem) -> None:
        try:
            self._run_job(job)
        except Exception:
            # Never put exception text into the database: it could contain a
            # provider URL, request fragment, or another secret-bearing detail.
            self._record_internal_error(job)
        finally:
            self._last_round_completed_at = self.clock()

    def _record_internal_error(self, job: QueueItem) -> None:
        now = self.clock()
        next_run = self._next_scheduled_run(job.monitor_id, now + 0.000001)
        self.db.record_round_and_finish(
            job,
            status="error",
            target_probability=None,
            best_model=None,
            checked_at=now,
            message_code="internal_error",
            ranking=[],
            diagnostics={"request_count": 0, "valid_count": 0},
            next_run_at=next_run,
            next_run_after=None,
            upstream_statuses=[],
            receipt_count=0,
            receipt_consistent=None,
            actual_cost_usd=None,
            reserved_usd=None,
        )

    def _run_job(self, job: QueueItem) -> None:
        """One single-probe round with retest state machine."""
        monitor = self.config.monitors.get(job.monitor_id)
        if monitor is None:
            self._record_internal_error(job)
            return
        if not self.is_supported(monitor):
            self.db.record_round_and_finish(
                job, status="unsupported", target_probability=None, best_model=None,
                checked_at=self.clock(), message_code="unsupported_model", ranking=[],
                diagnostics={"request_count": 0, "valid_count": 0}, next_run_at=None,
                next_run_after=None, upstream_statuses=[], receipt_count=0,
                receipt_consistent=None, actual_cost_usd=None, reserved_usd=None,
            )
            return

        challenges = generate_challenges(count=1)
        if len(challenges) != 1:
            raise ValueError("challenge_generation_failed")
        challenge = challenges[0]
        if len(str(challenge.get("prompt", "")).encode("utf-8")) > self.config.max_prompt_bytes:
            raise ValueError("prompt_too_large")

        user_agent = f"ModelTraceProbe/{uuid.uuid4()}"
        session_affinity = str(uuid.uuid4())
        reasoning_effort = self.config.reasoning_effort_for(monitor.model)

        if not self.db.claim_probe_attempt(job, 0, user_agent, now=self.clock()):
            return

        send_start = self.monotonic()
        try:
            result = self.transport.run(
                model=monitor.model,
                challenge=challenge,
                user_agent=user_agent,
                session_affinity=session_affinity,
            )
        except Exception:
            result = ProbeResult(None, None, "transport_error")
        total_time_ms = max(0, round((self.monotonic() - send_start) * 1000))

        detail = result.transport_detail or {}
        first_event_ms = detail.get("first_event_ms")
        first_text_ms = detail.get("first_text_ms")
        received_bytes = detail.get("received_bytes", 0)
        output_tokens = detail.get("output_tokens")
        reasoning_tokens = detail.get("reasoning_tokens")

        expected = int(challenge.get("expected_count") or 0)
        structured_mismatch = result.upstream_model is not None and result.upstream_model != monitor.model
        parsed_numbers_count = 0
        is_complete = False

        if result.text is not None and result.error_code is None and not structured_mismatch:
            numbers = parse_numbers(result.text)
            parsed_numbers_count = len(numbers)
            is_complete = parsed_numbers_count >= max(80, math.ceil(expected * 0.55))

        diagnostics_single = {
            "first_event_ms": first_event_ms,
            "first_text_ms": first_text_ms,
            "total_time_ms": total_time_ms,
            "reasoning_tokens": reasoning_tokens,
            "output_tokens": output_tokens,
            "parsed_numbers_count": parsed_numbers_count,
            "is_complete": is_complete,
            "received_bytes": received_bytes,
        }

        # Handle upstream errors (timeout, 5xx, network, no_available_account, etc.)
        upstream_status = result.status_code
        transport_error = result.error_code
        if structured_mismatch:
            transport_error = "upstream_model_mismatch"

        # Check if error or incomplete
        is_upstream_error = transport_error is not None or structured_mismatch

        finish = self.clock()

        if is_upstream_error:
            # End round, do NOT trigger retest, reset retest state
            self._retest_states.pop(monitor.monitor_id, None)
            msg = transport_error or "probe_failed"
            self.db.record_round_and_finish(
                job,
                status="error",
                target_probability=None,
                best_model=None,
                checked_at=finish,
                message_code=msg,
                ranking=[],
                diagnostics={
                    "trigger": job.trigger,
                    "request_count": 1,
                    "valid_count": 0,
                    "parsed_counts": [parsed_numbers_count],
                    "expected_counts": [expected],
                    "upstream_statuses": [upstream_status],
                    "transport_error_codes": [msg],
                    "identity_mismatch": structured_mismatch,
                    "transport_details": [detail],
                    "reasoning_effort": reasoning_effort,
                    "cost_controls_enabled": False,
                    **diagnostics_single,
                },
                next_run_at=self._next_scheduled_run(monitor.monitor_id, finish + 0.000001),
                next_run_after=None,
                upstream_statuses=[upstream_status],
                receipt_count=0,
                receipt_consistent=None,
                actual_cost_usd=None,
                reserved_usd=None,
            )
            return

        if not is_complete:
            # Output truncated / insufficient output
            self._retest_states.pop(monitor.monitor_id, None)
            msg = "output_truncated" if (
                detail.get("termination_event_received") is False
                or detail.get("status") in ("incomplete", "in_progress", "failed")
                or detail.get("incomplete_reason") == "max_output_tokens"
            ) else "insufficient_sample"
            self.db.record_round_and_finish(
                job,
                status="error",
                target_probability=None,
                best_model=None,
                checked_at=finish,
                message_code=msg,
                ranking=[],
                diagnostics={
                    "trigger": job.trigger,
                    "request_count": 1,
                    "valid_count": 0,
                    "parsed_counts": [parsed_numbers_count],
                    "expected_counts": [expected],
                    "upstream_statuses": [upstream_status],
                    "transport_error_codes": [msg],
                    "identity_mismatch": False,
                    "transport_details": [detail],
                    "reasoning_effort": reasoning_effort,
                    "cost_controls_enabled": False,
                    **diagnostics_single,
                },
                next_run_at=self._next_scheduled_run(monitor.monitor_id, finish + 0.000001),
                next_run_after=None,
                upstream_statuses=[upstream_status],
                receipt_count=0,
                receipt_consistent=None,
                actual_cost_usd=None,
                reserved_usd=None,
            )
            return

        # Output is valid! Analyze with 1 output
        analysis = analyze_outputs([{"text": result.text, "expected_count": expected}], self.bank)
        best_model = str(analysis["prediction"])
        results = analysis.get("results", [])
        normalized_results = [
            {
                "model": str(item["model"]),
                "probability": _clamp_probability(item.get("probability")) or 0.0,
            }
            for item in results
            if isinstance(item, dict) and item.get("model") is not None
        ]
        ranking = [
            {"model": item["model"], "probability": round(item["probability"], 6)}
            for item in normalized_results[:3]
        ]
        target_prob_item = next(
            (item["probability"] for item in normalized_results if item["model"] == self.config.calibration_model_for(monitor.model)),
            None,
        )
        target_probability = _clamp_probability(target_prob_item)

        previous_samples = self._retest_states.get(monitor.monitor_id, {}).get("previous_samples", [])
        sample_class = classify_sample(analysis, self.config.calibration_model_for(monitor.model), previous=previous_samples)
        outcome = sample_class["outcome"]

        # Check if currently in retest state
        retest_state = self._retest_states.get(monitor.monitor_id)
        if retest_state is None:
            # This is the initial probe
            if outcome == "compatible":
                status = "match"
                message_code = "compatible"
                self._retest_states.pop(monitor.monitor_id, None)
                next_run = self._next_scheduled_run(monitor.monitor_id, finish + 0.000001)
            else:
                # Need retest! Start retest batch (target = 3 retest rounds)
                retest_state = {
                    "total": 3,
                    "done": 0,
                    "previous_samples": [{"prediction": sample_class["prediction"], "outcome": outcome}],
                    "results": [],
                }
                self._retest_states[monitor.monitor_id] = retest_state
                status = "uncertain"
                message_code = outcome
                # Enqueue immediately for next worker turn!
                next_run = finish
                self.db.enqueue(monitor.monitor_id, now=finish, available_at=finish, trigger="auto_retest", retest_index=1)
        else:
            # This is a retest probe!
            retest_state["done"] += 1
            retest_state["results"].append(outcome)
            retest_state["previous_samples"].append({"prediction": sample_class["prediction"], "outcome": outcome})

            if outcome == "compatible" or any(r == "compatible" for r in retest_state["results"]):
                status = "match"
                message_code = "compatible"
                self._retest_states.pop(monitor.monitor_id, None)
                next_run = self._next_scheduled_run(monitor.monitor_id, finish + 0.000001)
            elif retest_state["done"] >= retest_state["total"]:
                # Finished all 3 retests
                all_diff = all(r in {"difference_signal", "repeated_difference"} for r in retest_state["results"])
                if all_diff:
                    status = "suspect"
                    message_code = "repeated_other_model"
                else:
                    status = "uncertain"
                    message_code = "low_or_competing_probability"
                self._retest_states.pop(monitor.monitor_id, None)
                next_run = self._next_scheduled_run(monitor.monitor_id, finish + 0.000001)
            else:
                # Continue retest
                status = "uncertain"
                message_code = outcome
                next_run = finish
                self.db.enqueue(monitor.monitor_id, now=finish, available_at=finish, trigger="auto_retest", retest_index=retest_state["done"])

        retest_prog = None
        if monitor.monitor_id in self._retest_states:
            st = self._retest_states[monitor.monitor_id]
            retest_prog = {"done": st["done"], "total": st["total"]}

        self.db.record_round_and_finish(
            job,
            status=status,
            target_probability=target_probability,
            best_model=best_model,
            checked_at=finish,
            message_code=message_code,
            ranking=ranking,
            diagnostics={
                "trigger": job.trigger,
                "request_count": 1,
                "valid_count": 1,
                "parsed_counts": [parsed_numbers_count],
                "expected_counts": [expected],
                "upstream_statuses": [upstream_status],
                "transport_error_codes": [],
                "identity_mismatch": False,
                "transport_details": [detail],
                "reasoning_effort": reasoning_effort,
                "cost_controls_enabled": False,
                "used_outputs": 1,
                "calibration_queries": analysis.get("calibration", {}).get("queries"),
                "retest_progress": retest_prog,
                **diagnostics_single,
            },
            next_run_at=next_run,
            next_run_after=None,
            upstream_statuses=[upstream_status],
            receipt_count=0,
            receipt_consistent=None,
            actual_cost_usd=None,
            reserved_usd=None,
        )
    def _other_model_streak(self, monitor_id: int, winner: str) -> int:
        rows = self.db.get_rounds(monitor_id, limit=HISTORY_LIMIT)
        streak = 0
        for row in reversed(rows):
            if row["status"] not in {"match", "uncertain", "suspect"}:
                break
            if row["best_model"] != winner:
                break
            streak += 1
        return streak + 1

    def _diagnostic_summary(self, row: Any) -> dict[str, Any] | None:
        """Expose bounded, non-content failure evidence to the admin UI.

        The persisted diagnostics also contain timing counters.  The panel only
        needs the request cardinality, HTTP statuses, valid-output count and
        stable transport codes to explain why a round was not scored.  Never
        copy upstream bodies, prompts, account ids or request identifiers into
        this response.
        """
        diagnostics = self.db.decode_diagnostics(row)
        if not isinstance(diagnostics, dict):
            return None
        raw_statuses = diagnostics.get("upstream_statuses")
        statuses = [
            int(value) for value in raw_statuses
            if isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599
        ] if isinstance(raw_statuses, list) else []

        raw_errors = diagnostics.get("transport_error_codes")
        errors = [
            str(value) for value in raw_errors
            if isinstance(value, str) and value in SAFE_MESSAGE_CODES
        ] if isinstance(raw_errors, list) else []

        raw_details = diagnostics.get("transport_details")
        incomplete = sum(
            1 for detail in raw_details
            if isinstance(detail, dict) and detail.get("termination_event_received") is False
        ) if isinstance(raw_details, list) else 0

        request_count = diagnostics.get("request_count")
        valid_count = diagnostics.get("valid_count")
        req_int = int(request_count) if isinstance(request_count, int) and not isinstance(request_count, bool) and request_count >= 0 else 0
        val_int = int(valid_count) if isinstance(valid_count, int) and not isinstance(valid_count, bool) and valid_count >= 0 else 0
        return {
            "request_count": req_int,
            "valid_count": val_int,
            "upstream_statuses": statuses[:3],
            "transport_error_codes": errors[:3],
            "incomplete_responses": incomplete,
        }

    def _round_shape(self, row: Any) -> dict[str, Any]:
        shape = {
            "id": int(row["id"]),
            "status": str(row["status"]),
            "target_probability": _clamp_probability(row["target_probability"]),
            "best_model": str(row["best_model"]) if row["best_model"] is not None else None,
            "checked_at": utc_iso(float(row["checked_at"])),
            "message_code": str(row["message_code"]),
            "ranking": [
                {
                    "model": str(item.get("model")),
                    "probability": round(_clamp_probability(item.get("probability")) or 0.0, 6),
                }
                for item in self.db.decode_ranking(row)[:3]
                if isinstance(item, dict) and item.get("model") is not None
            ],
        }
        summary = self._diagnostic_summary(row)
        if summary is not None and shape["status"] == "error":
            shape["diagnostic_summary"] = summary
        return shape

    def _synthetic_round(self, monitor: MonitorConfig, *, now: float) -> dict[str, Any]:
        supported = self.is_supported(monitor)
        return {
            "id": 0,
            "status": "not_tested" if supported else "unsupported",
            "target_probability": None,
            "best_model": None,
            "checked_at": utc_iso(now),
            "message_code": "not_tested" if supported else self.support_message_code(monitor),
            "ranking": [],
        }

    @staticmethod
    def _pending_job_state(pending: sqlite3.Row | None) -> dict[str, Any]:
        # Admission, eligibility and execution are different timestamps. None
        # means no pending job, not an inferred successful probe.
        return {
            "running": pending is not None and pending["state"] == "running",
            "queued": pending is not None and pending["state"] == "queued",
            "queued_at": utc_iso(pending["requested_at"]) if pending is not None else None,
            "available_at": utc_iso(pending["available_at"]) if pending is not None else None,
            "started_at": utc_iso(pending["started_at"]) if pending is not None and pending["state"] == "running" else None,
        }

    def snapshot(self, monitor_id: int, *, admin: bool = False) -> dict[str, Any]:
        self._refresh_accounts(self.clock(), if_due=True)
        with self.db.read_snapshot():
            return self._snapshot(monitor_id, admin=admin)

    def _snapshot(self, monitor_id: int, *, admin: bool = False) -> dict[str, Any]:
        monitor = self.config.monitors.get(monitor_id)
        if monitor is None:
            raise UnknownMonitor(monitor_id)
        now = self.clock()
        state = self.db.get_state(monitor_id)
        latest_row = self.db.get_latest_round(monitor_id)
        history_rows = self.db.get_rounds(monitor_id, limit=HISTORY_LIMIT)
        latest = self._round_shape(latest_row) if latest_row is not None else self._synthetic_round(monitor, now=now)
        history = [self._round_shape(row) for row in history_rows]
        account_view = (
            self._per_account_monitor_view(monitor, history_rows, now=now)
            if self.config.per_account_enabled else None
        )
        if account_view is not None:
            # In per-account mode the channel shows the same rounds as the
            # account page; channel-mode rounds from before the switch are old.
            latest = account_view["latest"] or self._synthetic_round(monitor, now=now)
            history = account_view["history"]
        pending = self.db.get_pending_job(monitor_id)
        # Per-account mode does not create recurring channel jobs. Persisted
        # pending work is projected independently of that automatic schedule.
        scheduled = (not self.config.per_account_enabled and self.config.enabled
                     and monitor.enabled and self.is_supported(monitor))
        state_next = float(state["next_run_at"]) if scheduled and state["next_run_at"] is not None else None
        if pending is not None:
            # Pending work owns the visible slot. A paused scheduled job is
            # still queued, but has no executable countdown; manual work does.
            can_claim = self.config.enabled or pending["trigger"] == "manual"
            state_next = float(pending["available_at"]) if pending["state"] == "queued" and can_claim else None
        last_checked = float(state["last_checked_at"]) if state["last_checked_at"] is not None else None
        stale_after = max(STALE_AFTER_SECONDS, self.config.interval_seconds * 2)
        schedule_mode = "scheduled" if scheduled else "manual"
        pending_state = self._pending_job_state(pending)
        if account_view is not None:
            last_checked = account_view["last_checked_at"]
            stale_after = account_view["stale_after"]
            if pending is None:
                state_next = account_view["next_run_at"]
                if account_view["running"] is not None:
                    pending_state = self._pending_job_state(account_view["running"])
            if account_view["scheduled"] and self.config.enabled and monitor.enabled:
                schedule_mode = "scheduled"
        public = {
            "snapshot_at": utc_iso(now, timespec="microseconds"),
            "monitor_id": monitor.monitor_id,
            "model": monitor.model,
            "scope_label": self.config.scope_label,
            "protocol": PROTOCOL,
            "reasoning_effort": self.config.reasoning_effort_for(monitor.model),
            "calibration_reference_effort": "none",
            "calibration_compatibility": "unverified",
            "calibration_model": self.config.calibration_model_for(monitor.model),
            "service_tier": SERVICE_TIER,
            "enabled": bool(monitor.enabled),
            "supported": self.is_supported(monitor),
            **pending_state,
            "schedule_mode": schedule_mode,
            "interval_seconds": self.config.interval_seconds,
            "last_checked_at": utc_iso(last_checked),
            "next_run_at": utc_iso(state_next),
            "stale": bool(last_checked is not None and now - last_checked > stale_after),
            "latest": latest,
            # History is deliberately oldest -> newest for charting.
            "history": history,
            "retest_progress": (
                {"done": self._retest_states[monitor_id]["done"], "total": self._retest_states[monitor_id]["total"]}
                if monitor_id in self._retest_states else None
            ),
            "paused_budget": False,
            "paused_reconciliation": False,
            "next_run_after": None,
            "accounts_summary": self._accounts_summary_for_model(monitor.model),
        }
        if admin:
            try:
                upstream_statuses = json.loads(state["last_upstream_statuses_json"])
            except (TypeError, json.JSONDecodeError):
                upstream_statuses = []
            public["diagnostics"] = {
                "cost_controls_enabled": False,
                "queue_pending": pending is not None,
                "last_error_code": state["last_error_code"],
                "last_upstream_statuses": upstream_statuses,
                "auto_retests": 0, "retention_days": 90,
                "history_order": "oldest_to_newest",
                "message_codes_are_safe_identifiers": True,
                "latest_round": self.db.decode_diagnostics(latest_row) if latest_row is not None else {},
            }
        return public


    def _per_account_monitor_view(
        self, monitor: MonitorConfig, monitor_rows: list[sqlite3.Row], *, now: float,
    ) -> dict[str, Any]:
        """Channel view of ``monitor.model``: per-account rounds of accounts in
        the host pool (same data as the account page) merged with channel
        rounds, newest first, so a recent account check supersedes an old
        channel-mode result instead of sitting beside it."""
        model = monitor.model
        targets = [
            t for t in self._get_all_targets()
            if str(t["mode"]) != "retired" and t["participating"] and model in t["models"]
        ]
        rows: list[sqlite3.Row] = []
        for target in targets:
            for mid in self._pool_member_ids(target):
                rows.extend(self.db.get_account_rounds(mid, model=model, limit=HISTORY_LIMIT))
        # A manual reset is a neutral marker, not a check result.
        rows = [r for r in rows if str(r["status"]) != "reset"]
        rows.extend(monitor_rows)
        rows.sort(key=lambda r: (float(r["checked_at"]), int(r["id"])), reverse=True)
        rows = rows[:HISTORY_LIMIT]

        def shape(row: sqlite3.Row) -> dict[str, Any]:
            if "monitor_id" in row.keys():
                return self._round_shape(row)
            return {
                "id": int(row["id"]),
                "status": str(row["status"]),
                "target_probability": _clamp_probability(row["target_probability"]),
                "best_model": str(row["best_model"]) if row["best_model"] is not None else None,
                "checked_at": utc_iso(float(row["checked_at"])),
                "message_code": str(row["message_code"]),
                "ranking": [
                    {
                        "model": str(item.get("model")),
                        "probability": round(_clamp_probability(item.get("probability")) or 0.0, 6),
                    }
                    for item in self.db.decode_ranking(row)[:3]
                    if isinstance(item, dict) and item.get("model") is not None
                ],
            }

        next_runs: list[float] = []
        running = None
        scheduled = bool(self.config.enabled and targets)
        for target in targets:
            pending = self.db.get_pending_account_job(target["rep_account_id"], cluster_id=target["cluster_id"])
            if pending is not None:
                if str(pending["model"]) == model and pending["state"] == "running" and running is None:
                    running = pending
                continue
            if target["next_run_at"] is not None and self._compute_target_next_model(target) == model:
                next_runs.append(float(target["next_run_at"]))
        intervals = [int(t["interval_seconds"]) for t in targets] or [self.config.idle_interval_seconds]
        return {
            "latest": shape(rows[0]) if rows else None,
            # Oldest -> newest, as the channel chart expects.
            "history": [shape(r) for r in reversed(rows)],
            "last_checked_at": float(rows[0]["checked_at"]) if rows else None,
            "next_run_at": min(next_runs) if scheduled and next_runs else None,
            "stale_after": max(STALE_AFTER_SECONDS, max(intervals) * 2),
            "scheduled": scheduled,
            "running": running,
        }

    # ------------------ Account-level Scheduling & Probing ------------------

    def _cluster_key_rotation(self, cluster_id: str, model: str) -> int:
        if not hasattr(self, "_cluster_rotations"):
            self._cluster_rotations: dict[tuple[str, str], int] = {}
        idx = self._cluster_rotations.get((cluster_id, model), 0)
        self._cluster_rotations[(cluster_id, model)] = idx + 1
        return idx

    @staticmethod
    def _representative(members: list[sqlite3.Row]) -> sqlite3.Row:
        """Lowest-id member still in the host's pool; switching one key off must
        not stop the rest of its cluster from being checked."""
        ordered = sorted(members, key=lambda a: int(a["account_id"]))
        for member in ordered:
            if bool(member["schedulable"]):
                return member
        return ordered[0]

    @staticmethod
    def _cluster_models(members: list[sqlite3.Row]) -> list[str]:
        # Only keys that can actually be probed decide what the cluster serves;
        # an all-off cluster still lists its models for the last results.
        live = [m for m in members if bool(m["schedulable"])] or members
        models: set[str] = set()
        for member in live:
            try:
                models.update(json.loads(member["models_json"]))
            except Exception:
                pass
        return sorted(models)

    @staticmethod
    def _pool_member_ids(target: dict[str, Any]) -> list[int]:
        return [int(m["account_id"]) for m in target["members"] if bool(m["schedulable"])]

    @staticmethod
    def _participation(rep: sqlite3.Row) -> dict[str, Any]:
        participating = bool(rep["schedulable"])
        reason = rep["inactive_reason"] if "inactive_reason" in rep.keys() else None
        return {
            "participating": participating,
            "inactive_reason": None if participating else (reason or "unschedulable"),
        }

    def _get_target_and_members(self, account_id: int) -> tuple[dict[str, Any] | None, list[sqlite3.Row]]:
        acc = self.db.get_account(account_id)
        if acc is None:
            return None, []
        cid = acc["cluster_id"] if "cluster_id" in acc.keys() else None
        if cid:
            all_accs = self.db.get_all_accounts()
            cluster_members = [
                a for a in all_accs
                if ("cluster_id" in a.keys() and a["cluster_id"] == cid and str(a["mode"]) != "retired")
            ]
            if not cluster_members:
                cluster_members = [acc]
            cluster_members.sort(key=lambda a: int(a["account_id"]))
            rep = self._representative(cluster_members)
            cluster_models = self._cluster_models(cluster_members)

            real_models_list: list[str] = []
            seen_real = set()
            for m in cluster_members:
                try:
                    rm = json.loads(m["real_models_json"]) if "real_models_json" in m.keys() else []
                except Exception:
                    rm = []
                for mod in rm:
                    if mod not in seen_real:
                        seen_real.add(mod)
                        real_models_list.append(mod)

            target = {
                "target_type": "cluster",
                **self._participation(rep),
                "cluster_id": cid,
                "cluster_name": acc["cluster_name"] if "cluster_name" in acc.keys() else None,
                "rep_account_id": int(rep["account_id"]),
                "members": cluster_members,
                "member_account_ids": [int(a["account_id"]) for a in cluster_members],
                "mode": rep["mode"],
                "type": rep["type"] if "type" in rep.keys() else None,
                "interval_seconds": int(rep["interval_seconds"]),
                "next_run_at": rep["next_run_at"],
                "last_model_index": int(rep["last_model_index"]),
                "models": cluster_models,
                "real_models": real_models_list,
                "name": str(acc["name"]),
                "platform": acc["platform"],
            }
            return target, cluster_members
        else:
            try:
                models = json.loads(acc["models_json"])
            except Exception:
                models = []
            try:
                rm = json.loads(acc["real_models_json"]) if "real_models_json" in acc.keys() else []
            except Exception:
                rm = []
            target = {
                "target_type": "single",
                **self._participation(acc),
                "cluster_id": None,
                "cluster_name": None,
                "rep_account_id": int(acc["account_id"]),
                "members": [acc],
                "member_account_ids": [int(acc["account_id"])],
                "mode": acc["mode"],
                "type": acc["type"] if "type" in acc.keys() else None,
                "interval_seconds": int(acc["interval_seconds"]),
                "next_run_at": acc["next_run_at"],
                "last_model_index": int(acc["last_model_index"]),
                "models": models,
                "real_models": rm,
                "name": str(acc["name"]),
                "platform": acc["platform"],
            }
            return target, [acc]

    def _get_all_targets(self) -> list[dict[str, Any]]:
        accounts = self.db.get_all_accounts()
        non_retired = [acc for acc in accounts if str(acc["mode"]) != "retired"]
        targets: list[dict[str, Any]] = []
        cluster_groups: dict[str, list[sqlite3.Row]] = {}
        singles: list[sqlite3.Row] = []

        for acc in non_retired:
            cid = acc["cluster_id"] if "cluster_id" in acc.keys() else None
            if cid:
                cluster_groups.setdefault(cid, []).append(acc)
            else:
                singles.append(acc)

        for cid, members in cluster_groups.items():
            members.sort(key=lambda a: int(a["account_id"]))
            rep = self._representative(members)
            cluster_models = self._cluster_models(members)

            real_models_list = []
            seen_real = set()
            for m in members:
                try:
                    rm = json.loads(m["real_models_json"]) if "real_models_json" in m.keys() else []
                except Exception:
                    rm = []
                for mod in rm:
                    if mod not in seen_real:
                        seen_real.add(mod)
                        real_models_list.append(mod)

            targets.append({
                "target_type": "cluster",
                **self._participation(rep),
                "cluster_id": cid,
                "cluster_name": rep["cluster_name"] if "cluster_name" in rep.keys() else None,
                "rep_account_id": int(rep["account_id"]),
                "members": members,
                "member_account_ids": [int(a["account_id"]) for a in members],
                "mode": rep["mode"],
                "type": rep["type"] if "type" in rep.keys() else None,
                "interval_seconds": int(rep["interval_seconds"]),
                "next_run_at": rep["next_run_at"],
                "last_model_index": int(rep["last_model_index"]),
                "models": cluster_models,
                "real_models": real_models_list,
                "name": str(rep["name"]),
                "platform": rep["platform"],
            })

        for acc in singles:
            try:
                models = json.loads(acc["models_json"])
            except Exception:
                models = []
            try:
                rm = json.loads(acc["real_models_json"]) if "real_models_json" in acc.keys() else []
            except Exception:
                rm = []
            targets.append({
                "target_type": "single",
                **self._participation(acc),
                "cluster_id": None,
                "cluster_name": None,
                "rep_account_id": int(acc["account_id"]),
                "members": [acc],
                "member_account_ids": [int(acc["account_id"])],
                "mode": acc["mode"],
                "type": acc["type"] if "type" in acc.keys() else None,
                "interval_seconds": int(acc["interval_seconds"]),
                "next_run_at": acc["next_run_at"],
                "last_model_index": int(acc["last_model_index"]),
                "models": models,
                "real_models": rm,
                "name": str(acc["name"]),
                "platform": acc["platform"],
            })

        targets.sort(key=lambda t: t["rep_account_id"])
        return targets

    def _is_target_apikey(self, target: dict[str, Any]) -> bool:
        t_type = target.get("type")
        if t_type:
            return str(t_type).lower() != "oauth"
        members = target.get("members", [])
        if members:
            m_type = members[0]["type"] if "type" in members[0].keys() else None
            if m_type:
                return str(m_type).lower() != "oauth"
        return True

    def _is_model_paused(self, target: dict[str, Any], model: str, now: float) -> bool:
        for m in target.get("members", []):
            acc_row = self.db.get_account(int(m["account_id"])) or m
            try:
                pms = json.loads(acc_row["paused_models_json"]) if "paused_models_json" in acc_row.keys() and acc_row["paused_models_json"] else []
            except Exception:
                pms = []
            for pm in pms:
                if pm.get("model") == model and pm.get("until"):
                    try:
                        u_ts = datetime.fromisoformat(str(pm["until"]).replace("Z", "+00:00")).timestamp()
                        if u_ts > now:
                            return True
                    except Exception:
                        pass
        return False

    def _has_paused_models(self, target: dict[str, Any], now: float) -> bool:
        for m in target.get("members", []):
            acc_row = self.db.get_account(int(m["account_id"])) or m
            try:
                pms = json.loads(acc_row["paused_models_json"]) if "paused_models_json" in acc_row.keys() and acc_row["paused_models_json"] else []
            except Exception:
                pms = []
            for pm in pms:
                if pm.get("until"):
                    try:
                        u_ts = datetime.fromisoformat(str(pm["until"]).replace("Z", "+00:00")).timestamp()
                        if u_ts > now:
                            return True
                    except Exception:
                        pass
        return False

    def _get_model_paused_until(self, members: list[sqlite3.Row], model: str, now: float) -> str | None:
        latest_ts: float | None = None
        latest_str: str | None = None
        for m in members:
            acc_row = self.db.get_account(int(m["account_id"])) or m
            try:
                pms = json.loads(acc_row["paused_models_json"]) if "paused_models_json" in acc_row.keys() and acc_row["paused_models_json"] else []
            except Exception:
                pms = []
            for pm in pms:
                if pm.get("model") == model and pm.get("until"):
                    u_str = str(pm["until"])
                    try:
                        u_ts = datetime.fromisoformat(u_str.replace("Z", "+00:00")).timestamp()
                        if u_ts > now:
                            if latest_ts is None or u_ts > latest_ts:
                                latest_ts = u_ts
                                latest_str = u_str
                    except Exception:
                        pass
        return latest_str

    def _update_member_paused_model(self, account_id: int, model: str, until: str | None) -> None:
        try:
            acc = self.db.get_account(account_id)
            if acc is None:
                return
            pms = json.loads(acc["paused_models_json"]) if "paused_models_json" in acc.keys() and acc["paused_models_json"] else []
            new_pms = [pm for pm in pms if pm.get("model") != model]
            if until is not None:
                new_pms.append({"model": model, "until": until})
            new_json = json.dumps(new_pms, ensure_ascii=False, separators=(",", ":"))
            with self.db._lock:
                self.db._conn.execute(
                    "UPDATE accounts SET paused_models_json = ?, updated_at = ? WHERE account_id = ?",
                    (new_json, self.clock(), account_id),
                )
        except Exception:
            pass

    def _handle_auto_pause_resume(
        self,
        *,
        target: dict[str, Any],
        members: list[sqlite3.Row],
        model: str,
        status: str,
        message_code: str,
        best_model: str | None,
        now: float,
    ) -> None:
        if not self.config.enabled or not self.config.auto_pause_enabled:
            return
        if not self.host_client:
            return

        supporting_members = []
        for m in members:
            try:
                m_models = json.loads(m["models_json"])
            except Exception:
                m_models = []
            if model in m_models:
                supporting_members.append(m)
        if not supporting_members:
            return
        # A cluster result must not change any member using unverified evidence.
        # Explicit account/route/effort calibration is required for EVERY member.
        if not all(self.config.auto_actions_calibrated(int(m["account_id"]), model)
                   for m in supporting_members):
            return

        if status == "suspect":
            evidence = f"{message_code} {best_model}" if best_model else message_code
            for m in supporting_members:
                acct_id = int(m["account_id"])
                acc_row = self.db.get_account(acct_id) or m
                acct_type = str(acc_row["type"]).lower() if acc_row["type"] else ""
                minutes = getattr(self.config, "pause_minutes_oauth", 1440) if acct_type == "oauth" else getattr(self.config, "pause_minutes_apikey", 60)
                try:
                    paused = bool(self.host_client.pause_model(acct_id, model, minutes, evidence))
                except Exception as exc:
                    paused = False
                    logger.warning("pause_model call failed for account %s: %s", acct_id, type(exc).__name__)
                # Show a pause only once the host has actually applied it.
                if paused:
                    self._update_member_paused_model(acct_id, model, utc_iso(now + minutes * 60))

        elif status == "match":
            is_paused = False
            for m in supporting_members:
                acc_row = self.db.get_account(int(m["account_id"])) or m
                try:
                    pms = json.loads(acc_row["paused_models_json"]) if "paused_models_json" in acc_row.keys() and acc_row["paused_models_json"] else []
                except Exception:
                    pms = []
                for pm in pms:
                    if pm.get("model") == model and pm.get("until"):
                        try:
                            u_ts = datetime.fromisoformat(str(pm["until"]).replace("Z", "+00:00")).timestamp()
                            if u_ts > now:
                                is_paused = True
                                break
                        except Exception:
                            pass
                if is_paused:
                    break

            if is_paused:
                for m in supporting_members:
                    acct_id = int(m["account_id"])
                    try:
                        resumed = bool(self.host_client.resume_model(acct_id, model))
                    except Exception as exc:
                        resumed = False
                        logger.warning("resume_model call failed for account %s: %s", acct_id, type(exc).__name__)
                    if resumed:
                        self._update_member_paused_model(acct_id, model, None)

    def _compute_target_next_model(self, target: dict[str, Any]) -> str | None:
        models = target.get("models", [])
        if not models:
            return None
        if self._is_target_apikey(target):
            paused_models = [m for m in models if self._is_model_paused(target, m, self.clock())]
            if paused_models:
                idx = int(target.get("last_model_index", 0)) % len(paused_models)
                return paused_models[idx]
        mode = str(target.get("mode", "idle"))
        real_models = target.get("real_models", [])
        if mode == "active":
            valid_reals = [m for m in real_models if m in models]
            if valid_reals:
                idx = int(target.get("last_model_index", 0)) % len(valid_reals)
                return valid_reals[idx]
        idx = int(target.get("last_model_index", 0)) % len(models)
        return models[idx]

    def _select_model_for_target(self, target: dict[str, Any]) -> str | None:
        models = target.get("models", [])
        if not models:
            return None
        if self._is_target_apikey(target):
            paused_models = [m for m in models if self._is_model_paused(target, m, self.clock())]
            if paused_models:
                idx = int(target.get("last_model_index", 0)) % len(paused_models)
                chosen = paused_models[idx]
                next_idx = (idx + 1) % len(paused_models)
                self.db.set_account_last_model_index(target["rep_account_id"], next_idx, now=self.clock())
                return chosen
        mode = str(target.get("mode", "idle"))
        real_models = target.get("real_models", [])
        chosen = None
        if mode == "active":
            valid_reals = [m for m in real_models if m in models]
            if valid_reals:
                idx = int(target.get("last_model_index", 0)) % len(valid_reals)
                chosen = valid_reals[idx]
                next_idx = (idx + 1) % len(valid_reals)
                self.db.set_account_last_model_index(target["rep_account_id"], next_idx, now=self.clock())
                return chosen
        idx = int(target.get("last_model_index", 0)) % len(models)
        chosen = models[idx]
        next_idx = (idx + 1) % len(models)
        self.db.set_account_last_model_index(target["rep_account_id"], next_idx, now=self.clock())
        return chosen

    def _select_model_for_account(self, acc: Any) -> str | None:
        target, _ = self._get_target_and_members(int(acc["account_id"]))
        if target is None:
            return None
        return self._select_model_for_target(target)

    def _compute_idle_next_run(self, account_id: int, now: float, cluster_id: str | None = None) -> float:
        cid = cluster_id
        if cid is None:
            acc = self.db.get_account(account_id)
            cid = acc["cluster_id"] if (acc and "cluster_id" in acc.keys()) else None
        if cid:
            hash_str = f"cluster:{cid}"
        else:
            hash_str = str(account_id)
        offset = int(hashlib.sha256(hash_str.encode("utf-8")).hexdigest(), 16) % 3600
        hour_start = (int(now) // 3600) * 3600
        candidate = hour_start + offset
        if candidate <= now:
            candidate += 3600
        return float(candidate)

    def _fetch_accounts_before_deadline(self, models: list[str], deadline: float) -> Any:
        # httpx timeouts bound individual I/O phases, not total response time.
        # Keep at most one daemon fetch in flight and discard timed-out results;
        # a late response must never publish stale rows after a newer save/GET.
        previous = self._account_fetch_thread
        if previous is not None and previous.is_alive():
            previous.join(max(0.0, deadline - time.monotonic()))
            if previous.is_alive():
                return None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        result: list[Any] = []
        host_client = self.host_client

        def fetch() -> None:
            try:
                result.append(host_client.fetch_accounts(models))
            except Exception as exc:
                logger.warning("host account sync failed: %s", type(exc).__name__)
                result.append(([], False))

        self._account_fetch_thread = threading.Thread(target=fetch, daemon=True, name="modeltrace-account-fetch")
        self._account_fetch_thread.start()
        self._account_fetch_thread.join(max(0.0, deadline - time.monotonic()))
        if self._account_fetch_thread.is_alive():
            return None
        return result[0] if result else None

    def _refresh_accounts(self, now: float, *, if_due: bool = False) -> None:
        if not self.host_client or (if_due and not self.config.per_account_enabled):
            return
        # Account GETs share a 4s budget for lock, host and SQLite admission,
        # below the existing Go proxy's 5s timeout. No paid probe holds this lock.
        deadline = time.monotonic() + ACCOUNT_SYNC_TIMEOUT_SECONDS
        if not self._account_refresh_lock.acquire(timeout=ACCOUNT_SYNC_TIMEOUT_SECONDS):
            return
        try:
            if if_due and self._last_account_refresh_at is not None and now - self._last_account_refresh_at < 60:
                return
            self._last_account_refresh_at = self.clock()
            all_models = sorted(set(self.bank_models) | set(self.config.model_aliases))
            res = self._fetch_accounts_before_deadline(all_models, deadline)
            if res is None or time.monotonic() >= deadline:
                logger.warning("host account sync deadline exceeded; retaining committed snapshot")
                return
            if isinstance(res, tuple):
                account_list, fetch_ok = res
            else:
                account_list, fetch_ok = res, True
            if not fetch_ok:
                # Never replay HostClient's old cached payload on failure. Only
                # heal schedules from already committed local state.
                with self.db.account_sync_transaction(timeout_seconds=deadline - time.monotonic()):
                    self._reconcile_active_account_schedules(now)
                return
            if not isinstance(account_list, list):
                logger.warning("host account sync rejected a non-list payload")
                return
            with self.db.account_sync_transaction(timeout_seconds=deadline - time.monotonic()):
                self._apply_account_sync(account_list, now)
        except (KeyError, TypeError, ValueError):
            # Invalid data is not an authoritative deletion. Roll back the whole
            # batch, including any model changes already applied to other keys.
            logger.warning("host account sync rejected an invalid account payload")
        except TimeoutError:
            logger.warning("account sync lock deadline exceeded; retaining committed snapshot")
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            logger.warning("account sync database is busy; retaining committed snapshot")
        finally:
            self._account_refresh_lock.release()

    def _apply_account_sync(self, account_list: list[dict[str, Any]], now: float) -> None:
        previous_clusters: dict[str, list[sqlite3.Row]] = {}
        for account in self.db.get_all_accounts():
            if account["cluster_id"] and account["mode"] != "retired":
                previous_clusters.setdefault(account["cluster_id"], []).append(account)
        fetched_ids = set()
        active_clusters = set()
        cluster_reals: dict[str, list[str]] = {}

        # First pass: collect cluster active status & real models from openai accounts
        for acc in account_list:
            plat = acc.get("platform")
            if plat is not None and plat != "openai":
                continue
            acct_id = int(acc["account_id"])
            fetched_ids.add(acct_id)
            cid = acc.get("cluster_id")
            last_real_str = acc.get("last_real_request_at")
            last_real_ts = None
            if last_real_str:
                try:
                    last_real_ts = datetime.fromisoformat(last_real_str.replace("Z", "+00:00")).timestamp()
                except Exception:
                    last_real_ts = None

            is_active = (last_real_ts is not None and (now - last_real_ts <= self.config.active_window_seconds))
            if cid:
                if is_active:
                    active_clusters.add(cid)
                rms = acc.get("real_models_10m", [])
                if rms:
                    for rm in rms:
                        if self.config.calibration_model_for(rm) in self.bank_models:
                            cluster_reals.setdefault(cid, [])
                            if rm not in cluster_reals[cid]:
                                cluster_reals[cid].append(rm)

        # Second pass: upsert each account
        for acc in account_list:
            plat = acc.get("platform")
            if plat is not None and plat != "openai":
                continue
            acct_id = int(acc["account_id"])
            name = str(acc.get("name", ""))
            platform = acc.get("platform")
            type_ = acc.get("type")
            schedulable = bool(acc.get("schedulable", True))
            raw_models = acc.get("models", [])
            valid_models = [m for m in raw_models if self.config.calibration_model_for(m) in self.bank_models]
            last_real_str = acc.get("last_real_request_at")
            last_real_ts = None
            if last_real_str:
                try:
                    last_real_ts = datetime.fromisoformat(last_real_str.replace("Z", "+00:00")).timestamp()
                except Exception:
                    last_real_ts = None
            last_real_model = acc.get("last_real_model")
            real_requests_10m = int(acc.get("real_requests_10m", 0))

            cid = acc.get("cluster_id")
            cname = acc.get("cluster_name")
            if cid:
                is_active = cid in active_clusters
                real_models = cluster_reals.get(cid, [])
            else:
                is_active = (last_real_ts is not None and (now - last_real_ts <= self.config.active_window_seconds))
                rms = acc.get("real_models_10m", [])
                real_models = [m for m in rms if self.config.calibration_model_for(m) in self.bank_models] if rms else []

            paused_models = acc.get("paused_models", [])
            if not isinstance(paused_models, list):
                paused_models = []
            inactive_reason = None if schedulable else (str(acc.get("inactive_reason") or "") or "unschedulable")

            mode = "active" if is_active else "idle"
            interval = self.config.active_interval_seconds if is_active else self.config.idle_interval_seconds

            existing = self.db.get_account(acct_id)
            if existing is None:
                if is_active:
                    # Cluster deadlines depend on all members' existing rounds,
                    # not on when a new lowest-id key was discovered.
                    initial_next_run = None if cid else now
                else:
                    initial_next_run = self._compute_idle_next_run(acct_id, now, cluster_id=cid)
                    if (str(type_).lower() != "oauth") and any(pm.get("until") for pm in paused_models):
                        initial_next_run = min(initial_next_run, now + getattr(self.config, "paused_recheck_seconds_apikey", 1800))
                self.db.upsert_account_sync(
                    account_id=acct_id,
                    name=name,
                    platform=platform,
                    type_=type_,
                    schedulable=schedulable,
                    models=valid_models,
                    last_real_request_at=last_real_ts,
                    last_real_model=last_real_model,
                    real_requests_10m=real_requests_10m,
                    mode=mode,
                    interval_seconds=interval,
                    next_run_at=initial_next_run,
                    cluster_id=cid,
                    cluster_name=cname,
                    real_models=real_models,
                    paused_models=paused_models,
                    inactive_reason=inactive_reason,
                    now=now,
                )
            else:
                old_mode = existing["mode"]
                self.db.upsert_account_sync(
                    account_id=acct_id,
                    name=name,
                    platform=platform,
                    type_=type_,
                    schedulable=schedulable,
                    models=valid_models,
                    last_real_request_at=last_real_ts,
                    last_real_model=last_real_model,
                    real_requests_10m=real_requests_10m,
                    mode=mode,
                    interval_seconds=interval,
                    next_run_at=None,
                    cluster_id=cid,
                    cluster_name=cname,
                    real_models=real_models,
                    paused_models=paused_models,
                    inactive_reason=inactive_reason,
                    now=now,
                )
                if (str(type_).lower() != "oauth") and any(pm.get("until") for pm in paused_models):
                    cur_acc = self.db.get_account(acct_id)
                    recheck_limit = getattr(self.config, "paused_recheck_seconds_apikey", 1800)
                    if cur_acc and cur_acc["next_run_at"] is not None and float(cur_acc["next_run_at"]) > now + recheck_limit:
                        self.db.set_account_next_run(acct_id, now + recheck_limit, now=now)
                if old_mode == "retired":
                    if is_active:
                        self.db.set_account_next_run(acct_id, now, now=now)
                    else:
                        idle_next = self._compute_idle_next_run(acct_id, now, cluster_id=cid)
                        self.db.set_account_next_run(acct_id, idle_next, now=now)
                elif old_mode == "active" and not is_active:
                    idle_next = self._compute_idle_next_run(acct_id, now, cluster_id=cid)
                    self.db.set_account_next_run(acct_id, idle_next, now=now)

        for local_acc in self.db.get_all_accounts():
            local_id = int(local_acc["account_id"])
            if local_id not in fetched_ids and str(local_acc["mode"]) != "retired":
                self.db.retire_account(local_id, now=now)

        # Align scheduling for clusters: only the representative has next_run_at; others are None.
        # If representative changed, carry over next_run_at and last_model_index.
        all_accs = self.db.get_all_accounts()
        cluster_accs: dict[str, list[sqlite3.Row]] = {}
        for a in all_accs:
            if str(a["mode"]) != "retired" and ("cluster_id" in a.keys() and a["cluster_id"]):
                cluster_accs.setdefault(a["cluster_id"], []).append(a)

        for cid, members in cluster_accs.items():
            members.sort(key=lambda x: int(x["account_id"]))
            rep = self._representative(members)
            others = [m for m in members if int(m["account_id"]) != int(rep["account_id"])]
            rep_id = int(rep["account_id"])
            rep_next = rep["next_run_at"]
            rep_model_idx = rep["last_model_index"]
            old_members = previous_clusters.get(cid, [])
            old_rep = self._representative(old_members) if old_members else None
            if old_rep is not None and int(old_rep["account_id"]) != rep_id:
                # Capture before retirement clears the old representative's
                # deadline. A newly discovered rep must not reset it to now.
                rep_next = old_rep["next_run_at"]
                rep_model_idx = old_rep["last_model_index"]
                self.db.set_account_next_run(rep_id, rep_next, now=now)
                self.db.set_account_last_model_index(rep_id, int(rep_model_idx), now=now)

            # If rep has no next_run_at, see if any other member had one
            if rep_next is None:
                for other in others:
                    if other["next_run_at"] is not None:
                        rep_next = other["next_run_at"]
                        rep_model_idx = other["last_model_index"]
                        self.db.set_account_next_run(rep_id, float(rep_next), now=now)
                        self.db.set_account_last_model_index(rep_id, int(rep_model_idx), now=now)
                        break

            # If still None, initialize it
            if rep_next is None:
                if rep["mode"] == "active":
                    rounds = [self.db.get_account_latest_round(int(m["account_id"])) for m in members]
                    checked = [float(r["checked_at"]) for r in rounds if r is not None]
                    rep_next = max(now, max(checked) + self.config.active_interval_seconds) if checked else now
                else:
                    rep_next = self._compute_idle_next_run(rep_id, now, cluster_id=cid)
                self.db.set_account_next_run(rep_id, rep_next, now=now)

            rep_acc = self.db.get_account(rep_id)
            if rep_acc and str(rep_acc["type"]).lower() != "oauth":
                try:
                    has_pms = any(pm.get("until") for m in members for pm in json.loads(m["paused_models_json"] or "[]"))
                except Exception:
                    has_pms = False
                if has_pms:
                    cur_rn = rep_acc["next_run_at"]
                    recheck_limit = getattr(self.config, "paused_recheck_seconds_apikey", 1800)
                    if cur_rn is not None and float(cur_rn) > now + recheck_limit:
                        self.db.set_account_next_run(rep_id, now + recheck_limit, now=now)

            # Ensure all non-rep members have next_run_at = NULL
            for non_rep in others:
                if non_rep["next_run_at"] is not None:
                    self.db.set_account_next_run(int(non_rep["account_id"]), None, now=now)

        self._reconcile_active_account_schedules(now)

    def _reconcile_active_account_schedules(self, now: float) -> None:
        # Reconcile AFTER choosing cluster representatives: the latest result
        # can belong to a fallback key. An idle -> active transition must shorten
        # the hourly deadline even when the last round is less than 5 minutes old.
        # Recheck already-active targets too, to heal persisted stale deadlines.
        latest_rounds = self.db.get_latest_rounds_for_all_accounts()
        for target in self._get_all_targets():
            if target["mode"] != "active" or not target["participating"]:
                continue
            checked_times = [
                float(latest_rounds[aid]["checked_at"])
                for aid in target["member_account_ids"] if aid in latest_rounds
            ]
            active_due = (
                max(now, max(checked_times) + self.config.active_interval_seconds)
                if checked_times else now
            )
            current_due = target["next_run_at"]
            # Only pull a deadline forward; polling must never postpone work.
            if current_due is None or float(current_due) > active_due:
                self.db.set_account_next_run(target["rep_account_id"], active_due, now=now)

    def _schedule_due_accounts(self, now: float) -> None:
        if not self.config.enabled or not self.config.per_account_enabled:
            return
        targets = self._get_all_targets()
        for target in targets:
            rep_id = target["rep_account_id"]
            rep_acc = self.db.get_account(rep_id)
            if not rep_acc or not bool(rep_acc["schedulable"]):
                continue
            due_at = rep_acc["next_run_at"]
            if due_at is None or self.db.has_pending_account(rep_id, cluster_id=target["cluster_id"]):
                continue
            due_at_val = float(due_at)
            interval = int(target["interval_seconds"])
            mode = str(target["mode"])

            is_apikey = self._is_target_apikey(target)
            has_paused = self._has_paused_models(target, now)
            recheck_limit = getattr(self.config, "paused_recheck_seconds_apikey", 1800)
            if is_apikey and has_paused:
                interval = min(interval, recheck_limit)
                if due_at_val > now + recheck_limit:
                    due_at_val = now + recheck_limit
                    self.db.set_account_next_run(rep_id, due_at_val, now=now)

            # Skip missed intervals without replaying
            if (now - due_at_val) > interval:
                if mode == "idle":
                    new_due = self._compute_idle_next_run(rep_id, now)
                    if is_apikey and has_paused:
                        new_due = min(new_due, now + recheck_limit)
                else:
                    new_due = now
                self.db.set_account_next_run(rep_id, new_due, now=now)
                due_at_val = new_due

            if due_at_val <= now:
                model_to_use = self._select_model_for_target(target)
                if not model_to_use:
                    continue
                queued, qid = self.db.enqueue_account(
                    rep_id,
                    model_to_use,
                    now=now,
                    available_at=now,
                    trigger="scheduled",
                    retest_index=0,
                )
                if queued:
                    next_run = now + interval if mode == "active" else self._compute_idle_next_run(rep_id, now)
                    if is_apikey and has_paused:
                        next_run = min(next_run, now + recheck_limit)
                    self.db.set_account_next_run(rep_id, next_run, now=now)

    def _run_account_job_safely(self, job: Any) -> None:
        try:
            self._run_account_job(job)
        except Exception:
            self._record_account_internal_error(job)
        finally:
            self._last_round_completed_at = self.clock()

    def _record_account_internal_error(self, job: Any) -> None:
        now = self.clock()
        acc = self.db.get_account(job.account_id)
        interval = int(acc["interval_seconds"]) if acc else self.config.idle_interval_seconds
        next_run = now + interval
        self.db.record_account_round_and_finish(
            job,
            status="error",
            target_probability=None,
            best_model=None,
            checked_at=now,
            message_code="internal_error",
            reasoning_effort=None,
            ranking=[],
            diagnostics={"request_count": 0, "valid_count": 0},
            next_run_at=next_run,
        )

    def _run_account_job(self, job: Any) -> None:
        model = job.model
        rep_account_id = job.account_id
        target, members = self._get_target_and_members(rep_account_id)
        if (target is None or not target["participating"] or target["mode"] == "retired"
                or model not in target["models"] or self.config.calibration_model_for(model) not in self.bank_models):
            self.db.cancel_account_job(job.queue_id, now=self.clock(), reason="account_or_model_unavailable")
            return
        rep_account_id = target["rep_account_id"]

        # Find eligible members supporting this model and schedulable
        eligible_members: list[sqlite3.Row] = []
        for m in members:
            if not bool(m["schedulable"]):
                continue
            try:
                m_models = json.loads(m["models_json"])
            except Exception:
                m_models = []
            if model in m_models:
                eligible_members.append(m)

        if not eligible_members:
            self.db.cancel_account_job(job.queue_id, now=self.clock(), reason="account_or_model_unavailable")
            return

        reasoning_effort = self.config.reasoning_effort_for(model)

        def is_key_failure(status_code: int | None, error_code: str | None) -> bool:
            # 超时、解析失败、模型不符不换 Key
            if error_code in ("upstream_timeout", "timeout", "model_mismatch", "upstream_model_mismatch"):
                return False
            if error_code == "account_unavailable":
                return True
            if status_code is not None:
                if status_code in (401, 402, 403, 429, 503) or status_code >= 500:
                    return True
            return False

        attempts: list[dict[str, Any]] = []
        total_requests = 0
        questions_valid = 0
        wrong_count = 0
        previous_samples: list[dict[str, Any]] = []
        other_model_counts: dict[str, int] = {}
        last_tested_sample: dict[str, Any] | None = None
        last_tested_account_id: int = int(eligible_members[0]["account_id"])
        last_error_code: str = "probe_failed"

        conclusion_status: str | None = None
        conclusion_message_code: str | None = None
        conclusion_best_model: str | None = None

        while total_requests < 5 and conclusion_status is None:
            requests_before_cycle = total_requests
            challenges = generate_challenges(count=1)
            if len(challenges) != 1:
                raise ValueError("challenge_generation_failed")
            challenge = challenges[0]
            if len(str(challenge.get("prompt", "")).encode("utf-8")) > self.config.max_prompt_bytes:
                raise ValueError("prompt_too_large")

            expected = int(challenge.get("expected_count") or 0)
            user_agent = f"ModelTraceProbe/{uuid.uuid4()}"
            session_affinity = str(uuid.uuid4())

            cid = target.get("cluster_id")
            if cid:
                rot_idx = self._cluster_key_rotation(cid, model) % len(eligible_members)
                ordered_members = eligible_members[rot_idx:] + eligible_members[:rot_idx]
            else:
                ordered_members = eligible_members

            for member in ordered_members:
                if total_requests >= 5:
                    break

                curr_acct_id = int(member["account_id"])
                current_member = self.db.get_account(curr_acct_id)
                if (current_member is None or not current_member["schedulable"]
                        or current_member["mode"] == "retired"
                        or current_member["cluster_id"] != target["cluster_id"]
                        or model not in json.loads(current_member["models_json"])):
                    continue
                total_requests += 1
                try:
                    result = self.transport.run(
                        model=model,
                        challenge=challenge,
                        user_agent=user_agent,
                        session_affinity=session_affinity,
                        account_id=curr_acct_id,
                    )
                except Exception:
                    result = ProbeResult(None, None, "transport_error")

                detail = result.transport_detail or {}
                structured_mismatch = result.upstream_model is not None and result.upstream_model != model
                parsed_numbers_count = 0
                is_complete = False

                if result.text is not None and result.error_code is None and not structured_mismatch:
                    numbers = parse_numbers(result.text)
                    parsed_numbers_count = len(numbers)
                    is_complete = parsed_numbers_count >= max(80, math.ceil(expected * 0.55))

                transport_error = result.error_code
                if structured_mismatch:
                    transport_error = "upstream_model_mismatch"
                is_upstream_error = transport_error is not None or structured_mismatch

                if is_upstream_error:
                    err_code = transport_error or "probe_failed"
                    last_error_code = err_code
                    attempts.append({
                        "account_id": curr_acct_id,
                        "outcome": None,
                        "prediction": None,
                        "target_probability": None,
                        "error_code": err_code,
                    })
                    if is_key_failure(result.status_code, result.error_code):
                        continue
                    else:
                        break

                if not is_complete:
                    err_code = "output_truncated" if (
                        detail.get("termination_event_received") is False
                        or detail.get("status") in ("incomplete", "in_progress", "failed")
                        or detail.get("incomplete_reason") == "max_output_tokens"
                    ) else "insufficient_sample"
                    last_error_code = err_code
                    attempts.append({
                        "account_id": curr_acct_id,
                        "outcome": None,
                        "prediction": None,
                        "target_probability": None,
                        "error_code": err_code,
                    })
                    break

                # The question was successfully tested!
                analysis = analyze_outputs([{"text": result.text, "expected_count": expected}], self.bank)
                pred_model = str(analysis["prediction"])
                results = analysis.get("results", [])
                normalized_results = [
                    {
                        "model": str(item["model"]),
                        "probability": _clamp_probability(item.get("probability")) or 0.0,
                    }
                    for item in results
                    if isinstance(item, dict) and item.get("model") is not None
                ]
                ranking = [
                    {"model": item["model"], "probability": round(item["probability"], 6)}
                    for item in normalized_results[:3]
                ]
                target_prob_item = next(
                    (item["probability"] for item in normalized_results if item["model"] == self.config.calibration_model_for(model)),
                    None,
                )
                target_prob = _clamp_probability(target_prob_item)

                sample_class = classify_sample(analysis, self.config.calibration_model_for(model), previous=previous_samples)
                outcome = sample_class["outcome"]
                previous_samples.append({"prediction": pred_model, "outcome": outcome})

                questions_valid += 1
                last_tested_account_id = curr_acct_id
                last_tested_sample = {
                    "target_probability": target_prob,
                    "best_model": pred_model,
                    "ranking": ranking,
                }

                attempts.append({
                    "account_id": curr_acct_id,
                    "outcome": outcome,
                    "prediction": pred_model,
                    "target_probability": target_prob,
                    "error_code": None,
                })

                if outcome == "compatible":
                    conclusion_status = "match"
                    conclusion_message_code = "compatible"
                    conclusion_best_model = pred_model
                elif outcome in ("difference_signal", "repeated_difference"):
                    wrong_count += 1
                    other_model_counts[pred_model] = other_model_counts.get(pred_model, 0) + 1
                    if wrong_count >= 3:
                        conclusion_status = "suspect"
                        conclusion_message_code = "repeated_other_model"
                        # best_model 取出现最多的那个预测模型
                        conclusion_best_model = max(other_model_counts.items(), key=lambda x: x[1])[0]

                break

            if total_requests == requests_before_cycle:
                # A refresh may remove all remaining candidates between retries.
                # Do not loop forever or manufacture a result without sending.
                if total_requests == 0:
                    self.db.cancel_account_job(job.queue_id, now=self.clock(), reason="account_or_model_unavailable")
                    return
                break

        # Check loop conclusion or hit limit
        finish = self.clock()
        original_mode = str(target["mode"])
        original_interval = int(target["interval_seconds"])
        # A force refresh may change the representative while HTTP is in flight.
        # Publish completion to the current target, not the queue's former key.
        current_target, current_members = self._get_target_and_members(job.account_id)
        if current_target is not None:
            target, members = current_target, current_members
            rep_account_id = target["rep_account_id"]
        mode = str(target["mode"])
        interval = int(target["interval_seconds"])
        regular_next_run = (finish + interval) if mode == "active" else self._compute_idle_next_run(rep_account_id, finish)
        # A mode change may shorten, but must not postpone, the deadline that
        # this in-flight job would otherwise have produced.
        original_next_run = ((finish + original_interval) if original_mode == "active"
                             else self._compute_idle_next_run(rep_account_id, finish))
        regular_next_run = min(regular_next_run, original_next_run)

        if conclusion_status is not None:
            final_status = conclusion_status
            final_message_code = conclusion_message_code
            final_best_model = conclusion_best_model
            final_target_prob = last_tested_sample.get("target_probability") if last_tested_sample else None
            final_ranking = last_tested_sample.get("ranking", []) if last_tested_sample else []
            final_account_id = last_tested_account_id
        else:
            # Reached request limit (5 requests) without match or 3 wrong
            if questions_valid >= 1:
                final_status = "uncertain"
                final_message_code = "low_or_competing_probability"
                final_best_model = last_tested_sample.get("best_model") if last_tested_sample else None
                final_target_prob = last_tested_sample.get("target_probability") if last_tested_sample else None
                final_ranking = last_tested_sample.get("ranking", []) if last_tested_sample else []
                final_account_id = last_tested_account_id
            else:
                final_status = "error"
                final_message_code = last_error_code
                final_best_model = None
                final_target_prob = None
                final_ranking = []
                final_account_id = attempts[-1]["account_id"] if attempts else int(eligible_members[0]["account_id"])

        next_run = regular_next_run
        is_apikey = self._is_target_apikey(target)
        if is_apikey and (final_status == "suspect" or self._has_paused_models(target, finish)):
            next_run = min(regular_next_run, finish + getattr(self.config, "paused_recheck_seconds_apikey", 1800))

        exec_job = replace(job, account_id=final_account_id)
        self.db.record_account_round_and_finish(
            exec_job,
            schedule_account_id=rep_account_id,
            status=final_status,
            target_probability=final_target_prob,
            best_model=final_best_model,
            checked_at=finish,
            message_code=final_message_code,
            reasoning_effort=reasoning_effort,
            ranking=final_ranking,
            diagnostics={
                "trigger": job.trigger,
                "request_count": total_requests,
                "valid_count": questions_valid,
                "attempts": attempts,
                "questions_valid": questions_valid,
                "wrong_count": wrong_count,
                "reasoning_effort": reasoning_effort,
                "cost_controls_enabled": False,
            },
            next_run_at=next_run,
        )

        self._handle_auto_pause_resume(
            target=target,
            members=members,
            model=model,
            status=final_status,
            message_code=final_message_code,
            best_model=final_best_model,
            now=finish,
        )

    def _enqueue_model_on_all_accounts(self, model: str, *, now: float) -> int:
        """Queue one check of ``model`` on every current target that serves it."""
        queued_count = 0
        targets = self._get_all_targets()
        for target in targets:
            if not target["participating"]:
                continue
            if model not in target["models"] or self.db.has_pending_account(target["rep_account_id"], cluster_id=target["cluster_id"]):
                continue
            queued, _ = self.db.enqueue_account(
                target["rep_account_id"], model, now=now, available_at=now, trigger="manual", retest_index=0,
            )
            queued_count += int(bool(queued))
        return queued_count

    def reset_account_model(self, account_id: int, *, model: str) -> dict[str, Any]:
        """Manual reset: lift the checker's pause for ``model`` on every member and
        clear the suspect verdict with a neutral ``reset`` round."""
        target, members = self._get_target_and_members(account_id)
        if target is None:
            raise UnknownAccount(account_id)
        if not isinstance(model, str) or model not in target["models"]:
            raise ModelNotSupported(model)
        now = self.clock()
        rep_id = target["rep_account_id"]
        resumed = 0
        failed_accounts = []
        for m in members:
            try:
                m_models = json.loads(m["models_json"])
            except Exception:
                m_models = []
            if model not in m_models:
                continue
            acct_id = int(m["account_id"])
            confirmed = False
            if self.host_client is not None:
                try:
                    confirmed = bool(self.host_client.resume_model(acct_id, model))
                except Exception as exc:
                    logger.warning("resume_model call failed for account %s: %s", acct_id, type(exc).__name__)
            if confirmed:
                resumed += 1
                self._update_member_paused_model(acct_id, model, None)
            else:
                failed_accounts.append(acct_id)
        if failed_accounts:
            return {"reset": False, "resumed": resumed, "failed_account_ids": failed_accounts}
        self.db.insert_account_round(
            account_id=rep_id, model=model, status="reset", message_code="manual_reset",
            checked_at=now, diagnostics={"manual_reset": True},
        )
        return {"reset": True, "resumed": resumed}

    def enqueue_manual_account(
        self, account_id: int, *, model: str | None = None, force_refresh: bool = False,
    ) -> EnqueueResult:
        now = self.clock()
        # HTTP admission refreshes even immediately after a periodic fetch.
        if force_refresh and self.config.per_account_enabled:
            self._refresh_accounts(now)
        target, members = self._get_target_and_members(account_id)
        if target is None:
            raise UnknownAccount(account_id)
        if not target["participating"] or str(target["mode"]) == "retired":
            # The host refuses probes for accounts outside its pool; a manual
            # run would only overwrite the last real result with an error.
            raise NotParticipating(account_id)
        rep_id = target["rep_account_id"]
        if self.db.has_pending_account(rep_id, cluster_id=target["cluster_id"]):
            raise DuplicateQueue(account_id)

        target_models = target["models"]
        if model is not None:
            if model not in target_models:
                raise ModelNotSupported(model)
            model_to_use = model
        else:
            model_to_use = self._compute_target_next_model(target)
            if not model_to_use:
                raise UnknownAccount(account_id)

        queued, qid = self.db.enqueue_account(
            rep_id,
            model_to_use,
            now=now,
            available_at=now,
            trigger="manual",
            retest_index=0,
        )
        if not queued:
            raise DuplicateQueue(account_id)
        return EnqueueResult(True, qid, model=model_to_use)

    def account_snapshot(self, account_id: int, *, force_refresh: bool = False) -> dict[str, Any]:
        # HTTP GETs opt into synchronization without a new query parameter;
        # direct/internal projections remain read-only by default.
        if force_refresh and self.config.per_account_enabled:
            self._refresh_accounts(self.clock())
        with self.db.read_snapshot():
            return self._account_snapshot(account_id)

    def _account_snapshot(self, account_id: int, *, now: float | None = None) -> dict[str, Any]:
        now = self.clock() if now is None else now
        target, members = self._get_target_and_members(account_id)
        if target is None:
            raise UnknownAccount(account_id)

        member_ids = target["member_account_ids"]
        # Fetch enough rounds per member for the combined recent history.
        member_rounds: list[sqlite3.Row] = []
        for mid in member_ids:
            member_rounds.extend(self.db.get_account_rounds(mid, limit=12))
        member_rounds.sort(key=lambda r: (float(r["checked_at"]), int(r["id"])), reverse=True)

        def _format_acc_round(r: Any) -> dict[str, Any]:
            account_id_val = None
            if "account_id" in r.keys():
                account_id_val = int(r["account_id"])
            else:
                diag = self.db.decode_diagnostics(r)
                if "account_id" in diag:
                    account_id_val = int(diag["account_id"])
            return {
                "model": str(r["model"]),
                "status": str(r["status"]),
                "message_code": str(r["message_code"]),
                "target_probability": _clamp_probability(r["target_probability"]),
                "best_model": str(r["best_model"]) if r["best_model"] is not None else None,
                "checked_at": utc_iso(float(r["checked_at"])),
                "reasoning_effort": str(r["reasoning_effort"]) if r["reasoning_effort"] is not None else "none",
                "account_id": account_id_val,
                "ranking": [
                    {
                        "model": str(item.get("model")),
                        "probability": round(_clamp_probability(item.get("probability")) or 0.0, 6),
                    }
                    for item in self.db.decode_ranking(r)[:3]
                    if isinstance(item, dict) and item.get("model") is not None
                ],
            }

        target_models = target["models"]
        per_model = []
        for mod in target_models:
            # Limit each model independently: a frequently checked model must
            # not push another model's last result out of the detail view.
            mod_rounds = [
                r for mid in member_ids
                for r in self.db.get_account_rounds(mid, model=mod, limit=12)
            ]
            mod_rounds.sort(key=lambda r: (float(r["checked_at"]), int(r["id"])), reverse=True)
            mod_latest = _format_acc_round(mod_rounds[0]) if mod_rounds else None
            mod_history = [_format_acc_round(r) for r in mod_rounds[:12]]
            paused_until = self._get_model_paused_until(members, mod, now)
            per_model.append({
                "model": mod,
                "calibration_model": self.config.calibration_model_for(mod),
                "auto_actions_eligible": bool(self.config.enabled and self.config.auto_pause_enabled
                    and all(self.config.auto_actions_calibrated(int(m["account_id"]), mod)
                            for m in members if mod in json.loads(m["models_json"]))),
                "latest": mod_latest,
                "history": mod_history,
                "paused_until": paused_until,
            })

        latest = _format_acc_round(member_rounds[0]) if member_rounds else None
        history = [_format_acc_round(r) for r in member_rounds[:12]]

        # Each key keeps its own last result per model, including keys that
        # are switched off, so a cluster row can show every key.
        member_views = []
        for m in members:
            try:
                m_models = json.loads(m["models_json"])
            except Exception:
                m_models = []
            m_latest = {}
            for mod in m_models:
                rows = self.db.get_account_rounds(int(m["account_id"]), model=mod, limit=1)
                m_latest[mod] = _format_acc_round(rows[0]) if rows else None
            m_reason = m["inactive_reason"] if "inactive_reason" in m.keys() else None
            member_views.append({
                "account_id": int(m["account_id"]),
                "name": str(m["name"]),
                "participating": bool(m["schedulable"]),
                "inactive_reason": None if bool(m["schedulable"]) else (m_reason or "unschedulable"),
                "models": m_models,
                "latest_by_model": m_latest,
            })

        rep_id = target["rep_account_id"]
        rep_acc = self.db.get_account(rep_id)
        pending = self.db.get_pending_account_job(rep_id, cluster_id=target["cluster_id"])

        retest_progress = None

        last_real_str = utc_iso(float(rep_acc["last_real_request_at"])) if (rep_acc and rep_acc["last_real_request_at"] is not None) else None
        scheduled = bool(
            self.config.enabled and self.config.per_account_enabled
            and rep_acc and rep_acc["schedulable"]
            and str(target["mode"]) != "retired" and target_models
        )
        next_run = rep_acc["next_run_at"] if scheduled else None
        next_model = self._compute_target_next_model(target) if scheduled and next_run is not None else None
        if pending is not None:
            next_model = str(pending["model"])
            can_claim = self.config.per_account_enabled and (self.config.enabled or pending["trigger"] == "manual")
            next_run = pending["available_at"] if pending["state"] == "queued" and can_claim else None

        return {
            "snapshot_at": utc_iso(now, timespec="microseconds"),
            "account_id": account_id,
            "cluster_id": target["cluster_id"],
            "cluster_name": target["cluster_name"],
            "member_account_ids": member_ids,
            "name": str(target["name"]),
            "models": target_models,
            "participating": bool(target["participating"]),
            "inactive_reason": target["inactive_reason"],
            "members": member_views,
            # Admission advances rotation; pending work owns the visible model
            # until it finishes. Idle snapshots forecast the next scheduled job.
            "next_model": next_model,
            "per_model": per_model,
            "mode": str(target["mode"]),
            "interval_seconds": int(target["interval_seconds"]),
            "last_real_request_at": last_real_str,
            "next_run_at": utc_iso(next_run),
            **self._pending_job_state(pending),
            "schedule_mode": "scheduled" if scheduled else "manual",
            "latest": latest,
            "history": history,
            "retest_progress": retest_progress,
        }

    def all_accounts_snapshot(self, *, force_refresh: bool = False) -> dict[str, Any]:
        if force_refresh and self.config.per_account_enabled:
            self._refresh_accounts(self.clock())
        with self.db.read_snapshot():
            now = self.clock()
            targets = self._get_all_targets()
            return {
                "snapshot_at": utc_iso(now, timespec="microseconds"),
                "accounts": [
                    self._account_snapshot(target["rep_account_id"], now=now)
                    for target in targets
                    if str(target["mode"]) != "retired"
                ]
            }

    def accounts_summary_for_model(self, model: str) -> dict[str, Any]:
        self._refresh_accounts(self.clock(), if_due=True)
        with self.db.read_snapshot():
            return self._accounts_summary_for_model(model)

    def _accounts_summary_for_model(self, model: str) -> dict[str, Any]:
        targets = self._get_all_targets()
        latest_by_account = self.db.get_latest_rounds_for_all_accounts(model)
        total = 0
        match_count = 0
        suspect_count = 0
        uncertain_count = 0
        error_count = 0
        active_count = 0

        for target in targets:
            if str(target["mode"]) == "retired" or not target["participating"]:
                continue
            if model not in target["models"]:
                continue
            total += 1
            if str(target["mode"]) == "active":
                active_count += 1

            # Latest round for this model across the target's keys in the pool.
            mod_rounds = [latest_by_account[mid] for mid in self._pool_member_ids(target) if mid in latest_by_account]
            if mod_rounds:
                mod_rounds.sort(key=lambda r: (float(r["checked_at"]), int(r["id"])), reverse=True)
                latest_r = mod_rounds[0]
                st = str(latest_r["status"])
                if st == "match":
                    match_count += 1
                elif st == "suspect":
                    suspect_count += 1
                elif st == "uncertain":
                    uncertain_count += 1
                elif st in {"error", "budget_exhausted"}:
                    error_count += 1

        return {
            "model": model,
            "total": total,
            "match": match_count,
            "suspect": suspect_count,
            "uncertain": uncertain_count,
            "error": error_count,
            "active": active_count,
        }
