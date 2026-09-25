from __future__ import annotations

import fcntl
import json
import math
import re
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Iterable
from uuid import UUID


RETENTION_SECONDS = 90 * 24 * 60 * 60



@dataclass(frozen=True)
class AccountQueueItem:
    queue_id: int
    account_id: int
    model: str
    trigger: str
    requested_at: float
    available_at: float
    retest_index: int
    started_at: float | None = None
    error_code: str | None = None

@dataclass(frozen=True)
class QueueItem:
    queue_id: int
    monitor_id: int
    trigger: str
    requested_at: float
    available_at: float
    retest_index: int
    started_at: float | None = None
    reservation_id: int | None = None


@dataclass(frozen=True)
class Reservation:
    reservation_id: int
    queue_id: int
    monitor_id: int
    budget_day: str
    amount_usd: float
    budget_basis: str | None = None
    billing_prices: dict[str, float] | None = None
    reserve_prices: dict[str, float] | None = None
    probe_user_agents: tuple[str, ...] = ()


@dataclass(frozen=True)
class BudgetSnapshot:
    budget_day: str
    spent_usd: float
    reserved_usd: float
    paused_until: float | None


class ModelTraceDB:
    """Small, independent SQLite store; all writes are serialized in-process."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._worker_owner: BinaryIO | None = None
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._create_schema()


    # ----------------- Account scheduling & persistence -----------------

    def get_all_accounts(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM accounts ORDER BY account_id ASC"
            ).fetchall()

    def get_account(self, account_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM accounts WHERE account_id = ?", (account_id,)
            ).fetchone()

    def upsert_account_sync(
        self,
        *,
        account_id: int,
        name: str,
        platform: str | None,
        type_: str | None,
        schedulable: bool,
        models: list[str],
        last_real_request_at: float | None,
        last_real_model: str | None,
        real_requests_10m: int,
        mode: str,
        interval_seconds: int,
        next_run_at: float | None,
        cluster_id: str | None = None,
        cluster_name: str | None = None,
        real_models: list[str] | None = None,
        paused_models: list[dict[str, Any]] | None = None,
        now: float,
    ) -> None:
        models_json = json.dumps(models, ensure_ascii=False, separators=(",", ":"))
        real_models_json = json.dumps(real_models or [], ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            existing = self._conn.execute(
                "SELECT account_id, paused_models_json FROM accounts WHERE account_id = ?", (account_id,)
            ).fetchone()
            if existing is None:
                pms_json = json.dumps(paused_models or [], ensure_ascii=False, separators=(",", ":"))
                self._conn.execute(
                    """
                    INSERT INTO accounts (
                        account_id, name, platform, type, schedulable, models_json,
                        last_real_request_at, last_real_model, real_requests_10m,
                        mode, interval_seconds, next_run_at, last_model_index,
                        cluster_id, cluster_name, real_models_json, paused_models_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_id, name, platform, type_, 1 if schedulable else 0,
                        models_json, last_real_request_at, last_real_model,
                        real_requests_10m, mode, interval_seconds, next_run_at,
                        cluster_id, cluster_name, real_models_json, pms_json, now,
                    ),
                )
            else:
                if paused_models is not None:
                    pms_json = json.dumps(paused_models, ensure_ascii=False, separators=(",", ":"))
                else:
                    pms_json = existing["paused_models_json"] if "paused_models_json" in existing.keys() and existing["paused_models_json"] is not None else "[]"
                self._conn.execute(
                    """
                    UPDATE accounts SET
                        name = ?, platform = ?, type = ?, schedulable = ?,
                        models_json = ?, last_real_request_at = ?, last_real_model = ?,
                        real_requests_10m = ?, mode = ?, interval_seconds = ?,
                        cluster_id = ?, cluster_name = ?, real_models_json = ?,
                        paused_models_json = ?,
                        updated_at = ?
                    WHERE account_id = ?
                    """,
                    (
                        name, platform, type_, 1 if schedulable else 0,
                        models_json, last_real_request_at, last_real_model,
                        real_requests_10m, mode, interval_seconds,
                        cluster_id, cluster_name, real_models_json,
                        pms_json,
                        now, account_id,
                    ),
                )

    def retire_account(self, account_id: int, *, now: float) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE accounts
                SET schedulable = 0, mode = 'retired', next_run_at = NULL, updated_at = ?
                WHERE account_id = ?
                """,
                (now, account_id),
            )
            self._conn.execute(
                """
                DELETE FROM account_queue
                WHERE account_id = ? AND state = 'queued'
                """,
                (account_id,),
            )

    def set_account_next_run(self, account_id: int, next_run_at: float | None, *, now: float) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE accounts SET next_run_at = ?, updated_at = ? WHERE account_id = ?",
                (next_run_at, now, account_id),
            )

    def set_account_last_model_index(self, account_id: int, index: int, *, now: float) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE accounts SET last_model_index = ?, updated_at = ? WHERE account_id = ?",
                (index, now, account_id),
            )

    def has_pending_account(self, account_id: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM account_queue WHERE account_id = ? AND state IN ('queued', 'running')",
                (account_id,),
            ).fetchone()
            return row is not None

    def is_account_running(self, account_id: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM account_queue WHERE account_id = ? AND state = 'running'",
                (account_id,),
            ).fetchone()
            return row is not None

    def enqueue_account(
        self,
        account_id: int,
        model: str,
        *,
        now: float,
        available_at: float | None = None,
        trigger: str = "scheduled",
        retest_index: int = 0,
    ) -> tuple[bool, int | None]:
        available_at = now if available_at is None else available_at
        with self._lock:
            try:
                cursor = self._conn.execute(
                    """
                    INSERT INTO account_queue (
                        account_id, model, state, trigger, requested_at, available_at, retest_index
                    ) VALUES (?, ?, 'queued', ?, ?, ?, ?)
                    """,
                    (account_id, model, trigger, now, available_at, retest_index),
                )
                return True, int(cursor.lastrowid)
            except sqlite3.IntegrityError:
                existing = self._conn.execute(
                    """
                    SELECT id FROM account_queue
                    WHERE account_id = ? AND state IN ('queued', 'running')
                    ORDER BY id LIMIT 1
                    """,
                    (account_id,),
                ).fetchone()
                return False, int(existing["id"]) if existing else None

    def claim_next_account(self, *, now: float) -> AccountQueueItem | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                running = self._conn.execute(
                    "SELECT 1 FROM account_queue WHERE state = 'running' LIMIT 1"
                ).fetchone()
                if running is not None:
                    self._conn.execute("COMMIT")
                    return None
                row = self._conn.execute(
                    """
                    SELECT * FROM account_queue
                    WHERE state = 'queued' AND available_at <= ?
                    ORDER BY available_at ASC, id ASC
                    LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                if row is None:
                    self._conn.execute("COMMIT")
                    return None
                self._conn.execute(
                    "UPDATE account_queue SET state = 'running', started_at = ? WHERE id = ?",
                    (now, row["id"]),
                )
                self._conn.execute("COMMIT")
                return AccountQueueItem(
                    queue_id=int(row["id"]),
                    account_id=int(row["account_id"]),
                    model=str(row["model"]),
                    trigger=str(row["trigger"]),
                    requested_at=float(row["requested_at"]),
                    available_at=float(row["available_at"]),
                    retest_index=int(row["retest_index"]),
                    started_at=now,
                    error_code=row["error_code"],
                )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def insert_account_round(
        self,
        *,
        account_id: int,
        model: str,
        status: str,
        message_code: str,
        checked_at: float,
        diagnostics: dict[str, Any] | None = None,
    ) -> int:
        """Record a round that did not come from a queued probe (e.g. a manual reset)."""
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT INTO account_rounds (
                    account_id, model, status, target_probability, best_model,
                    checked_at, message_code, reasoning_effort, ranking_json, diagnostics_json
                ) VALUES (?, ?, ?, NULL, NULL, ?, ?, NULL, '[]', ?)
                """,
                (account_id, model, status, checked_at, message_code,
                 json.dumps(diagnostics or {}, ensure_ascii=False, separators=(",", ":"))),
            )
            self._conn.commit()
            return int(cursor.lastrowid)

    def record_account_round_and_finish(
        self,
        job: AccountQueueItem,
        *,
        status: str,
        target_probability: float | None,
        best_model: str | None,
        checked_at: float,
        message_code: str,
        reasoning_effort: str | None,
        ranking: list[dict[str, Any]],
        diagnostics: dict[str, Any],
        next_run_at: float | None,
        schedule_account_id: int | None = None,
    ) -> int:
        """Record a round under the key actually used (``job.account_id``) and move
        the schedule of ``schedule_account_id`` (the target's representative;
        defaults to the same account)."""
        ranking_json = json.dumps(ranking, ensure_ascii=False, separators=(",", ":"))
        diagnostics_json = json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute("SELECT state FROM account_queue WHERE id = ?", (job.queue_id,)).fetchone()
                if row is None or row["state"] != "running":
                    self._conn.execute("COMMIT")
                    return 0
                cursor = self._conn.execute(
                    """
                    INSERT INTO account_rounds (
                        account_id, model, status, target_probability, best_model,
                        checked_at, message_code, reasoning_effort, ranking_json, diagnostics_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job.account_id,
                        job.model,
                        status,
                        target_probability,
                        best_model,
                        checked_at,
                        message_code,
                        reasoning_effort,
                        ranking_json,
                        diagnostics_json,
                    ),
                )
                round_id = int(cursor.lastrowid)
                self._conn.execute(
                    """
                    UPDATE account_queue
                    SET state = ?, finished_at = ?, error_code = ?
                    WHERE id = ?
                    """,
                    ("done" if status != "error" else "error", checked_at, message_code, job.queue_id),
                )
                if next_run_at is not None:
                    self._conn.execute(
                        "UPDATE accounts SET next_run_at = ?, updated_at = ? WHERE account_id = ?",
                        (next_run_at, checked_at, schedule_account_id if schedule_account_id is not None else job.account_id),
                    )
                self._conn.execute("COMMIT")
                return round_id
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def get_account_latest_round(self, account_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                """
                SELECT * FROM account_rounds WHERE account_id = ?
                ORDER BY checked_at DESC, id DESC LIMIT 1
                """,
                (account_id,),
            ).fetchone()

    def get_account_rounds(self, account_id: int, *, limit: int = 12) -> list[sqlite3.Row]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM (
                    SELECT * FROM account_rounds WHERE account_id = ?
                    ORDER BY checked_at DESC, id DESC LIMIT ?
                ) ORDER BY checked_at DESC, id DESC
                """,
                (account_id, limit),
            ).fetchall()
            return rows

    def get_latest_rounds_for_all_accounts(self, model: str | None = None) -> dict[int, sqlite3.Row]:
        """Latest round per account; with ``model``, the latest round for that model."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT ar.* FROM account_rounds ar
                INNER JOIN (
                    SELECT account_id, MAX(id) as max_id
                    FROM account_rounds
                    WHERE ? IS NULL OR model = ?
                    GROUP BY account_id
                ) latest ON ar.id = latest.max_id
                """,
                (model, model),
            ).fetchall()
            return {int(row["account_id"]): row for row in rows}

    def recover_running_account_jobs(self, *, now: float) -> list[tuple[int, str]]:
        with self._lock:
            self._conn.execute(
                """
                UPDATE account_queue
                SET state = 'error', finished_at = ?, error_code = 'auto_retest_disabled'
                WHERE state = 'queued' AND (trigger = 'auto_retest' OR retest_index > 0)
                """,
                (now,),
            )
            rows = self._conn.execute(
                "SELECT * FROM account_queue WHERE state = 'running' ORDER BY id"
            ).fetchall()
        recovered_jobs = []
        for row in rows:
            job = AccountQueueItem(
                queue_id=int(row["id"]),
                account_id=int(row["account_id"]),
                model=str(row["model"]),
                trigger=str(row["trigger"]),
                requested_at=float(row["requested_at"]),
                available_at=float(row["available_at"]),
                retest_index=int(row["retest_index"]),
                started_at=float(row["started_at"]) if row["started_at"] is not None else None,
                error_code=row["error_code"],
            )
            self.record_account_round_and_finish(
                job,
                status="interrupted",
                target_probability=None,
                best_model=None,
                checked_at=now,
                message_code="worker_restarted",
                reasoning_effort=None,
                ranking=[],
                diagnostics={"recovered_orphaned_job": True},
                next_run_at=None,
            )
            recovered_jobs.append((job.account_id, job.model))
        return recovered_jobs

    def acquire_worker_ownership(self) -> None:
        # Recovery is only safe after the old owner is gone. A second service
        # must not turn an active paid request into an orphan and replay work.
        with self._lock:
            if self._worker_owner is not None:
                raise RuntimeError("worker_already_active")
            owner = open(str(Path(self.path).resolve()) + ".worker.lock", "a+b")
            try:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                owner.close()
                raise RuntimeError("worker_already_active") from None
            self._worker_owner = owner

    def close(self) -> None:
        with self._lock:
            self._conn.close()
            if self._worker_owner is not None:
                self._worker_owner.close()
                self._worker_owner = None

    def _create_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS scheduler_state (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                anchor_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS accounts (
                account_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                platform TEXT,
                type TEXT,
                schedulable INTEGER NOT NULL DEFAULT 1,
                models_json TEXT NOT NULL DEFAULT '[]',
                last_real_request_at REAL,
                last_real_model TEXT,
                real_requests_10m INTEGER NOT NULL DEFAULT 0,
                mode TEXT NOT NULL DEFAULT 'idle',
                interval_seconds INTEGER NOT NULL DEFAULT 3600,
                next_run_at REAL,
                last_model_index INTEGER NOT NULL DEFAULT 0,
                cluster_id TEXT,
                cluster_name TEXT,
                real_models_json TEXT NOT NULL DEFAULT '[]',
                paused_models_json TEXT NOT NULL DEFAULT '[]',
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS account_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                model TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('queued', 'running', 'done', 'error')),
                trigger TEXT NOT NULL,
                requested_at REAL NOT NULL,
                available_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                retest_index INTEGER NOT NULL DEFAULT 0,
                error_code TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS account_queue_one_pending
                ON account_queue(account_id) WHERE state IN ('queued', 'running');
            CREATE INDEX IF NOT EXISTS account_queue_ready_idx
                ON account_queue(state, available_at, id);

            CREATE TABLE IF NOT EXISTS account_rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                model TEXT NOT NULL,
                status TEXT NOT NULL,
                target_probability REAL,
                best_model TEXT,
                checked_at REAL NOT NULL,
                message_code TEXT NOT NULL,
                reasoning_effort TEXT,
                ranking_json TEXT NOT NULL DEFAULT '[]',
                diagnostics_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS account_rounds_acct_checked_idx
                ON account_rounds(account_id, checked_at DESC, id DESC);

            CREATE TABLE IF NOT EXISTS monitor_state (
                monitor_id INTEGER PRIMARY KEY,
                last_checked_at REAL,
                next_run_at REAL,
                last_error_code TEXT,
                last_upstream_statuses_json TEXT NOT NULL DEFAULT '[]',
                last_receipt_count INTEGER NOT NULL DEFAULT 0,
                last_receipt_consistent INTEGER,
                last_actual_cost_usd REAL,
                last_reserved_usd REAL,
                next_run_after REAL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                monitor_id INTEGER NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('queued', 'running', 'done', 'error')),
                trigger TEXT NOT NULL,
                requested_at REAL NOT NULL,
                available_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                retest_index INTEGER NOT NULL DEFAULT 0,
                reservation_id INTEGER,
                error_code TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS queue_one_pending_monitor
                ON queue(monitor_id) WHERE state IN ('queued', 'running');
            CREATE INDEX IF NOT EXISTS queue_ready_idx
                ON queue(state, available_at, id);

            CREATE TABLE IF NOT EXISTS detection_attempts (
                queue_id INTEGER NOT NULL REFERENCES queue(id) ON DELETE CASCADE,
                ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 2),
                user_agent TEXT NOT NULL UNIQUE,
                started_at REAL NOT NULL,
                PRIMARY KEY(queue_id, ordinal)
            );

            CREATE TABLE IF NOT EXISTS rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                monitor_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                target_probability REAL,
                best_model TEXT,
                checked_at REAL NOT NULL,
                message_code TEXT NOT NULL,
                ranking_json TEXT NOT NULL DEFAULT '[]',
                diagnostics_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS rounds_monitor_checked_idx
                ON rounds(monitor_id, checked_at, id);

            CREATE TABLE IF NOT EXISTS budget_days (
                budget_day TEXT PRIMARY KEY,
                spent_usd REAL NOT NULL DEFAULT 0,
                reserved_usd REAL NOT NULL DEFAULT 0,
                paused_until REAL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS budget_reservations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id INTEGER NOT NULL,
                monitor_id INTEGER NOT NULL,
                budget_day TEXT NOT NULL,
                amount_usd REAL NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('reserved', 'settled')),
                created_at REAL NOT NULL,
                settled_amount_usd REAL,
                settled_at REAL,
                budget_basis TEXT,
                settlement_basis TEXT,
                billing_prices_json TEXT,
                reserve_prices_json TEXT
            );
            CREATE INDEX IF NOT EXISTS budget_reservations_day_idx
                ON budget_reservations(budget_day, state, created_at);

            -- Observed lower bounds are not the configured reservation estimate.
            CREATE TABLE IF NOT EXISTS reservation_cost_floors (
                reservation_id INTEGER PRIMARY KEY REFERENCES budget_reservations(id) ON DELETE CASCADE,
                known_cost_floor_usd REAL NOT NULL CHECK(known_cost_floor_usd >= 0)
            );
            CREATE TABLE IF NOT EXISTS reconciliation_rounds (
                reservation_id INTEGER PRIMARY KEY REFERENCES budget_reservations(id) ON DELETE CASCADE,
                expected_model TEXT NOT NULL,
                expected_count INTEGER NOT NULL DEFAULT 3 CHECK(expected_count = 3),
                created_at REAL NOT NULL,
                grace_seconds REAL NOT NULL CHECK(grace_seconds >= 0),
                next_check_at REAL NOT NULL,
                checks INTEGER NOT NULL DEFAULT 0,
                last_checked_at REAL,
                state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'settled')),
                error_code TEXT
            );
            CREATE INDEX IF NOT EXISTS reconciliation_due_idx
                ON reconciliation_rounds(state, next_check_at, reservation_id);
            CREATE TABLE IF NOT EXISTS reconciliation_probes (
                reservation_id INTEGER NOT NULL REFERENCES reconciliation_rounds(reservation_id) ON DELETE CASCADE,
                ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 2),
                user_agent TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'planned' CHECK(status IN ('planned', 'attempted', 'cancelled')),
                started_at REAL,
                PRIMARY KEY(reservation_id, ordinal)
            );

            -- A historical unknown hold can be released only by an explicit
            -- admin action.  Keep it separate from budget_reservations so the
            -- original row remains visibly unresolved/unknown rather than
            -- being misreported as a confirmed settlement.
            CREATE TABLE IF NOT EXISTS reservation_conservative_resolutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reservation_id INTEGER NOT NULL UNIQUE
                    REFERENCES budget_reservations(id) ON DELETE RESTRICT,
                budget_day TEXT NOT NULL,
                reservation_amount_usd REAL NOT NULL CHECK(reservation_amount_usd >= 0),
                observed_floor_usd REAL NOT NULL CHECK(observed_floor_usd >= 0),
                conservative_amount_usd REAL NOT NULL CHECK(conservative_amount_usd > 0),
                basis TEXT NOT NULL CHECK(basis = 'conservative_estimate'),
                actor TEXT NOT NULL,
                reason TEXT NOT NULL,
                evidence_json TEXT NOT NULL DEFAULT '{}',
                resolved_at REAL NOT NULL,
                actual_cost_usd REAL CHECK(actual_cost_usd IS NULL),
                actual_known INTEGER NOT NULL DEFAULT 0 CHECK(actual_known = 0)
            );
            CREATE INDEX IF NOT EXISTS reservation_conservative_resolutions_day_idx
                ON reservation_conservative_resolutions(budget_day, resolved_at, reservation_id);
            CREATE TRIGGER IF NOT EXISTS reservation_conservative_resolutions_no_update
                BEFORE UPDATE ON reservation_conservative_resolutions
                BEGIN SELECT RAISE(ABORT, 'conservative_resolution_immutable'); END;
            CREATE TRIGGER IF NOT EXISTS reservation_conservative_resolutions_no_delete
                BEFORE DELETE ON reservation_conservative_resolutions
                BEGIN SELECT RAISE(ABORT, 'conservative_resolution_immutable'); END;

            -- A verified receipt may arrive after an estimate was posted.  It
            -- is a separate append-only adjustment, never an edit of the
            -- original estimate audit.
            CREATE TABLE IF NOT EXISTS reservation_conservative_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resolution_id INTEGER NOT NULL UNIQUE
                    REFERENCES reservation_conservative_resolutions(id) ON DELETE RESTRICT,
                reservation_id INTEGER NOT NULL UNIQUE
                    REFERENCES budget_reservations(id) ON DELETE RESTRICT,
                budget_day TEXT NOT NULL,
                estimated_amount_usd REAL NOT NULL CHECK(estimated_amount_usd > 0),
                actual_cost_usd REAL NOT NULL CHECK(actual_cost_usd >= 0),
                adjustment_usd REAL NOT NULL,
                basis TEXT NOT NULL CHECK(basis = 'verified_actual'),
                actor TEXT NOT NULL,
                reason TEXT NOT NULL,
                evidence_json TEXT NOT NULL DEFAULT '{}',
                adjusted_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS reservation_conservative_adjustments_day_idx
                ON reservation_conservative_adjustments(budget_day, adjusted_at, reservation_id);
            CREATE TRIGGER IF NOT EXISTS reservation_conservative_adjustments_no_update
                BEFORE UPDATE ON reservation_conservative_adjustments
                BEGIN SELECT RAISE(ABORT, 'conservative_adjustment_immutable'); END;
            CREATE TRIGGER IF NOT EXISTS reservation_conservative_adjustments_no_delete
                BEFORE DELETE ON reservation_conservative_adjustments
                BEGIN SELECT RAISE(ABORT, 'conservative_adjustment_immutable'); END;
            """
        )
        # v0.1.3 databases already contain budget_reservations.  Keep those
        # rows byte-for-byte/accounting-wise untouched and add only nullable
        # metadata for newly created reservations.
        acct_columns = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(accounts)").fetchall()
        }
        for name, definition in (
            ("cluster_id", "TEXT"),
            ("cluster_name", "TEXT"),
            ("real_models_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("paused_models_json", "TEXT NOT NULL DEFAULT '[]'"),
        ):
            if name not in acct_columns:
                self._conn.execute(f"ALTER TABLE accounts ADD COLUMN {name} {definition}")

        columns = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(budget_reservations)").fetchall()
        }
        for name, definition in (
            ("budget_basis", "TEXT"),
            ("settlement_basis", "TEXT"),
            ("billing_prices_json", "TEXT"),
            ("reserve_prices_json", "TEXT"),
        ):
            if name not in columns:
                self._conn.execute(f"ALTER TABLE budget_reservations ADD COLUMN {name} {definition}")

    def _transaction(self):
        return self._conn

    @staticmethod
    def budget_day(now: float) -> str:
        return datetime.fromtimestamp(now, tz=timezone.utc).date().isoformat()

    @staticmethod
    def next_utc_midnight(now: float) -> float:
        current = datetime.fromtimestamp(now, tz=timezone.utc)
        next_day = current.date().fromordinal(current.date().toordinal() + 1)
        midnight = datetime.combine(next_day, datetime.min.time(), tzinfo=timezone.utc)
        return midnight.timestamp()

    def seed_monitors(self, monitor_ids: Iterable[int], *, now: float) -> None:
        with self._lock:
            for monitor_id in monitor_ids:
                self._conn.execute(
                    """
                    INSERT INTO monitor_state(monitor_id, updated_at)
                    VALUES (?, ?)
                    ON CONFLICT(monitor_id) DO NOTHING
                    """,
                    (monitor_id, now),
                )

    def schedule_anchor(self, *, now: float) -> float:
        with self._lock:
            self._conn.execute(
                "INSERT INTO scheduler_state(id, anchor_at) VALUES (1, ?) ON CONFLICT(id) DO NOTHING",
                (now,),
            )
            return float(self._conn.execute("SELECT anchor_at FROM scheduler_state WHERE id = 1").fetchone()[0])

    def set_initial_next_run(self, monitor_id: int, next_run_at: float, *, now: float) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE monitor_state
                SET next_run_at = COALESCE(next_run_at, ?), updated_at = ?
                WHERE monitor_id = ?
                """,
                (next_run_at, now, monitor_id),
            )

    def set_next_run(self, monitor_id: int, next_run_at: float | None, *, now: float) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE monitor_state SET next_run_at = ?, updated_at = ? WHERE monitor_id = ?",
                (next_run_at, now, monitor_id),
            )

    def get_state(self, monitor_id: int) -> sqlite3.Row:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM monitor_state WHERE monitor_id = ?", (monitor_id,)
            ).fetchone()
        if row is None:
            raise KeyError(monitor_id)
        return row

    def get_all_states(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute("SELECT * FROM monitor_state ORDER BY monitor_id").fetchall()

    def is_running(self, monitor_id: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM queue WHERE monitor_id = ? AND state = 'running' LIMIT 1",
                (monitor_id,),
            ).fetchone()
        return row is not None

    def has_pending(self, monitor_id: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM queue WHERE monitor_id = ? AND state IN ('queued', 'running') LIMIT 1",
                (monitor_id,),
            ).fetchone()
        return row is not None

    def last_request_at(self, monitor_id: int) -> float | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT requested_at FROM queue WHERE monitor_id = ? ORDER BY requested_at DESC, id DESC LIMIT 1",
                (monitor_id,),
            ).fetchone()
        return float(row["requested_at"]) if row is not None else None

    def enqueue(
        self,
        monitor_id: int,
        *,
        now: float,
        available_at: float | None = None,
        trigger: str = "manual",
        retest_index: int = 0,
    ) -> tuple[bool, int | None]:
        available_at = now if available_at is None else available_at
        with self._lock:
            try:
                cursor = self._conn.execute(
                    """
                    INSERT INTO queue(
                        monitor_id, state, trigger, requested_at, available_at, retest_index
                    ) VALUES (?, 'queued', ?, ?, ?, ?)
                    """,
                    (monitor_id, trigger, now, available_at, retest_index),
                )
            except sqlite3.IntegrityError:
                existing = self._conn.execute(
                    """
                    SELECT id FROM queue
                    WHERE monitor_id = ? AND state IN ('queued', 'running')
                    ORDER BY id LIMIT 1
                    """,
                    (monitor_id,),
                ).fetchone()
                return False, int(existing["id"]) if existing else None
        return True, int(cursor.lastrowid)

    def enqueue_due_monitors(self, monitor_ids: Iterable[int], *, now: float) -> list[int]:
        created: list[int] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for monitor_id in monitor_ids:
                    row = self._conn.execute(
                        "SELECT next_run_at FROM monitor_state WHERE monitor_id = ?", (monitor_id,)
                    ).fetchone()
                    if row is None or row["next_run_at"] is None or float(row["next_run_at"]) > now:
                        continue
                    try:
                        cursor = self._conn.execute(
                            """
                            INSERT INTO queue(
                                monitor_id, state, trigger, requested_at, available_at, retest_index
                            ) VALUES (?, 'queued', 'scheduled', ?, ?, 0)
                            """,
                            (monitor_id, now, now),
                        )
                    except sqlite3.IntegrityError:
                        continue
                    self._conn.execute(
                        "UPDATE monitor_state SET next_run_at = NULL, updated_at = ? WHERE monitor_id = ?",
                        (now, monitor_id),
                    )
                    created.append(int(cursor.lastrowid))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return created

    def claim_next(self, *, now: float) -> QueueItem | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # Serialize paid rounds across connections, not just one worker thread.
                if self._conn.execute("SELECT 1 FROM queue WHERE state = 'running' LIMIT 1").fetchone():
                    self._conn.execute("COMMIT")
                    return None
                row = self._conn.execute(
                    """
                    SELECT * FROM queue
                    WHERE state = 'queued' AND available_at <= ?
                    ORDER BY available_at, id
                    LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                if row is None:
                    self._conn.execute("COMMIT")
                    return None
                self._conn.execute(
                    """
                    UPDATE queue SET state = 'running', started_at = ? WHERE id = ?
                    """,
                    (now, row["id"]),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return QueueItem(
            queue_id=int(row["id"]),
            monitor_id=int(row["monitor_id"]),
            trigger=str(row["trigger"]),
            requested_at=float(row["requested_at"]),
            available_at=float(row["available_at"]),
            retest_index=int(row["retest_index"]),
            started_at=now,
            reservation_id=int(row["reservation_id"]) if row["reservation_id"] is not None else None,
        )

    def claim_probe_attempt(self, job: QueueItem, ordinal: int, user_agent: str, *, now: float) -> bool:
        if ordinal not in (0, 1, 2):
            raise ValueError("invalid_probe_ordinal")
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO detection_attempts(queue_id,ordinal,user_agent,started_at) "
                "SELECT id,?,?,? FROM queue WHERE id=? AND monitor_id=? AND state='running'",
                (ordinal, user_agent, now, job.queue_id, job.monitor_id),
            )
            return cursor.rowcount == 1

    def attach_reservation(self, queue_id: int, reservation_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE queue SET reservation_id = ? WHERE id = ?", (reservation_id, queue_id)
            )

    def _ensure_budget_day_locked(self, budget_day: str, *, now: float) -> sqlite3.Row:
        self._conn.execute(
            """
            INSERT INTO budget_days(budget_day, updated_at)
            VALUES (?, ?)
            ON CONFLICT(budget_day) DO NOTHING
            """,
            (budget_day, now),
        )
        row = self._conn.execute(
            "SELECT * FROM budget_days WHERE budget_day = ?", (budget_day,)
        ).fetchone()
        assert row is not None
        if row["paused_until"] is not None and float(row["paused_until"]) <= now:
            self._conn.execute(
                "UPDATE budget_days SET paused_until = NULL, updated_at = ? WHERE budget_day = ?",
                (now, budget_day),
            )
            row = self._conn.execute(
                "SELECT * FROM budget_days WHERE budget_day = ?", (budget_day,)
            ).fetchone()
            assert row is not None
        return row

    def _carried_reservations_locked(self, budget_day: str) -> float:
        # No date boundary (or retention cleanup) is evidence that an in-flight
        # or unconfirmed upstream charge disappeared. Hold it on every new day.
        return float(self._conn.execute(
            "SELECT COALESCE(SUM(b.amount_usd), 0) "
            "FROM budget_reservations b "
            "WHERE b.state = 'reserved' AND b.budget_day != ? "
            "AND NOT EXISTS (SELECT 1 FROM reservation_conservative_resolutions cr "
            "WHERE cr.reservation_id = b.id)",
            (budget_day,),
        ).fetchone()[0])

    def budget_snapshot(self, *, now: float) -> BudgetSnapshot:
        budget_day = self.budget_day(now)
        with self._lock:
            row = self._ensure_budget_day_locked(budget_day, now=now)
            carried = self._carried_reservations_locked(budget_day)
        return BudgetSnapshot(
            budget_day=budget_day,
            spent_usd=float(row["spent_usd"]),
            reserved_usd=float(row["reserved_usd"]) + carried,
            paused_until=float(row["paused_until"]) if row["paused_until"] is not None else None,
        )

    def pause_budget(self, *, now: float, until: float | None = None) -> float:
        budget_day = self.budget_day(now)
        reset = self.next_utc_midnight(now) if until is None else until
        with self._lock:
            self._ensure_budget_day_locked(budget_day, now=now)
            self._conn.execute(
                "UPDATE budget_days SET paused_until = ?, updated_at = ? WHERE budget_day = ?",
                (reset, now, budget_day),
            )
        return reset

    def reserve_budget(
        self,
        monitor_id: int,
        queue_id: int,
        amount_usd: float,
        *,
        daily_budget_usd: float,
        now: float,
        budget_basis: str | None = None,
        billing_prices: dict[str, Any] | None = None,
        reserve_prices: dict[str, Any] | None = None,
        probe_model: str | None = None,
        probe_user_agents: Iterable[str] | None = None,
    ) -> Reservation | None:
        agents = tuple(probe_user_agents or ())
        if probe_model is not None or agents:
            if not isinstance(probe_model, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}", probe_model):
                raise ValueError("invalid_expected_model")
            if len(agents) != 3 or len(set(agents)) != 3:
                raise ValueError("invalid_probe_identities")
            for agent in agents:
                if not isinstance(agent, str) or not agent.startswith("ModelTraceProbe/") or str(UUID(agent.split("/",1)[1])) != agent.split("/",1)[1]:
                    raise ValueError("invalid_probe_identity")
        amount_usd = float(amount_usd)
        if not math.isfinite(amount_usd) or amount_usd < 0:
            raise ValueError("invalid_reservation_amount")
        if not math.isfinite(daily_budget_usd) or daily_budget_usd < 0:
            raise ValueError("invalid_daily_budget")
        if budget_basis is not None and budget_basis != "lei_group4_v1":
            raise ValueError("unsupported_budget_basis")
        if budget_basis is None and (billing_prices is not None or reserve_prices is not None):
            raise ValueError("pricing_snapshot_without_basis")
        billing_prices_json = self._serialize_price_snapshot(billing_prices, "invalid_billing_prices")
        reserve_prices_json = self._serialize_price_snapshot(reserve_prices, "invalid_reserve_prices")
        if budget_basis == "lei_group4_v1":
            if billing_prices_json is None:
                raise ValueError("missing_billing_prices")
            if reserve_prices_json is None:
                raise ValueError("missing_reserve_prices")
        budget_day = self.budget_day(now)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                job = self._conn.execute(
                    "SELECT monitor_id, state, reservation_id FROM queue WHERE id = ?", (queue_id,)
                ).fetchone()
                if job is None or job["monitor_id"] != monitor_id or job["state"] != "running" or job["reservation_id"] is not None:
                    raise ValueError("job_not_reservable")
                row = self._ensure_budget_day_locked(budget_day, now=now)
                spent = float(row["spent_usd"])
                reserved = float(row["reserved_usd"]) + self._carried_reservations_locked(budget_day)
                if (row["paused_until"] is not None and float(row["paused_until"]) > now) or spent + reserved + amount_usd > daily_budget_usd:
                    reset = self.next_utc_midnight(now)
                    self._conn.execute(
                        "UPDATE budget_days SET paused_until = ?, updated_at = ? WHERE budget_day = ?",
                        (reset, now, budget_day),
                    )
                    self._conn.execute("COMMIT")
                    return None
                cursor = self._conn.execute(
                    """
                    INSERT INTO budget_reservations(
                        queue_id, monitor_id, budget_day, amount_usd, state, created_at,
                        budget_basis, billing_prices_json, reserve_prices_json
                    ) VALUES (?, ?, ?, ?, 'reserved', ?, ?, ?, ?)
                    """,
                    (
                        queue_id,
                        monitor_id,
                        budget_day,
                        amount_usd,
                        now,
                        budget_basis,
                        billing_prices_json,
                        reserve_prices_json,
                    ),
                )
                reservation_id = int(cursor.lastrowid)
                self._conn.execute(
                    """
                    UPDATE budget_days
                    SET reserved_usd = reserved_usd + ?, updated_at = ?
                    WHERE budget_day = ?
                    """,
                    (amount_usd, now, budget_day),
                )
                self._conn.execute(
                    "UPDATE queue SET reservation_id = ? WHERE id = ?",
                    (reservation_id, queue_id),
                )
                if agents:
                    self._conn.execute(
                        "INSERT INTO reconciliation_rounds(reservation_id, expected_model, created_at, grace_seconds, next_check_at) VALUES (?, ?, ?, 300, ?)",
                        (reservation_id, probe_model, now, now + 300),
                    )
                    self._conn.executemany(
                        "INSERT INTO reconciliation_probes(reservation_id, ordinal, user_agent) VALUES (?, ?, ?)",
                        [(reservation_id, ordinal, agent) for ordinal, agent in enumerate(agents)],
                    )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return Reservation(
            reservation_id,
            queue_id,
            monitor_id,
            budget_day,
            amount_usd,
            budget_basis,
            self._parse_price_snapshot(billing_prices_json),
            self._parse_price_snapshot(reserve_prices_json),
            agents,
        )

    @staticmethod
    def _serialize_price_snapshot(
        values: dict[str, Any] | None, error: str
    ) -> str | None:
        if values is None:
            return None
        if not isinstance(values, dict):
            raise ValueError(error)
        required = {
            "input_per_million_usd",
            "cache_read_per_million_usd",
            "output_per_million_usd",
        }
        if set(values) != required:
            raise ValueError(error)
        normalized: dict[str, float] = {}
        for key in sorted(required):
            value = values[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(error)
            value = float(value)
            if not math.isfinite(value) or value < 0:
                raise ValueError(error)
            normalized[key] = value
        return json.dumps(normalized, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _parse_price_snapshot(value: str | None) -> dict[str, float] | None:
        if value is None:
            return None
        try:
            decoded = json.loads(value)
        except (KeyError, IndexError, TypeError, json.JSONDecodeError):
            return None
        if not isinstance(decoded, dict):
            return None
        try:
            result = {
                key: float(decoded[key])
                for key in (
                    "input_per_million_usd",
                    "cache_read_per_million_usd",
                    "output_per_million_usd",
                )
            }
        except (KeyError, TypeError, ValueError):
            return None
        if any(not math.isfinite(value) or value < 0 for value in result.values()):
            return None
        return result

    def settle_reservation(
        self,
        reservation_id: int | None,
        *,
        actual_cost_usd: float | None,
        now: float,
        minimum_cost_usd: float = 0.0,
    ) -> float | None:
        if reservation_id is None:
            return None
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM budget_reservations WHERE id = ?", (reservation_id,)
                ).fetchone()
                if row is None:
                    self._conn.execute("COMMIT")
                    return None
                if row["state"] != "reserved":
                    if row["settlement_basis"] == "conservative_estimate":
                        raise ValueError("conservative_resolution_requires_explicit_true_up")
                    self._conn.execute("COMMIT")
                    return None
                if self._conservative_resolution_locked(reservation_id) is not None:
                    raise ValueError("conservative_resolution_requires_verified_actual")
                reserved_amount = float(row["amount_usd"])
                # Unknown/invalid costs stay outstanding, including after UTC
                # rollover. Partial or duplicate receipts may prove a larger
                # charge than the configured estimate; never discard that cost.
                known_cost = (
                    isinstance(actual_cost_usd, (int, float))
                    and not isinstance(actual_cost_usd, bool)
                    and math.isfinite(actual_cost_usd) and actual_cost_usd >= 0
                )
                if not known_cost:
                    if not math.isfinite(minimum_cost_usd) or minimum_cost_usd < 0:
                        raise ValueError("invalid_cost_floor")
                    held_amount = max(reserved_amount, minimum_cost_usd)
                    self._conn.execute(
                        "UPDATE budget_reservations SET amount_usd = ? WHERE id = ?",
                        (held_amount, reservation_id),
                    )
                    self._conn.execute(
                        "UPDATE budget_days SET reserved_usd = reserved_usd + ?, updated_at = ? WHERE budget_day = ?",
                        (held_amount - reserved_amount, now, row["budget_day"]),
                    )
                    self._conn.execute("COMMIT")
                    return held_amount
                settled_amount = float(actual_cost_usd)
                self._settle_reservation_locked(row, settled_amount, now=now)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return settled_amount

    @contextmanager
    def _reconciliation_transaction(self):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _valid_nonnegative(value: float, error: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(error)
        return float(value)

    @staticmethod
    def _valid_audit_text(value: str, error: str, *, maximum: int) -> str:
        if not isinstance(value, str) or not value.strip() or len(value) > maximum:
            raise ValueError(error)
        return value.strip()

    @classmethod
    def _serialize_audit_evidence(cls, evidence: dict[str, Any] | None) -> str:
        if evidence is None:
            return "{}"
        if not isinstance(evidence, dict):
            raise ValueError("invalid_audit_evidence")

        # Evidence is metadata only.  Reject obvious credential-bearing keys
        # rather than persisting a token, password, cookie, or authorization
        # header in an immutable audit row.
        sensitive_key = re.compile(
            r"(?:secret|token|password|passwd|credential|authorization|cookie|private[_-]?key|api[_-]?key)",
            re.IGNORECASE,
        )

        def validate(value: Any) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    if not isinstance(key, str) or not key or sensitive_key.search(key):
                        raise ValueError("audit_evidence_must_not_contain_secrets")
                    validate(child)
            elif isinstance(value, list):
                for child in value:
                    validate(child)
            elif isinstance(value, (str, int, float, bool)) or value is None:
                if isinstance(value, float) and not math.isfinite(value):
                    raise ValueError("invalid_audit_evidence")
            else:
                raise ValueError("invalid_audit_evidence")

        validate(evidence)
        try:
            payload = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid_audit_evidence") from exc
        if len(payload) > 16_384:
            raise ValueError("audit_evidence_too_large")
        return payload

    def _conservative_resolution_locked(self, reservation_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM reservation_conservative_resolutions WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchone()

    def _known_floor_locked(self, reservation_id: int) -> float:
        row = self._conn.execute(
            "SELECT known_cost_floor_usd FROM reservation_cost_floors WHERE reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        return float(row[0]) if row is not None else 0.0

    def _settle_reservation_locked(self, row: sqlite3.Row, amount: float, *, now: float) -> None:
        """Caller owns the write transaction and has checked state='reserved'."""
        if amount < self._known_floor_locked(row["id"]):
            raise ValueError("cost_below_known_floor")
        reservation_amount = self._valid_nonnegative(row["amount_usd"], "invalid_reservation_amount")
        budget_row = self._conn.execute(
            "SELECT reserved_usd, spent_usd FROM budget_days WHERE budget_day = ?",
            (row["budget_day"],),
        ).fetchone()
        if budget_row is None:
            raise ValueError("budget_ledger_missing")
        reserved_usd = self._valid_nonnegative(budget_row["reserved_usd"], "budget_ledger_invalid")
        spent_usd = self._valid_nonnegative(budget_row["spent_usd"], "budget_ledger_invalid")
        remaining_reserved = reserved_usd - reservation_amount
        new_spent = spent_usd + amount
        if not math.isfinite(new_spent):
            raise ValueError("budget_ledger_overflow")
        # Do not hide a ledger underflow behind SQL MAX(0, ...).  A tiny
        # floating-point residue is normalized, but a material mismatch aborts
        # the transaction and leaves the reservation held for diagnosis.
        if remaining_reserved < -1e-9:
            raise ValueError("budget_ledger_underflow")
        remaining_reserved = 0.0 if remaining_reserved < 0 else remaining_reserved
        self._conn.execute(
            "UPDATE budget_reservations SET state = 'settled', settled_amount_usd = ?, "
            "settlement_basis = 'confirmed_actual', settled_at = ? WHERE id = ?",
            (amount, now, row["id"]),
        )
        self._conn.execute(
            "UPDATE budget_days SET reserved_usd = ?, spent_usd = ?, updated_at = ? WHERE budget_day = ?",
            (remaining_reserved, new_spent, now, row["budget_day"]),
        )
        self._conn.execute(
            "UPDATE reconciliation_rounds SET state = 'settled', error_code = NULL "
            "WHERE reservation_id = ?", (row["id"],),
        )

    def record_known_cost(self, reservation_id: int, amount: float) -> None:
        amount = self._valid_nonnegative(amount, "invalid_cost_floor")
        with self._reconciliation_transaction():
            self._conn.execute(
                "INSERT INTO reservation_cost_floors VALUES (?, ?) "
                "ON CONFLICT(reservation_id) DO UPDATE SET known_cost_floor_usd = "
                "MAX(known_cost_floor_usd, excluded.known_cost_floor_usd)",
                (reservation_id, amount),
            )
            row = self._conn.execute("SELECT * FROM budget_reservations WHERE id=? AND state='reserved'",
                                     (reservation_id,)).fetchone()
            # A post-resolution floor is evidence for a later explicit true-up,
            # not permission to silently re-hold or settle the estimate.
            if row is not None and self._conservative_resolution_locked(reservation_id) is None:
                if amount > float(row["amount_usd"]):
                    self._conn.execute("UPDATE budget_reservations SET amount_usd=? WHERE id=?", (amount, reservation_id))
                    self._conn.execute("UPDATE budget_days SET reserved_usd=reserved_usd+? WHERE budget_day=?",
                                       (amount-float(row["amount_usd"]), row["budget_day"]))

    def admin_resolve_conservative(
        self,
        reservation_id: int,
        now: float,
        actor: str,
        reason: str,
        evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Explicitly expense one terminal unknown hold conservatively.

        This explicit DB primitive has no implicit age-based side effect. The
        service may call it only after its strict receipt-missing eligibility
        checks, using a distinct automatic actor and complete audit evidence. The
        amount is ``max(reservation.amount_usd, known_cost_floor_usd)``; it is
        debited on the reservation's original UTC budget day and the hold is
        released.  The reservation remains marked as an estimate, not a
        confirmed actual bill.
        """
        if isinstance(reservation_id, bool) or not isinstance(reservation_id, int) or reservation_id <= 0:
            raise ValueError("invalid_reservation_id")
        now = self._valid_nonnegative(now, "invalid_time")
        actor = self._valid_audit_text(actor, "invalid_audit_actor", maximum=256)
        reason = self._valid_audit_text(reason, "invalid_audit_reason", maximum=2048)
        evidence_json = self._serialize_audit_evidence(evidence)

        with self._reconciliation_transaction():
            row = self._conn.execute(
                "SELECT b.*, q.state AS queue_state, q.finished_at "
                "FROM budget_reservations b LEFT JOIN queue q ON q.id = b.queue_id "
                "WHERE b.id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise ValueError("reservation_not_found")

            existing = self._conservative_resolution_locked(reservation_id)
            if existing is not None:
                if (
                    row["state"] == "settled"
                    and row["settlement_basis"] == "conservative_estimate"
                    and existing["actor"] == actor
                    and existing["reason"] == reason
                    and existing["evidence_json"] == evidence_json
                ):
                    return self._resolution_dict_locked(reservation_id)
                raise ValueError("conservative_resolution_conflict")

            if row["state"] != "reserved":
                raise ValueError("reservation_not_held")
            if row["queue_state"] not in ("done", "error") or row["finished_at"] is None:
                raise ValueError("reservation_not_terminal")

            reservation_amount = self._valid_nonnegative(row["amount_usd"], "invalid_reservation_amount")
            observed_floor = self._known_floor_locked(reservation_id)
            amount = max(reservation_amount, observed_floor)
            if amount <= 0:
                # Do not turn an unknown zero into a claimed zero bill.
                raise ValueError("conservative_amount_zero")

            # record_known_cost normally keeps amount_usd at least the floor,
            # but repair that invariant here atomically for old/manual rows.
            if amount > reservation_amount:
                self._conn.execute(
                    "UPDATE budget_reservations SET amount_usd = ? WHERE id = ?",
                    (amount, reservation_id),
                )
                changed = self._conn.execute(
                    "UPDATE budget_days SET reserved_usd = reserved_usd + ?, updated_at = ? "
                    "WHERE budget_day = ?",
                    (amount - reservation_amount, now, row["budget_day"]),
                )
                if changed.rowcount != 1:
                    raise ValueError("budget_ledger_missing")
                row = self._conn.execute(
                    "SELECT b.*, q.state AS queue_state, q.finished_at "
                    "FROM budget_reservations b LEFT JOIN queue q ON q.id = b.queue_id "
                    "WHERE b.id = ?",
                    (reservation_id,),
                ).fetchone()

            # Reuse the normal release/debit invariant, then relabel the
            # settlement explicitly as an estimate before the transaction
            # commits. State=settled is needed to release the hold; the basis
            # column and side audit prevent it from being read as actual.
            self._settle_reservation_locked(row, amount, now=now)
            self._conn.execute(
                "UPDATE budget_reservations SET settled_amount_usd = NULL, settled_at = NULL, "
                "settlement_basis = 'conservative_estimate' WHERE id = ?",
                (reservation_id,),
            )
            self._conn.execute(
                """
                INSERT INTO reservation_conservative_resolutions(
                    reservation_id, budget_day, reservation_amount_usd, observed_floor_usd,
                    conservative_amount_usd, basis, actor, reason, evidence_json, resolved_at
                ) VALUES (?, ?, ?, ?, ?, 'conservative_estimate', ?, ?, ?, ?)
                """,
                (
                    reservation_id,
                    row["budget_day"],
                    reservation_amount,
                    observed_floor,
                    amount,
                    actor,
                    reason,
                    evidence_json,
                    now,
                ),
            )
            return self._resolution_dict_locked(reservation_id)

    def _resolution_dict_locked(self, reservation_id: int) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT r.*, b.state, b.settled_amount_usd, b.settlement_basis "
            "FROM reservation_conservative_resolutions r "
            "JOIN budget_reservations b ON b.id = r.reservation_id "
            "WHERE r.reservation_id = ?",
            (reservation_id,),
        ).fetchone()
        assert row is not None
        result = dict(row)
        try:
            result["evidence"] = json.loads(result.pop("evidence_json"))
        except (KeyError, IndexError, TypeError, json.JSONDecodeError):
            result["evidence"] = {}
        result["actual_known"] = False
        return result

    def conservative_resolution_summary(
        self,
        *,
        now: float | None = None,
        budget_day: str | None = None,
    ) -> dict[str, Any]:
        """Read-only estimate summary, optionally filtered to one UTC day.

        ``estimated_usd`` is intentionally not named or reported as actual
        spend.  Verified late receipts are exposed separately as adjustments.
        """
        if now is not None and budget_day is not None:
            raise ValueError("summary_day_selectors_conflict")
        if now is not None:
            budget_day = self.budget_day(self._valid_nonnegative(now, "invalid_time"))
        if budget_day is not None and (
            not isinstance(budget_day, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", budget_day)
        ):
            raise ValueError("invalid_budget_day")
        where = "" if budget_day is None else " WHERE r.budget_day = ?"
        params: tuple[Any, ...] = () if budget_day is None else (budget_day,)
        with self._lock:
            rows = self._conn.execute(
                "SELECT r.*, b.state, b.settled_amount_usd, b.settlement_basis "
                "FROM reservation_conservative_resolutions r "
                "JOIN budget_reservations b ON b.id = r.reservation_id"
                + where
                + " ORDER BY r.budget_day, r.reservation_id",
                params,
            ).fetchall()
            entries: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                try:
                    item["evidence"] = json.loads(item.pop("evidence_json"))
                except (KeyError, IndexError, TypeError, json.JSONDecodeError):
                    item["evidence"] = {}
                item["actual_known"] = False
                adjustment = self._conn.execute(
                    "SELECT actual_cost_usd, adjustment_usd, basis, adjusted_at "
                    "FROM reservation_conservative_adjustments WHERE reservation_id = ?",
                    (row["reservation_id"],),
                ).fetchone()
                if adjustment is not None:
                    item["verified_actual"] = dict(adjustment)
                    item["actual_known"] = True
                entries.append(item)
        estimated_total = sum(float(item["conservative_amount_usd"]) for item in entries)
        verified = [item for item in entries if item.get("actual_known")]
        verified_total = sum(float(item["verified_actual"]["actual_cost_usd"]) for item in verified)
        unresolved_total = estimated_total - sum(
            float(item["conservative_amount_usd"]) for item in verified
        )
        return {
            "budget_day": budget_day,
            "basis": "conservative_estimate",
            "count": len(entries),
            "estimated_count": len(entries),
            "estimated_usd": round(estimated_total, 8),
            "unverified_estimated_count": len(entries) - len(verified),
            "unverified_estimated_usd": round(unresolved_total, 8),
            "verified_actual_count": len(verified),
            "verified_actual_usd": round(verified_total, 8),
            "reservation_ids": [int(item["reservation_id"]) for item in entries],
            "entries": entries,
        }

    def admin_true_up_conservative(
        self,
        reservation_id: int,
        actual_cost_usd: float,
        now: float,
        actor: str,
        reason: str,
        evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Replace one estimate with an explicitly verified actual receipt.

        This is never called by ordinary reconciliation: it is an explicit
        admin true-up and appends an immutable adjustment audit.
        """
        if isinstance(reservation_id, bool) or not isinstance(reservation_id, int) or reservation_id <= 0:
            raise ValueError("invalid_reservation_id")
        actual = self._valid_nonnegative(actual_cost_usd, "invalid_actual_cost")
        now = self._valid_nonnegative(now, "invalid_time")
        actor = self._valid_audit_text(actor, "invalid_audit_actor", maximum=256)
        reason = self._valid_audit_text(reason, "invalid_audit_reason", maximum=2048)
        evidence_json = self._serialize_audit_evidence(evidence)

        with self._reconciliation_transaction():
            row = self._conn.execute(
                "SELECT * FROM budget_reservations WHERE id = ?", (reservation_id,)
            ).fetchone()
            resolution = self._conservative_resolution_locked(reservation_id)
            if row is None or resolution is None:
                raise ValueError("conservative_resolution_not_found")
            if row["state"] != "settled" or row["settlement_basis"] != "conservative_estimate":
                raise ValueError("reservation_not_conservative_estimate")
            if actual < self._known_floor_locked(reservation_id):
                raise ValueError("actual_below_known_floor")
            estimated = self._valid_nonnegative(
                resolution["conservative_amount_usd"], "invalid_conservative_amount"
            )
            budget = self._conn.execute(
                "SELECT spent_usd FROM budget_days WHERE budget_day = ?", (row["budget_day"],)
            ).fetchone()
            if budget is None:
                raise ValueError("budget_ledger_missing")
            spent = self._valid_nonnegative(budget["spent_usd"], "budget_ledger_invalid")
            new_spent = spent + actual - estimated
            if not math.isfinite(new_spent) or new_spent < -1e-9:
                raise ValueError("budget_ledger_underflow")
            new_spent = 0.0 if new_spent < 0 else new_spent
            self._conn.execute(
                "UPDATE budget_days SET spent_usd = ?, updated_at = ? WHERE budget_day = ?",
                (new_spent, now, row["budget_day"]),
            )
            self._conn.execute(
                "UPDATE budget_reservations SET settled_amount_usd = ?, settled_at = ?, "
                "settlement_basis = 'verified_actual' WHERE id = ?",
                (actual, now, reservation_id),
            )
            self._conn.execute(
                """
                INSERT INTO reservation_conservative_adjustments(
                    resolution_id, reservation_id, budget_day, estimated_amount_usd, actual_cost_usd,
                    adjustment_usd, basis, actor, reason, evidence_json, adjusted_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'verified_actual', ?, ?, ?, ?)
                """,
                (
                    resolution["id"],
                    reservation_id,
                    row["budget_day"],
                    estimated,
                    actual,
                    actual - estimated,
                    actor,
                    reason,
                    evidence_json,
                    now,
                ),
            )
            result = self._resolution_dict_locked(reservation_id)
            result["actual_known"] = True
            result["verified_actual_usd"] = actual
            result["adjustment_usd"] = actual - estimated
            return result

    # Descriptive alias for callers that prefer the word "true-up".
    admin_true_up_conservative_resolution = admin_true_up_conservative

    def list_conservative_resolution_candidates(
        self,
        *,
        now: float | None = None,
        minimum_age_seconds: float = 0.0,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """List terminal held rows without changing them.

        The caller chooses the age policy. This method never auto-resolves a
        row and never treats a missing/invalid receipt as a zero charge.
        """
        if now is None:
            now_value = None
        else:
            now_value = self._valid_nonnegative(now, "invalid_time")
        minimum_age_seconds = self._valid_nonnegative(minimum_age_seconds, "invalid_minimum_age")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10000:
            raise ValueError("invalid_conservative_limit")
        cutoff = None if now_value is None else now_value - minimum_age_seconds
        with self._lock:
            rows = self._conn.execute(
                "SELECT b.id AS reservation_id, b.queue_id, b.monitor_id, b.budget_day, "
                "b.amount_usd, b.created_at, q.state AS queue_state, q.finished_at, "
                "COALESCE(f.known_cost_floor_usd, 0) AS observed_floor_usd "
                "FROM budget_reservations b JOIN queue q ON q.id = b.queue_id "
                "LEFT JOIN reservation_cost_floors f ON f.reservation_id = b.id "
                "WHERE b.state = 'reserved' AND q.state IN ('done', 'error') "
                "AND q.finished_at IS NOT NULL "
                "AND NOT EXISTS (SELECT 1 FROM reservation_conservative_resolutions r "
                "WHERE r.reservation_id = b.id) "
                "AND (? IS NULL OR q.finished_at <= ?) "
                "ORDER BY q.finished_at, b.id LIMIT ?",
                (cutoff, cutoff, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def orphan_summary(self) -> dict[str, Any]:
        """Report reserved rows with no reconciliation record, without mutating them.

        An orphan is intentionally diagnostic-only.  It may be a historical
        unlinked hold or evidence of a failed write; neither case is safe to
        release automatically.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT b.id, b.amount_usd FROM budget_reservations b "
                "LEFT JOIN reconciliation_rounds r ON r.reservation_id = b.id "
                "WHERE b.state = 'reserved' AND r.reservation_id IS NULL "
                "AND NOT EXISTS (SELECT 1 FROM reservation_conservative_resolutions cr "
                "WHERE cr.reservation_id = b.id) "
                "ORDER BY b.id"
            ).fetchall()
        return {
            "count": len(rows),
            "reserved_usd": round(sum(float(row["amount_usd"]) for row in rows), 8),
            "reservation_ids": [int(row["id"]) for row in rows],
            "release_automatic": False,
        }

    def reconciliation_summary(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT count(*) AS held_reservations, "
                "sum(CASE WHEN r.reservation_id IS NULL THEN 1 ELSE 0 END) AS legacy_unlinked, "
                "COALESCE(sum(CASE WHEN r.reservation_id IS NULL THEN b.amount_usd ELSE 0 END),0) AS legacy_held_usd, "
                "sum(CASE WHEN b.budget_basis IS NULL THEN 1 ELSE 0 END) AS legacy_carried_reservations, "
                "COALESCE(sum(CASE WHEN b.budget_basis IS NULL THEN b.amount_usd ELSE 0 END),0) "
                "AS legacy_carried_conservative_usd "
                "FROM budget_reservations b LEFT JOIN reconciliation_rounds r ON r.reservation_id=b.id "
                "WHERE b.state='reserved' AND NOT EXISTS ("
                "SELECT 1 FROM reservation_conservative_resolutions cr WHERE cr.reservation_id=b.id)").fetchone()
        orphan = self.orphan_summary()
        conservative = self.conservative_resolution_summary()
        return {"held_reservations": row["held_reservations"],
                "legacy_unlinked": row["legacy_unlinked"] or 0,
                "legacy_held_usd": round(row["legacy_held_usd"], 8),
                "legacy_carried_reservations": row["legacy_carried_reservations"] or 0,
                "legacy_carried_conservative_usd": round(
                    row["legacy_carried_conservative_usd"], 8
                ),
                "orphan_reservations": orphan["count"],
                "orphan_reserved_usd": orphan["reserved_usd"],
                "orphan_reservation_ids": orphan["reservation_ids"],
                "orphan_release_automatic": orphan["release_automatic"],
                "conservative_resolution_count": conservative["count"],
                "conservative_estimated_usd": conservative["estimated_usd"],
                "conservative_resolution": conservative,
                "interval_seconds": 300}

    def legacy_budget_summary(self, *, budget_day: str) -> dict[str, float]:
        """Return only untagged legacy amounts for one original budget day.

        ``budget_days.spent_usd`` and ``reserved_usd`` are intentionally mixed
        aggregates for admission.  These separate figures are diagnostics so
        a new Lei basis is never presented as if it billed historical rows.
        """
        if not isinstance(budget_day, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", budget_day):
            raise ValueError("invalid_budget_day")
        with self._lock:
            row = self._conn.execute(
                "SELECT "
                "COALESCE(SUM(CASE WHEN b.state = 'settled' "
                "AND (b.settlement_basis IS NULL OR b.settlement_basis <> 'conservative_estimate') "
                "THEN b.settled_amount_usd ELSE 0 END), 0) AS settled_usd, "
                "COALESCE(SUM(CASE WHEN b.state = 'settled' "
                "AND b.settlement_basis = 'conservative_estimate' "
                "THEN COALESCE(b.settled_amount_usd, r.conservative_amount_usd) ELSE 0 END), 0) "
                "AS conservative_estimated_usd, "
                "COALESCE(SUM(CASE WHEN b.state = 'reserved' THEN b.amount_usd ELSE 0 END), 0) "
                "AS held_usd "
                "FROM budget_reservations b LEFT JOIN reservation_conservative_resolutions r "
                "ON r.reservation_id = b.id "
                "WHERE b.budget_day = ? AND b.budget_basis IS NULL",
                (budget_day,),
            ).fetchone()
        return {
            "settled_usd": round(float(row["settled_usd"]), 8),
            "conservative_estimated_usd": round(float(row["conservative_estimated_usd"]), 8),
            "held_usd": round(float(row["held_usd"]), 8),
        }

    def resume_after_reconciliation(self, *, now: float, daily_budget_usd: float,
                                    required_usd: float, next_runs: dict[int, float]) -> bool:
        """Called only after verified settlement; no catch-up burst on unpause."""
        required_usd = self._valid_nonnegative(required_usd, "invalid_required_cost")
        with self._reconciliation_transaction():
            day = self.budget_day(now)
            row = self._ensure_budget_day_locked(day, now=now)
            reserved = float(row["reserved_usd"]) + self._carried_reservations_locked(day)
            if not row["paused_until"] or float(row["spent_usd"]) + reserved + required_usd > daily_budget_usd:
                return False
            self._conn.execute("UPDATE budget_days SET paused_until=NULL WHERE budget_day=?", (day,))
            for mid, due in next_runs.items():
                self._conn.execute(
                    "UPDATE monitor_state SET next_run_at=?, next_run_after=NULL, updated_at=? "
                    "WHERE monitor_id=? AND NOT EXISTS (SELECT 1 FROM queue WHERE monitor_id=? "
                    "AND state IN ('running','queued'))", (due, now, mid, mid))
            return True

    def register_reconciliation(
        self, reservation_id: int, model: str, user_agents: Iterable[str], *,
        now: float, grace_seconds: float = 60.0,
    ) -> None:
        """Persist all identities before transport; never invent identities for old jobs."""
        now = self._valid_nonnegative(now, "invalid_time")
        grace_seconds = self._valid_nonnegative(grace_seconds, "invalid_grace")
        if not isinstance(model, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}", model):
            raise ValueError("invalid_expected_model")
        agents = list(user_agents)
        if len(agents) != 3 or any(not isinstance(a, str) for a in agents) or len(set(agents)) != 3:
            raise ValueError("invalid_probe_identities")
        for agent in agents:
            prefix, separator, identity = agent.partition("/")
            try:
                valid = prefix == "ModelTraceProbe" and separator and str(UUID(identity)) == identity
            except (ValueError, AttributeError):
                valid = False
            if not valid:
                raise ValueError("invalid_probe_identity")
        with self._reconciliation_transaction():
            bound = self._conn.execute(
                "SELECT b.id FROM budget_reservations b JOIN queue q ON q.id = b.queue_id "
                "AND q.reservation_id = b.id AND q.monitor_id = b.monitor_id "
                "WHERE b.id = ? AND b.state = 'reserved' AND q.state = 'running'",
                (reservation_id,),
            ).fetchone()
            if bound is None:
                raise ValueError("reconciliation_not_active")
            existing = self._conn.execute(
                "SELECT expected_model, grace_seconds FROM reconciliation_rounds WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            if existing is not None:
                saved = [r[0] for r in self._conn.execute(
                    "SELECT user_agent FROM reconciliation_probes WHERE reservation_id = ? ORDER BY ordinal",
                    (reservation_id,),
                )]
                if existing["expected_model"] != model or saved != agents or existing["grace_seconds"] != grace_seconds:
                    raise ValueError("reconciliation_identity_conflict")
                return
            self._conn.execute(
                "INSERT INTO reconciliation_rounds(reservation_id, expected_model, created_at, "
                "grace_seconds, next_check_at) VALUES (?, ?, ?, ?, ?)",
                (reservation_id, model, now, grace_seconds, now + grace_seconds),
            )
            self._conn.executemany(
                "INSERT INTO reconciliation_probes(reservation_id, ordinal, user_agent) VALUES (?, ?, ?)",
                [(reservation_id, ordinal, agent) for ordinal, agent in enumerate(agents)],
            )

    def _transition_probe(self, reservation_id: int, ordinal: int, *, now: float, status: str) -> bool:
        now = self._valid_nonnegative(now, "invalid_time")
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal not in range(3):
            raise ValueError("invalid_probe_ordinal")
        with self._reconciliation_transaction():
            # Cancellation remains possible after recovery, but only a live job
            # may acquire permission to send. Attempted probes never revert.
            result = self._conn.execute(
                "UPDATE reconciliation_probes SET status = ?, started_at = ? "
                "WHERE reservation_id = ? AND ordinal = ? AND status = 'planned' "
                "AND EXISTS (SELECT 1 FROM budget_reservations b JOIN queue q "
                "ON q.id = b.queue_id AND q.reservation_id = b.id "
                "WHERE b.id = ? AND "
                "((? = 'cancelled' AND q.state IN ('done','error')) OR "
                "(? = 'attempted' AND b.state = 'reserved' AND q.state = 'running')))",
                (status, now if status == "attempted" else None, reservation_id, ordinal, reservation_id, status, status),
            )
            return result.rowcount == 1

    def mark_probe_attempted(self, reservation_id: int, ordinal: int, *, now: float) -> bool:
        """Send only on True. False on retry is NOT permission to replay transport."""
        return self._transition_probe(reservation_id, ordinal, now=now, status="attempted")

    def cancel_probe(self, reservation_id: int, ordinal: int, *, now: float) -> bool:
        return self._transition_probe(reservation_id, ordinal, now=now, status="cancelled")

    def has_unsettled_monitor(self, monitor_id: int) -> bool:
        with self._lock:
            return self._conn.execute(
                "SELECT 1 FROM budget_reservations b "
                "WHERE b.monitor_id=? AND b.state='reserved' "
                "AND NOT EXISTS (SELECT 1 FROM reservation_conservative_resolutions r "
                "WHERE r.reservation_id = b.id) LIMIT 1",
                (monitor_id,),
            ).fetchone() is not None

    def finish_reconciliation_blocked_job(self, job: QueueItem, *, now: float) -> None:
        # No model call, no reservation and no fake scored round.
        with self._reconciliation_transaction():
            self._conn.execute(
                "UPDATE queue SET state='error', finished_at=?, error_code='reconciliation_pending' "
                "WHERE id=? AND state='running' AND reservation_id IS NULL", (now, job.queue_id),
            )

    def record_nonbillable_proof(self, reservation_id: int, ordinal: int, proof: dict[str, Any], *, now: float) -> None:
        payload = json.dumps(proof, sort_keys=True, separators=(",", ":"))
        with self._reconciliation_transaction():
            self._conn.execute("CREATE TABLE IF NOT EXISTS nonbillable_probe_evidence ("
                "reservation_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, evidence_json TEXT NOT NULL, "
                "recorded_at REAL NOT NULL, PRIMARY KEY(reservation_id,ordinal))")
            for action in ("UPDATE", "DELETE"):
                self._conn.execute(f"CREATE TRIGGER IF NOT EXISTS nonbillable_probe_evidence_no_{action.lower()} "
                    f"BEFORE {action} ON nonbillable_probe_evidence BEGIN SELECT RAISE(ABORT,'append only'); END")
            existing = self._conn.execute("SELECT evidence_json FROM nonbillable_probe_evidence "
                "WHERE reservation_id=? AND ordinal=?", (reservation_id, ordinal)).fetchone()
            if existing is not None and existing[0] != payload:
                raise ValueError("nonbillable_evidence_conflict")
            self._conn.execute("INSERT OR IGNORE INTO nonbillable_probe_evidence VALUES(?,?,?,?)",
                (reservation_id, ordinal, payload, now))

    def list_reconciliation_due(self, *, now: float, limit: int = 100) -> list[dict[str, Any]]:
        now = self._valid_nonnegative(now, "invalid_time")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("invalid_reconciliation_limit")
        with self._lock:
            rows = self._conn.execute(
                "SELECT r.*, b.queue_id, b.monitor_id, b.budget_day, b.amount_usd, "
                "q.monitor_id AS queue_monitor_id, q.finished_at AS queue_finished_at, "
                "q.error_code AS queue_error_code, "
                "COALESCE(f.known_cost_floor_usd, 0) AS known_cost_floor_usd, "
                "b.budget_basis, b.billing_prices_json, b.reserve_prices_json "
                "FROM reconciliation_rounds r JOIN budget_reservations b ON b.id = r.reservation_id "
                "JOIN queue q ON q.id = b.queue_id AND q.reservation_id = b.id "
                "LEFT JOIN reservation_cost_floors f ON f.reservation_id = b.id "
                "WHERE r.state = 'pending' AND b.state = 'reserved' AND q.state IN ('done', 'error') "
                "AND NOT EXISTS (SELECT 1 FROM reservation_conservative_resolutions cr "
                "WHERE cr.reservation_id = b.id) "
                "AND r.next_check_at <= ? AND q.finished_at + r.grace_seconds <= ? "
                "ORDER BY r.next_check_at, r.reservation_id LIMIT ?", (now, now, limit),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["probes"] = [dict(p) for p in self._conn.execute(
                    "SELECT ordinal, user_agent, status, started_at FROM reconciliation_probes "
                    "WHERE reservation_id = ? ORDER BY ordinal", (row["reservation_id"],),
                )]
                result.append(item)
            return result

    def mark_reconciliation_check(
        self, reservation_id: int, *, now: float, next_check_at: float, error_code: str | None = None,
    ) -> bool:
        now = self._valid_nonnegative(now, "invalid_time")
        next_check_at = self._valid_nonnegative(next_check_at, "invalid_next_check")
        if next_check_at <= now:
            raise ValueError("invalid_next_check")
        if error_code is not None and (not isinstance(error_code, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", error_code)):
            raise ValueError("invalid_reconciliation_error_code")
        with self._reconciliation_transaction():
            result = self._conn.execute(
                "UPDATE reconciliation_rounds SET checks = checks + 1, last_checked_at = ?, "
                "next_check_at = ?, error_code = ? WHERE reservation_id = ? AND state = 'pending' "
                "AND (last_checked_at IS NULL OR last_checked_at < ?) "
                "AND EXISTS (SELECT 1 FROM budget_reservations b JOIN queue q "
                "ON q.id = b.queue_id AND q.reservation_id = b.id WHERE b.id = ? "
                "AND b.state = 'reserved' AND q.state IN ('done', 'error') "
                "AND NOT EXISTS (SELECT 1 FROM reservation_conservative_resolutions cr "
                "WHERE cr.reservation_id = b.id))",
                (now, next_check_at, error_code, reservation_id, now, reservation_id),
            )
            return result.rowcount == 1

    def settle_reconciled(
        self, reservation_id: int, *, actual_cost_usd: float, now: float, daily_budget_usd: float,
    ) -> float | None:
        """Commit a VERIFIED complete receipt sum, not an estimate or missing-row zero.

        Caller must validate model/UA/unique receipt identity and exactly one
        receipt for every attempted probe. Only explicitly cancelled probes are
        free. This scalar API cannot itself validate receipt evidence. No pause
        is cleared: the orchestrator owns budget-aware scheduling/resume policy.
        """
        amount = self._valid_nonnegative(actual_cost_usd, "invalid_actual_cost")
        now = self._valid_nonnegative(now, "invalid_time")
        self._valid_nonnegative(daily_budget_usd, "invalid_daily_budget")
        with self._reconciliation_transaction():
            row = self._conn.execute(
                "SELECT b.*, q.state AS queue_state, q.finished_at, r.grace_seconds "
                "FROM budget_reservations b JOIN reconciliation_rounds r ON r.reservation_id = b.id "
                "LEFT JOIN queue q ON q.id = b.queue_id AND q.reservation_id = b.id WHERE b.id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                return None
            if row["state"] == "settled":
                if row["settlement_basis"] == "conservative_estimate":
                    raise ValueError("conservative_resolution_requires_explicit_true_up")
                if float(row["settled_amount_usd"]) != amount:
                    raise ValueError("reconciliation_settlement_conflict")
                return amount
            if row["queue_state"] not in ("done", "error") or row["finished_at"] is None or now < row["finished_at"] + row["grace_seconds"]:
                raise ValueError("reconciliation_not_ready")
            probes = self._conn.execute(
                "SELECT status FROM reconciliation_probes WHERE reservation_id = ?", (reservation_id,),
            ).fetchall()
            if len(probes) != 3 or any(p[0] == "planned" for p in probes):
                raise ValueError("reconciliation_unresolved_probes")
            self._settle_reservation_locked(row, amount, now=now)
            return amount

    def record_round_and_finish(
        self,
        job: QueueItem,
        *,
        status: str,
        target_probability: float | None,
        best_model: str | None,
        checked_at: float,
        message_code: str,
        ranking: list[dict[str, Any]],
        diagnostics: dict[str, Any],
        next_run_at: float | None,
        next_run_after: float | None,
        upstream_statuses: list[int | None],
        receipt_count: int,
        receipt_consistent: bool | None,
        actual_cost_usd: float | None,
        reserved_usd: float | None,
    ) -> int:
        ranking_json = json.dumps(ranking, ensure_ascii=False, separators=(",", ":"))
        diagnostics_json = json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute("SELECT state FROM queue WHERE id = ?", (job.queue_id,)).fetchone()
                if row is None or row["state"] != "running":
                    # A recovered/completed job must never produce a second
                    # round or override its schedule on a repeated completion.
                    self._conn.execute("COMMIT")
                    return 0
                cursor = self._conn.execute(
                    """
                    INSERT INTO rounds(
                        monitor_id, status, target_probability, best_model, checked_at,
                        message_code, ranking_json, diagnostics_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job.monitor_id,
                        status,
                        target_probability,
                        best_model,
                        checked_at,
                        message_code,
                        ranking_json,
                        diagnostics_json,
                    ),
                )
                round_id = int(cursor.lastrowid)
                self._conn.execute(
                    """
                    UPDATE queue
                    SET state = ?, finished_at = ?, error_code = ?
                    WHERE id = ?
                    """,
                    ("done" if status not in {"error", "budget_exhausted"} else "error", checked_at, message_code, job.queue_id),
                )
                self._conn.execute(
                    """
                    UPDATE monitor_state
                    SET last_checked_at = ?, next_run_at = ?, last_error_code = ?,
                        last_upstream_statuses_json = ?, last_receipt_count = ?,
                        last_receipt_consistent = ?, last_actual_cost_usd = ?,
                        last_reserved_usd = ?, next_run_after = ?, updated_at = ?
                    WHERE monitor_id = ?
                    """,
                    (
                        checked_at,
                        next_run_at,
                        None if status in {"match", "uncertain", "suspect"} else message_code,
                        json.dumps(upstream_statuses, separators=(",", ":")),
                        receipt_count,
                        None if receipt_consistent is None else int(receipt_consistent),
                        actual_cost_usd,
                        reserved_usd,
                        next_run_after,
                        checked_at,
                        job.monitor_id,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return round_id

    def recover_running_jobs(
        self, *, now: float, interval_seconds: int,
        next_runs: dict[int, float | None] | None = None,
    ) -> int:
        """Turn orphaned running jobs into safe error rounds without releasing reserves."""
        with self._lock:
            # Config no longer allows automatic paid retests. Do not resurrect
            # one left queued by an older version, either.
            self._conn.execute(
                "UPDATE queue SET state = 'error', finished_at = ?, error_code = 'auto_retest_disabled' "
                "WHERE state = 'queued' AND (trigger = 'auto_retest' OR retest_index > 0)",
                (now,),
            )
            rows = self._conn.execute(
                "SELECT * FROM queue WHERE state = 'running' ORDER BY id"
            ).fetchall()
        recovered = 0
        for row in rows:
            job = QueueItem(
                queue_id=int(row["id"]),
                monitor_id=int(row["monitor_id"]),
                trigger=str(row["trigger"]),
                requested_at=float(row["requested_at"]),
                available_at=float(row["available_at"]),
                retest_index=int(row["retest_index"]),
                started_at=float(row["started_at"]) if row["started_at"] is not None else None,
                reservation_id=int(row["reservation_id"]) if row["reservation_id"] is not None else None,
            )
            self.record_round_and_finish(
                job,
                status="error",
                target_probability=None,
                best_model=None,
                checked_at=now,
                message_code="worker_restarted",
                ranking=[],
                diagnostics={"recovered_orphaned_job": True},
                next_run_at=next_runs.get(job.monitor_id) if next_runs is not None else now + interval_seconds,
                next_run_after=None,
                upstream_statuses=[],
                receipt_count=0,
                receipt_consistent=None,
                actual_cost_usd=None,
                reserved_usd=None,
            )
            recovered += 1
        return recovered

    def get_rounds(self, monitor_id: int, *, limit: int = 16) -> list[sqlite3.Row]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM (
                    SELECT * FROM rounds WHERE monitor_id = ? ORDER BY checked_at DESC, id DESC LIMIT ?
                ) ORDER BY checked_at ASC, id ASC
                """,
                (monitor_id, limit),
            ).fetchall()
        return rows

    def get_latest_round(self, monitor_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                """
                SELECT * FROM rounds WHERE monitor_id = ? ORDER BY checked_at DESC, id DESC LIMIT 1
                """,
                (monitor_id,),
            ).fetchone()

    def get_last_scored_rounds(self, monitor_id: int, *, limit: int = 16) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                """
                SELECT * FROM rounds
                WHERE monitor_id = ? AND status IN ('match', 'uncertain', 'suspect')
                ORDER BY checked_at DESC, id DESC LIMIT ?
                """,
                (monitor_id, limit),
            ).fetchall()

    def cleanup_old(self, *, now: float) -> None:
        cutoff = now - RETENTION_SECONDS
        with self._lock:
            self._conn.execute("DELETE FROM rounds WHERE checked_at < ?", (cutoff,))
            self._conn.execute(
                "DELETE FROM queue WHERE finished_at IS NOT NULL AND finished_at < ? "
                "AND NOT EXISTS (SELECT 1 FROM budget_reservations b "
                "WHERE b.queue_id = queue.id AND b.state = 'reserved')", (cutoff,)
            )
            self._conn.execute(
                "DELETE FROM budget_reservations WHERE settled_at IS NOT NULL AND settled_at < ? "
                "AND NOT EXISTS (SELECT 1 FROM reservation_conservative_resolutions r "
                "WHERE r.reservation_id = budget_reservations.id)",
                (cutoff,),
            )
            cutoff_day = datetime.fromtimestamp(cutoff, tz=timezone.utc).date().isoformat()
            self._conn.execute(
                "DELETE FROM budget_days WHERE budget_day < ? "
                "AND budget_day NOT IN (SELECT budget_day FROM budget_reservations WHERE state = 'reserved') "
                "AND budget_day NOT IN (SELECT budget_day FROM reservation_conservative_resolutions)",
                (cutoff_day,),
            )

    @staticmethod
    def decode_ranking(row: sqlite3.Row) -> list[dict[str, Any]]:
        try:
            value = json.loads(row["ranking_json"])
        except (KeyError, IndexError, TypeError, json.JSONDecodeError):
            return []
        return value if isinstance(value, list) else []

    @staticmethod
    def decode_diagnostics(row: sqlite3.Row) -> dict[str, Any]:
        try:
            value = json.loads(row["diagnostics_json"])
        except (KeyError, IndexError, TypeError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}
