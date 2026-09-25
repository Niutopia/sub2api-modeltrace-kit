from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from modeltrace.db import ModelTraceDB, RETENTION_SECONDS


DAY_ONE = datetime(2026, 9, 19, tzinfo=timezone.utc).timestamp()
DAY_TWO = datetime(2026, 9, 20, tzinfo=timezone.utc).timestamp()


def make_hold(
    db: ModelTraceDB,
    monitor_id: int,
    amount: float,
    now: float,
    *,
    floor: float | None = None,
    terminal: bool = True,
    daily_budget_usd: float = 100,
) -> int:
    db.seed_monitors([monitor_id], now=now)
    created, queue_id = db.enqueue(monitor_id, now=now, trigger="historical")
    assert created and queue_id is not None
    assert db.claim_next(now=now) is not None
    reservation = db.reserve_budget(
        monitor_id,
        queue_id,
        amount,
        daily_budget_usd=daily_budget_usd,
        now=now,
    )
    assert reservation is not None
    if floor is not None:
        db.record_known_cost(reservation.reservation_id, floor)
    if terminal:
        db._conn.execute(
            "UPDATE queue SET state='error', finished_at=? WHERE id=?",
            (now + 1, queue_id),
        )
    return reservation.reservation_id


def test_admin_resolve_all_19_on_original_days_and_keep_estimate_distinct(tmp_path):
    db = ModelTraceDB(tmp_path / "modeltrace.sqlite3")
    try:
        # The selected historical set totals the reported $2.84300025.  The
        # first row's known floor raises its $0.10 hold to $0.20.
        amounts = [0.10] * 18 + [0.94300025]
        reservation_ids = []
        for index, amount in enumerate(amounts, start=1):
            created_at = DAY_ONE if index <= 10 else DAY_TWO
            reservation_ids.append(
                make_hold(
                    db,
                    index,
                    amount,
                    created_at,
                    floor=0.20 if index == 1 else None,
                )
            )

        paused_until = DAY_TWO + 3600
        db.pause_budget(now=DAY_TWO, until=paused_until)
        before = db.budget_snapshot(now=DAY_TWO)
        assert before.reserved_usd == pytest.approx(2.84300025)

        resolved = [
            db.admin_resolve_conservative(
                reservation_id,
                DAY_TWO + 10,
                "admin:historical-closeout",
                "close terminal unknown holds after review",
                {"batch": "ALL19", "ticket": "billing-review-19"},
            )
            for reservation_id in reservation_ids
        ]

        assert len(resolved) == 19
        assert {item["basis"] for item in resolved} == {"conservative_estimate"}
        assert {item["actual_known"] for item in resolved} == {False}
        assert sum(item["conservative_amount_usd"] for item in resolved) == pytest.approx(2.84300025)
        assert resolved[0]["observed_floor_usd"] == pytest.approx(0.20)

        after = db.budget_snapshot(now=DAY_TWO + 10)
        assert after.reserved_usd == 0
        assert after.spent_usd == pytest.approx(1.74300025)
        assert after.paused_until == pytest.approx(paused_until)

        day_one = db._conn.execute(
            "SELECT spent_usd, reserved_usd FROM budget_days WHERE budget_day='2026-09-19'"
        ).fetchone()
        day_two = db._conn.execute(
            "SELECT spent_usd, reserved_usd FROM budget_days WHERE budget_day='2026-09-20'"
        ).fetchone()
        assert day_one["spent_usd"] == pytest.approx(1.10)
        assert day_one["reserved_usd"] == pytest.approx(0)
        assert day_two["spent_usd"] == pytest.approx(1.74300025)
        assert day_two["reserved_usd"] == pytest.approx(0)

        stored = db._conn.execute(
            "SELECT state, settled_amount_usd, settlement_basis "
            "FROM budget_reservations WHERE id=?",
            (reservation_ids[0],),
        ).fetchone()
        assert stored["state"] == "settled"
        assert stored["settled_amount_usd"] is None
        assert stored["settlement_basis"] == "conservative_estimate"

        summary = db.conservative_resolution_summary()
        assert summary["count"] == 19
        assert summary["estimated_usd"] == pytest.approx(2.84300025)
        assert summary["unverified_estimated_usd"] == pytest.approx(2.84300025)
        assert summary["verified_actual_usd"] == 0
        assert "actual_known" not in summary
        assert db.conservative_resolution_summary(budget_day="2026-09-19")["estimated_usd"] == pytest.approx(1.10)
        assert db.conservative_resolution_summary(now=DAY_TWO)["estimated_usd"] == pytest.approx(1.74300025)
        assert db.conservative_resolution_summary(now=DAY_TWO)["verified_actual_usd"] == 0
        assert db.orphan_summary()["count"] == 0
        assert db.list_conservative_resolution_candidates(now=DAY_TWO)[0:1] == []
    finally:
        db.close()


def test_resolution_requires_terminal_hold_and_is_not_implicit_age_writeoff(tmp_path):
    db = ModelTraceDB(tmp_path / "modeltrace.sqlite3")
    try:
        # The DB intentionally permits only one running queue item. Build the
        # terminal hold first, then leave the second hold active so the fixture
        # exercises both cases without violating the single-worker invariant.
        terminal_id = make_hold(db, 2, 0.35, DAY_ONE)
        active_id = make_hold(db, 1, 0.25, DAY_ONE, terminal=False)
        with pytest.raises(ValueError, match="reservation_not_terminal"):
            db.admin_resolve_conservative(active_id, DAY_TWO, "admin", "review")
        assert db._conn.execute(
            "SELECT state FROM budget_reservations WHERE id=?", (active_id,)
        ).fetchone()[0] == "reserved"

        db.cleanup_old(now=DAY_TWO + RETENTION_SECONDS + 1)
        candidates = db.list_conservative_resolution_candidates(now=DAY_TWO + RETENTION_SECONDS + 1)
        assert [row["reservation_id"] for row in candidates] == [terminal_id]
        assert db.budget_snapshot(now=DAY_TWO + RETENTION_SECONDS + 1).reserved_usd == pytest.approx(0.60)
    finally:
        db.close()


def test_unknown_zero_is_not_cleared_or_reported_as_a_bill(tmp_path):
    db = ModelTraceDB(tmp_path / "modeltrace.sqlite3")
    try:
        reservation_id = make_hold(db, 1, 0.0, DAY_ONE)
        with pytest.raises(ValueError, match="conservative_amount_zero"):
            db.admin_resolve_conservative(reservation_id, DAY_TWO, "admin", "close")
        assert db._conn.execute(
            "SELECT state FROM budget_reservations WHERE id=?", (reservation_id,)
        ).fetchone()[0] == "reserved"
        assert db.budget_snapshot(now=DAY_TWO).reserved_usd == pytest.approx(0.0)
        assert db.conservative_resolution_summary()["count"] == 0
    finally:
        db.close()


def test_resolution_audit_is_immutable_and_idempotent_only_for_same_admin_record(tmp_path):
    db = ModelTraceDB(tmp_path / "modeltrace.sqlite3")
    try:
        reservation_id = make_hold(db, 1, 0.5, DAY_ONE)
        evidence = {"ticket": "AUD-1", "source": "manual-review"}
        first = db.admin_resolve_conservative(reservation_id, DAY_TWO, "admin:a", "close", evidence)
        second = db.admin_resolve_conservative(reservation_id, DAY_TWO + 1, "admin:a", "close", evidence)
        assert second["id"] == first["id"]
        assert db._conn.execute(
            "SELECT count(*) FROM reservation_conservative_resolutions"
        ).fetchone()[0] == 1

        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db._conn.execute(
                "UPDATE reservation_conservative_resolutions SET reason='changed' WHERE id=?",
                (first["id"],),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db._conn.execute(
                "DELETE FROM reservation_conservative_resolutions WHERE id=?",
                (first["id"],),
            )
        with pytest.raises(ValueError, match="conflict"):
            db.admin_resolve_conservative(reservation_id, DAY_TWO, "admin:b", "different", evidence)
        with pytest.raises(ValueError, match="secrets"):
            db.admin_resolve_conservative(
                make_hold(db, 2, 0.1, DAY_ONE),
                DAY_TWO,
                "admin:a",
                "close",
                {"api_key": "do-not-store"},
            )
    finally:
        db.close()


def test_verified_late_receipt_is_explicit_append_only_true_up(tmp_path):
    db = ModelTraceDB(tmp_path / "modeltrace.sqlite3")
    try:
        reservation_id = make_hold(db, 1, 0.4, DAY_ONE, floor=0.4)
        db.admin_resolve_conservative(reservation_id, DAY_TWO, "admin", "bounded close")
        before = db._conn.execute(
            "SELECT spent_usd FROM budget_days WHERE budget_day='2026-09-19'"
        ).fetchone()[0]

        result = db.admin_true_up_conservative(
            reservation_id,
            0.55,
            DAY_TWO + 1,
            "admin:receipt",
            "verified delayed receipt",
            {"receipt_ref": "receipt-row-1"},
        )
        assert result["actual_known"] is True
        assert result["settlement_basis"] == "verified_actual"
        assert result["verified_actual_usd"] == pytest.approx(0.55)
        assert result["adjustment_usd"] == pytest.approx(0.15)
        assert db._conn.execute(
            "SELECT spent_usd FROM budget_days WHERE budget_day='2026-09-19'"
        ).fetchone()[0] == pytest.approx(before + 0.15)
        assert db.conservative_resolution_summary()["estimated_usd"] == pytest.approx(0.4)
        assert db.conservative_resolution_summary()["unverified_estimated_usd"] == 0
        assert db.conservative_resolution_summary()["verified_actual_usd"] == pytest.approx(0.55)
        assert db._conn.execute(
            "SELECT count(*) FROM reservation_conservative_adjustments"
        ).fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db._conn.execute(
                "DELETE FROM reservation_conservative_adjustments"
            )
        before_retry = db.conservative_resolution_summary()
        with pytest.raises(ValueError, match="not_conservative"):
            db.admin_true_up_conservative(
                reservation_id, 0.55, DAY_TWO + 2, "admin:receipt",
                "verified delayed receipt", {"receipt_ref": "receipt-row-1"},
            )
        with pytest.raises(ValueError, match="not_conservative"):
            db.admin_true_up_conservative(reservation_id, 0.55, DAY_TWO + 2, "admin", "again")
        assert db.conservative_resolution_summary() == before_retry
        assert db._conn.execute(
            "SELECT spent_usd FROM budget_days WHERE budget_day='2026-09-19'"
        ).fetchone()[0] == pytest.approx(0.55)
        assert db._conn.execute(
            "SELECT count(*) FROM reservation_conservative_adjustments"
        ).fetchone()[0] == 1
    finally:
        db.close()


@pytest.mark.parametrize("actual", [0.0, 0.25, 0.4, 0.55])
def test_true_up_changes_only_original_day_and_preserves_estimate_audit(tmp_path, actual):
    db = ModelTraceDB(tmp_path / "modeltrace.sqlite3")
    try:
        reservation_id = make_hold(db, 1, 0.4, DAY_ONE)
        db.admin_resolve_conservative(reservation_id, DAY_TWO, "admin", "close")
        other_id = make_hold(db, 2, 0.3, DAY_TWO)
        db.admin_resolve_conservative(other_id, DAY_TWO, "admin", "close")
        original_audit = tuple(db._conn.execute(
            "SELECT * FROM reservation_conservative_resolutions WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone())

        db.admin_true_up_conservative(reservation_id, actual, DAY_TWO + 1, "admin", "receipt")

        assert tuple(db._conn.execute(
            "SELECT * FROM reservation_conservative_resolutions WHERE reservation_id=?",
            (reservation_id,),
        ).fetchone()) == original_audit
        assert db._conn.execute(
            "SELECT spent_usd FROM budget_days WHERE budget_day='2026-09-19'"
        ).fetchone()[0] == pytest.approx(actual)
        snapshot = db.budget_snapshot(now=DAY_TWO + 1)
        assert snapshot.spent_usd == pytest.approx(0.3)
        assert snapshot.reserved_usd == 0
        first = db.conservative_resolution_summary(now=DAY_ONE)
        assert first["estimated_usd"] == pytest.approx(0.4)
        assert first["verified_actual_count"] == 1
        assert first["verified_actual_usd"] == pytest.approx(actual)
        assert first["unverified_estimated_count"] == 0
        assert first["entries"][0]["actual_known"] is True
        summary = db.conservative_resolution_summary()
        assert summary["estimated_usd"] == pytest.approx(0.7)
        assert summary["unverified_estimated_count"] == 1
        assert summary["unverified_estimated_usd"] == pytest.approx(0.3)
        assert summary["verified_actual_usd"] == pytest.approx(actual)
    finally:
        db.close()
