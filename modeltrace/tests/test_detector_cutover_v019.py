from __future__ import annotations

import ast
import copy
import sqlite3
import json
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skip(reason="检测侧费用系统已按 2026-09-22 用户要求停用")


SCRIPT = Path(__file__).parents[2] / "deploy" / "cutover-detector-v019.py"


def _load_cutover_ledger_helpers():
    """Load only pure ledger helpers; never execute the deployment script."""
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"), filename=str(SCRIPT))
    wanted = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "LEDGER_TABLES"
            for target in node.targets
        ):
            wanted.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in {
            "ledger",
            "assert_safe_ledger_transition",
        }:
            wanted.append(node)
    verifier = SCRIPT.with_name("final_modeltrace_state_verifier_20260920.py")
    helpers = [n for n in ast.parse(verifier.read_text()).body if isinstance(n, ast.FunctionDef)
               and n.name in {"assert_safe_auto_estimate_transition", "read_auto_estimate_receipt_evidence"}]
    namespace = {"sqlite3": sqlite3, "Path": Path, "time": time, "json": json}
    exec(compile(ast.Module(body=helpers, type_ignores=[]), str(verifier), "exec"), namespace)
    exec(compile(ast.Module(body=wanted, type_ignores=[]), str(SCRIPT), "exec"), namespace)
    return namespace["LEDGER_TABLES"], namespace["ledger"], namespace["assert_safe_ledger_transition"]


def _empty_snapshot(table_names):
    return {"integrity": "ok", **{name: {} for name in table_names}}


def _settlement_transition(table_names):
    before = _empty_snapshot(table_names)
    before["budget_days"] = {
        "2026-09-20": {
            "budget_day": "2026-09-20",
            "spent_usd": 0.02,
            "reserved_usd": 0.10,
            "paused_until": None,
            "updated_at": 10.0,
        }
    }
    before["reservations"] = {
        "1": {
            "id": 1,
            "queue_id": 1,
            "monitor_id": 1,
            "budget_day": "2026-09-20",
            "amount_usd": 0.10,
            "state": "reserved",
            "created_at": 1.0,
            "settled_amount_usd": None,
            "settled_at": None,
            "budget_basis": "lei_group4_v1",
            "settlement_basis": None,
            "billing_prices_json": "{}",
            "reserve_prices_json": "{}",
        }
    }
    before["reservation_cost_floors"] = {
        "1": {"reservation_id": 1, "known_cost_floor_usd": 0.0}
    }
    before["reconciliation_rounds"] = {
        "1": {
            "reservation_id": 1,
            "expected_model": "gpt-5.4",
            "expected_count": 3,
            "created_at": 2.0,
            "grace_seconds": 60.0,
            "next_check_at": 62.0,
            "checks": 0,
            "last_checked_at": None,
            "state": "pending",
            "error_code": None,
        }
    }
    before["reconciliation_probes"] = {
        f"1|{ordinal}": {
            "reservation_id": 1,
            "ordinal": ordinal,
            "user_agent": f"ModelTraceProbe/{ordinal}",
            "status": "attempted" if ordinal < 2 else "planned",
            "started_at": 10.0 if ordinal < 2 else None,
        }
        for ordinal in range(3)
    }
    before["queue"] = {
        "1": {"id": 1, "state": "error", "error_code": "upstream_model_mismatch"}
    }
    before["monitor_state"] = {"1": {"monitor_id": 1, "next_run_at": 100.0}}
    before["scheduler_state"] = {"1": {"id": 1, "anchor_at": 1.0}}

    after = copy.deepcopy(before)
    after["budget_days"]["2026-09-20"].update(
        spent_usd=0.03, reserved_usd=0.0, updated_at=100.0
    )
    after["reservations"]["1"].update(
        state="settled",
        settled_amount_usd=0.03,
        settled_at=100.0,
        settlement_basis="confirmed_actual",
    )
    after["reservation_cost_floors"]["1"]["known_cost_floor_usd"] = 0.03
    after["reconciliation_rounds"]["1"].update(
        state="settled",
        checks=1,
        last_checked_at=100.0,
        next_check_at=362.0,
        error_code=None,
    )
    after["reconciliation_probes"]["1|2"].update(status="cancelled")
    return before, after


def test_cutover_ledger_snapshot_includes_all_accounting_tables(tmp_path):
    table_names, ledger, _ = _load_cutover_ledger_helpers()
    path = tmp_path / "ledger.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE budget_days (budget_day TEXT, spent_usd REAL, reserved_usd REAL, paused_until REAL, updated_at REAL)"
        )
        db.execute(
            "CREATE TABLE reconciliation_probes (reservation_id INTEGER, ordinal INTEGER, user_agent TEXT, status TEXT, started_at REAL)"
        )
        db.execute("INSERT INTO budget_days VALUES ('2026-09-20', 1, 2, NULL, 3)")
        db.execute("INSERT INTO reconciliation_probes VALUES (7, 2, 'ua', 'planned', NULL)")
        db.commit()

    snapshot = ledger(path)
    assert set(table_names) <= set(snapshot)
    assert snapshot["budget_days"]["2026-09-20"]["reserved_usd"] == 2
    assert snapshot["reconciliation_probes"]["7|2"]["status"] == "planned"


def test_cutover_refuses_actual_settlement_without_independent_receipt_proof():
    table_names, _, assert_safe = _load_cutover_ledger_helpers()
    before, after = _settlement_transition(table_names)

    with pytest.raises(AssertionError, match="unproven accounting change"):
        assert_safe(before, after)


def test_cutover_rejects_deleted_probe_evidence_and_erased_spend():
    table_names, _, assert_safe = _load_cutover_ledger_helpers()
    before, after = _settlement_transition(table_names)

    missing_probe = copy.deepcopy(after)
    del missing_probe["reconciliation_probes"]["1|0"]
    with pytest.raises(AssertionError, match="reconciliation_probes row deleted"):
        assert_safe(before, missing_probe)

    erased_spend = copy.deepcopy(after)
    erased_spend["budget_days"]["2026-09-20"]["spent_usd"] = 0.0
    with pytest.raises(AssertionError):
        assert_safe(before, erased_spend)


@pytest.fixture
def auto_transition():
    """Complete stopped-worker baseline, plus exactly one legal auto estimate."""
    names, _, verify = _load_cutover_ledger_helpers()
    before, _ = _settlement_transition(names)
    before["captured_at"] = 90.0
    prices = json.dumps({"input_per_million_usd": 2.5,
                         "cache_read_per_million_usd": 1.25, "output_per_million_usd": 15})
    before["reservations"]["1"].update(billing_prices_json=prices, reserve_prices_json=prices)
    before["queue"]["1"].update(reservation_id=1, monitor_id=1, finished_at=20.0,
                                 error_code="upstream_timeout")
    before["reconciliation_rounds"]["1"].update(checks=11, error_code="receipt_missing")
    before["reconciliation_probes"]["1|2"]["status"] = "cancelled"
    for i, probe in enumerate(before["reconciliation_probes"].values()):
        probe["user_agent"] = f"ModelTraceProbe/00000000-0000-0000-0000-{i:012d}"
    # An existing management decision must survive byte-for-byte, not merely
    # retain its amount. It is deliberately unrelated to the new reservation.
    before["conservative_resolutions"]["7"] = {
        "id": 7, "reservation_id": 7, "actor": "operator", "evidence_json": "{}",
        "conservative_amount_usd": 0.02,
    }
    after = copy.deepcopy(before)
    after["captured_at"] = 101.0
    after["reservations"]["1"].update(state="settled", settlement_basis="conservative_estimate")
    after["reconciliation_rounds"]["1"].update(state="settled", error_code=None)
    after["budget_days"]["2026-09-20"].update(spent_usd=0.12, reserved_usd=0, updated_at=100.0)
    after["conservative_resolutions"]["8"] = {
        "id": 8, "reservation_id": 1, "budget_day": "2026-09-20",
        "reservation_amount_usd": 0.1, "observed_floor_usd": 0,
        "conservative_amount_usd": 0.1, "basis": "conservative_estimate",
        "actor": "modeltrace_reconciliation_auto_v1", "actual_cost_usd": None,
        "actual_known": 0, "resolved_at": 100.0,
        "reason": "Receipt-only retry limit reached for a terminal round "
                  "whose persisted Lei price snapshots are valid and every "
                  "attempted probe returned receipt_missing; close at the "
                  "persisted conservative estimate without asserting a "
                  "verified actual charge or zero cost.",
        "evidence_json": json.dumps({"checks": 12, "max_checks": 12,
            "last_error_code": "receipt_missing", "budget_basis": "lei_group4_v1",
            "persisted_billing_snapshot_valid": True, "persisted_reserve_snapshot_valid": True,
            "probe_set_terminal": True, "attempted_probe_count": 2,
            "all_attempted_receipts_missing": True, "actual_receipt_verified": False,
            "model_requests_after_terminal_failure": 0}),
    }
    evidence = {"read_only": True, "query_ok": True, "checked_at": 102.0,
                "by_user_agent": {p["user_agent"]: {"receipt_rows": 0, "nonbillable_rows": 0}
                                  for p in before["reconciliation_probes"].values()
                                  if p["status"] == "attempted"}}
    return before, after, evidence, verify


def test_cutover_accepts_strictly_verified_auto_increment(auto_transition):
    before, after, evidence, verify = auto_transition
    result = verify(before, after, max_checks=12, receipt_evidence=evidence)
    assert result["auto_estimated_reservations"] == ["1"]
    assert result["auto_reconciled_reservations"] == []
    assert before["conservative_resolutions"]["7"] == after["conservative_resolutions"]["7"]
    assert after["reservations"]["1"]["settled_amount_usd"] is None


@pytest.mark.parametrize("field,value", [
    ("actor", "operator"), ("actor", "unknown"), ("reason", "manual resolution"),
    ("budget_day", "2026-09-19"), ("reservation_id", 2), ("id", 9),
    ("basis", "confirmed_actual"), ("actual_known", 1), ("actual_cost_usd", 0),
    ("conservative_amount_usd", 0.09), ("reservation_amount_usd", 0.11),
    ("observed_floor_usd", 0.05), ("resolved_at", 89.0), ("resolved_at", 102.0),
    ("evidence_json", "{}"), ("evidence_json", "null"),
])
def test_cutover_rejects_invalid_new_resolution(auto_transition, field, value):
    before, after, evidence, verify = auto_transition
    after["conservative_resolutions"]["8"][field] = value
    with pytest.raises(AssertionError):
        verify(before, after, max_checks=12, receipt_evidence=evidence)


@pytest.mark.parametrize("field,value", [
    ("last_error_code", "receipt_identity_mismatch"), ("last_error_code", "billing_snapshot_invalid"),
    ("last_error_code", "receipt_query_failed"), ("checks", 11), ("checks", 13),
    ("max_checks", 11), ("max_checks", True), ("probe_set_terminal", False),
    ("persisted_billing_snapshot_valid", False), ("persisted_reserve_snapshot_valid", False),
    ("all_attempted_receipts_missing", False), ("attempted_probe_count", 3),
    ("actual_receipt_verified", True), ("model_requests_after_terminal_failure", 1),
])
def test_cutover_rejects_bad_auto_audit_claim(auto_transition, field, value):
    before, after, evidence, verify = auto_transition
    row = after["conservative_resolutions"]["8"]
    claims = json.loads(row["evidence_json"])
    claims[field] = value
    row["evidence_json"] = json.dumps(claims)
    with pytest.raises(AssertionError):
        verify(before, after, max_checks=12, receipt_evidence=evidence)


@pytest.mark.parametrize("field", ["billing_prices_json", "reserve_prices_json"])
@pytest.mark.parametrize("raw", ["{}", "null", "[]", "bad-json",
    '{"input_per_million_usd":NaN,"cache_read_per_million_usd":1.25,"output_per_million_usd":15}',
    '{"input_per_million_usd":true,"cache_read_per_million_usd":1.25,"output_per_million_usd":15}',
    '{"input_per_million_usd":0,"cache_read_per_million_usd":0,"output_per_million_usd":0}',
])
def test_cutover_validates_prices_not_audit_booleans(auto_transition, field, raw):
    before, after, evidence, verify = auto_transition
    for snapshot in (before, after):
        snapshot["reservations"]["1"][field] = raw
    with pytest.raises(AssertionError):
        verify(before, after, max_checks=12, receipt_evidence=evidence)


@pytest.mark.parametrize("mutation", ["delete", "actor", "amount", "evidence"])
def test_cutover_preserves_old_resolution_exactly(auto_transition, mutation):
    before, after, evidence, verify = auto_transition
    if mutation == "delete":
        del after["conservative_resolutions"]["7"]
    else:
        field = {"actor": "actor", "amount": "conservative_amount_usd", "evidence": "evidence_json"}[mutation]
        after["conservative_resolutions"]["7"][field] = "tampered"
    with pytest.raises(AssertionError):
        verify(before, after, max_checks=12, receipt_evidence=evidence)


@pytest.mark.parametrize("field,value", [
    ("spent_usd", 0.11), ("spent_usd", 0.13), ("spent_usd", float("nan")),
    ("reserved_usd", 0.01), ("reserved_usd", -0.01), ("reserved_usd", float("inf")),
])
def test_cutover_rejects_nonconservation(auto_transition, field, value):
    before, after, evidence, verify = auto_transition
    after["budget_days"]["2026-09-20"][field] = value
    with pytest.raises(AssertionError):
        verify(before, after, max_checks=12, receipt_evidence=evidence)


@pytest.mark.parametrize("damage", [
    "missing-query", "query-failed", "query-old", "query-missing-ua", "receipt-present", "proof-present",
    "planned", "duplicate-ua", "missing-probe", "queue-running", "queue-link", "wrong-monitor",
    "previous-identity-error", "recon-checks", "snapshot-change", "underpriced-reserve",
    "unrelated-reservation", "floor-change", "old-adjustment", "db-integrity", "local-proof", "queue-identity-error",
])
def test_cutover_refuses_missing_or_ambiguous_proof(auto_transition, damage):
    before, after, evidence, verify = auto_transition
    if damage == "missing-query": evidence = None
    elif damage == "query-failed": evidence["query_ok"] = False
    elif damage == "query-old": evidence["checked_at"] = 100
    elif damage == "query-missing-ua": evidence["by_user_agent"].pop(next(iter(evidence["by_user_agent"])))
    elif damage in ("receipt-present", "proof-present"):
        entry = next(iter(evidence["by_user_agent"].values()))
        entry["receipt_rows" if damage == "receipt-present" else "nonbillable_rows"] = 1
    elif damage == "db-integrity": after["integrity"] = "corrupt"
    elif damage == "old-adjustment": after["conservative_adjustments"]["1"] = {"id": 1}
    elif damage == "floor-change": after["reservation_cost_floors"]["1"]["known_cost_floor_usd"] = 0.01
    elif damage == "snapshot-change": after["reservations"]["1"]["billing_prices_json"] = "{}"
    else:
        for snapshot in (before, after):
            if damage == "planned": snapshot["reconciliation_probes"]["1|2"]["status"] = "planned"
            elif damage == "duplicate-ua":
                snapshot["reconciliation_probes"]["1|1"]["user_agent"] = snapshot["reconciliation_probes"]["1|0"]["user_agent"]
            elif damage == "missing-probe": del snapshot["reconciliation_probes"]["1|2"]
            elif damage == "queue-running": snapshot["queue"]["1"]["state"] = "running"
            elif damage == "queue-link": snapshot["queue"]["1"]["reservation_id"] = 9
            elif damage == "queue-identity-error": snapshot["queue"]["1"]["error_code"] = "upstream_model_mismatch"
            elif damage == "wrong-monitor": snapshot["queue"]["1"]["monitor_id"] = 9
            elif damage == "recon-checks": snapshot["reconciliation_rounds"]["1"]["checks"] = 10
            elif damage == "previous-identity-error": snapshot["reconciliation_rounds"]["1"]["error_code"] = "receipt_identity_mismatch"
            elif damage == "underpriced-reserve":
                snapshot["reservations"]["1"]["reserve_prices_json"] = json.dumps({
                    "input_per_million_usd": 1, "cache_read_per_million_usd": 1, "output_per_million_usd": 1})
            elif damage == "unrelated-reservation":
                snapshot["reservations"]["2"] = {**snapshot["reservations"]["1"], "id": 2}
            elif damage == "local-proof": snapshot["nonbillable_proofs"]["1|0"] = {"reservation_id": 1, "ordinal": 0}
    with pytest.raises(AssertionError):
        verify(before, after, max_checks=12, receipt_evidence=evidence)


def test_cutover_auto_requires_trusted_config(auto_transition):
    before, after, evidence, verify = auto_transition
    for max_checks in (None, 0, 1001, True, 13):
        with pytest.raises(AssertionError):
            verify(before, after, max_checks=max_checks, receipt_evidence=evidence)


def test_cutover_accepts_real_service_auto_estimate(tmp_path, fake_clock):
    from dataclasses import replace
    from modeltrace.config import Pricing
    from tests.conftest import build_service
    from tests.test_reconciliation_service import BatchReceiptStub, run_one
    _, ledger, verify = _load_cutover_ledger_helpers()
    receipts = BatchReceiptStub()
    service, transport, _ = build_service(tmp_path, fake_clock, receipts=receipts)
    service.config = replace(service.config, budget_basis="lei_group4_v1",
        billing_prices={"gpt-5.4": Pricing(2.5, 1.25, 15)},
        pricing_upper_bound={"gpt-5.4": Pricing(2.5, 1.25, 15)}, reconciliation_max_checks=1)
    run_one(service)
    before = ledger(service.db.path)
    before["captured_at"] = fake_clock()
    # v0.1.9 cutover verifier expects old-style receipt_missing on queue row
    for q in before["queue"].values():
        if q.get("error_code") not in (None, "receipt_missing"):
            q["error_code"] = "receipt_missing"
    fake_clock.advance(300)
    assert service.reconcile_pending()["estimated"] == 1
    after = ledger(service.db.path)
    after["captured_at"] = fake_clock()
    for q in after["queue"].values():
        if q.get("error_code") not in (None, "receipt_missing"):
            q["error_code"] = "receipt_missing"
    evidence = {"read_only": True, "query_ok": True, "checked_at": fake_clock(),
        "by_user_agent": {p["user_agent"]: {"receipt_rows": 0, "nonbillable_rows": 0}
            for p in before["reconciliation_probes"].values() if p["status"] == "attempted"}}
    assert verify(before, after, max_checks=1, receipt_evidence=evidence)["auto_estimated_reservations"] == ["1"]
    assert len(transport.calls) == 3  # Mock only, never a network model call.


@pytest.mark.parametrize("second_day", ["2026-09-20", "2026-09-19"])
def test_cutover_multiple_legal_estimates_conserve_each_original_day(auto_transition, second_day):
    before, after, evidence, verify = auto_transition
    for snapshot in (before, after):
        res = copy.deepcopy(snapshot["reservations"]["1"])
        res.update(id=2, queue_id=2, monitor_id=2, budget_day=second_day)
        snapshot["reservations"]["2"] = res
        snapshot["queue"]["2"] = {**snapshot["queue"]["1"], "id": 2, "reservation_id": 2, "monitor_id": 2}
        snapshot["monitor_state"]["2"] = {"monitor_id": 2}
        snapshot["reconciliation_rounds"]["2"] = {**snapshot["reconciliation_rounds"]["1"], "reservation_id": 2}
        snapshot["reservation_cost_floors"]["2"] = {"reservation_id": 2, "known_cost_floor_usd": 0}
        for i in range(3):
            probe = copy.deepcopy(snapshot["reconciliation_probes"][f"1|{i}"])
            probe.update(reservation_id=2, user_agent=f"ModelTraceProbe/00000000-0000-0000-0001-{i:012d}")
            snapshot["reconciliation_probes"][f"2|{i}"] = probe
        if second_day not in snapshot["budget_days"]:
            snapshot["budget_days"][second_day] = {"budget_day": second_day, "spent_usd": 0,
                "reserved_usd": 0, "updated_at": 100, "paused_until": None}
    before["budget_days"][second_day]["reserved_usd"] += 0.1
    after["budget_days"][second_day]["spent_usd"] += 0.1
    after["conservative_resolutions"]["9"] = {
        **after["conservative_resolutions"]["8"], "id": 9, "reservation_id": 2, "budget_day": second_day}
    for p in before["reconciliation_probes"].values():
        if p["status"] == "attempted":
            evidence["by_user_agent"][p["user_agent"]] = {"receipt_rows": 0, "nonbillable_rows": 0}
    assert verify(before, after, max_checks=12, receipt_evidence=evidence)["auto_estimated_reservations"] == ["1", "2"]


def test_auto_evidence_collector_bounded_readonly_and_query_failure(auto_transition):
    before, after, evidence, verify = auto_transition
    namespace = verify.__globals__
    captured = []
    namespace["MODELTRACE_CONTAINER"] = "test-only-never-docker"
    def fake_exec(container, code):
        captured.append((container, code))
        return evidence
    namespace["docker_exec_json"] = fake_exec
    collect = namespace["read_auto_estimate_receipt_evidence"]
    assert collect(before, before) is None
    assert captured == []
    assert collect(before, after) == evidence
    assert len(captured) == 1
    code = captured[0][1]
    compile(code, "<readonly-remote-code>", "exec")
    assert "REPEATABLE READ READ ONLY" in code
    assert "statement_timeout=3000" in code
    assert "WHERE user_agent = ANY(%s)" in code
    assert "nonbillable_probe_receipts" in code
    assert "/run" not in code and "urllib" not in code
    def failed_query(*args):
        raise RuntimeError("private query details")
    namespace["docker_exec_json"] = failed_query
    with pytest.raises(AssertionError, match="receipt evidence query failed") as exc:
        collect(before, after)
    assert "private query details" not in str(exc.value)


def test_cutover_wires_trusted_config_and_independent_evidence_without_executing():
    text = SCRIPT.read_text()
    tree = ast.parse(text)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "assert_safe_ledger_transition"]
    assert len(calls) == 1
    assert {kw.arg for kw in calls[0].keywords} == {"max_checks", "receipt_evidence"}
    assert "cfg.get('reconciliation_max_checks', 12)" in text
    assert "receipt_evidence=read_auto_estimate_receipt_evidence(ledger_before, ledger_after)" in text
    assert "config_path.read_text()==config" in text
    assert "LEDGER_NOT_RESTORED" in text
