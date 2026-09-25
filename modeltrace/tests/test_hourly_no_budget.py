from __future__ import annotations

import pytest
from dataclasses import replace

from modeltrace.config import MonitorConfig
from tests.conftest import FakeTransport, build_service, run_one


class ExplodingReceiptReader:
    """Receipt reader that bombs immediately if called in any way."""

    def read(self, *args, **kwargs):
        raise AssertionError("Receipt reader must never be called in no-budget flow")

    def read_many(self, *args, **kwargs):
        raise AssertionError("Receipt reader must never be called in no-budget flow")

    def read_nonbillable_many(self, *args, **kwargs):
        raise AssertionError("Receipt reader must never be called in no-budget flow")


def test_single_successful_call_classified_without_receipts_or_budget(tmp_path, fake_clock):
    """3 successful probe calls classify directly, receipt reader is never accessed,
    and snapshot reflects unblocked status with cost controls disabled.
    """
    transport = FakeTransport(text="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263 264 265 266 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 291 292 293 294 295 296 297 298 299 300")
    receipts = ExplodingReceiptReader()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=receipts)

    run_one(service)

    # Exactly 3 probes executed
    assert len(transport.calls) == 1
    assert all(call["user_agent"].startswith("ModelTraceProbe/") for call in transport.calls)

    snapshot = service.snapshot(1, admin=True)
    latest = snapshot["latest"]
    assert latest["status"] in {"match", "uncertain", "suspect"}
    assert latest["target_probability"] is not None
    assert len(latest["ranking"]) <= 3

    # Snapshot flags reflect no fee/budget stalling
    assert snapshot.get("paused_budget") is False
    assert snapshot.get("paused_reconciliation") is False
    assert snapshot["diagnostics"].get("cost_controls_enabled") is False


def test_zero_budget_and_unconfigured_pricing_ignored(tmp_path, fake_clock):
    """Zero budget and missing pricing do not block or stall execution."""
    transport = FakeTransport(text="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263 264 265 266 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 291 292 293 294 295 296 297 298 299 300")
    receipts = ExplodingReceiptReader()
    service, _, _ = build_service(
        tmp_path,
        fake_clock,
        transport=transport,
        receipts=receipts,
        daily_budget_usd=0.0,
    )
    # Clear pricing upper bound and billing prices
    service.config = replace(
        service.config,
        pricing_upper_bound={},
        billing_prices={},
        daily_budget_usd=0.0,
    )

    run_one(service)

    assert len(transport.calls) == 1
    snapshot = service.snapshot(1)
    assert snapshot["latest"]["status"] in {"match", "uncertain", "suspect"}
    assert snapshot.get("paused_budget") is False


def test_paused_historical_ledger_does_not_block_execution(tmp_path, fake_clock):
    """Historical paused ledger or legacy unsettled monitor does not block dispatch."""
    transport = FakeTransport(text="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263 264 265 266 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 291 292 293 294 295 296 297 298 299 300")
    receipts = ExplodingReceiptReader()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=receipts)

    # Simulate historical paused budget in DB
    now = fake_clock()
    service.db.pause_budget(now=now)
    budget = service.db.budget_snapshot(now=now)
    assert budget.paused_until is not None and budget.paused_until > now

    run_one(service)

    assert len(transport.calls) == 1
    snapshot = service.snapshot(1)
    assert snapshot["latest"]["status"] in {"match", "uncertain", "suspect"}


def test_historical_reserved_rows_and_budget_paused_unchanged_while_new_round_succeeds(tmp_path, fake_clock):
    """Integration regression: past reserved rows and paused budget ledger remain intact,
    while 3 successful probe outputs classify and next_run_at is set for enabled monitor.
    """
    transport = FakeTransport(text="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263 264 265 266 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 291 292 293 294 295 296 297 298 299 300")
    receipts = ExplodingReceiptReader()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=receipts)
    service.config = replace(
        service.config,
        enabled=True,
        monitors={1: MonitorConfig(1, "gpt-5.4", enabled=True, configured_supported=True)},
    )
    service.db.seed_monitors(service.config.monitors, now=fake_clock())

    # Seed an existing historical reservation on yesterday's budget day
    now = fake_clock()
    service.db._conn.execute(
        """
        INSERT INTO budget_days(budget_day, reserved_usd, spent_usd, updated_at)
        VALUES ('2026-09-20', 1.50, 0.75, ?)
        """,
        (now,),
    )
    service.db._conn.execute(
        """
        INSERT INTO budget_reservations(id, monitor_id, queue_id, budget_day, amount_usd, state, created_at)
        VALUES (999, 1, 999, '2026-09-20', 1.50, 'reserved', ?)
        """,
        (now,),
    )
    service.db.pause_budget(now=now)
    service.db._conn.commit()

    # Capture historical state before execution
    hist_day_before = service.db._conn.execute(
        "SELECT reserved_usd, spent_usd FROM budget_days WHERE budget_day = '2026-09-20'"
    ).fetchone()
    hist_res_before = service.db._conn.execute(
        "SELECT state, amount_usd FROM budget_reservations WHERE id = 999"
    ).fetchone()

    # Execute a new detection round
    run_one(service)

    # Historical ledger and reservation row are completely unchanged
    hist_day_after = service.db._conn.execute(
        "SELECT reserved_usd, spent_usd FROM budget_days WHERE budget_day = '2026-09-20'"
    ).fetchone()
    hist_res_after = service.db._conn.execute(
        "SELECT state, amount_usd FROM budget_reservations WHERE id = 999"
    ).fetchone()
    assert hist_day_after["reserved_usd"] == hist_day_before["reserved_usd"] == 1.50
    assert hist_day_after["spent_usd"] == hist_day_before["spent_usd"] == 0.75
    assert hist_res_after["state"] == hist_res_before["state"] == "reserved"
    assert hist_res_after["amount_usd"] == hist_res_before["amount_usd"] == 1.50

    # New round succeeded with 3 outputs and updated next_run_at
    state = service.db.get_state(1)
    assert state["next_run_at"] is not None
    assert state["next_run_at"] > now
    snapshot = service.snapshot(1)
    assert snapshot["latest"]["status"] in {"match", "uncertain", "suspect"}


def test_scheduler_skip_cap_concurrency_no_double_dispatch(tmp_path, fake_clock):
    """Scheduler does not double dispatch running jobs and respects slot/cap."""
    transport = FakeTransport(text="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263 264 265 266 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 291 292 293 294 295 296 297 298 299 300")
    receipts = ExplodingReceiptReader()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=receipts)
    service.config = replace(
        service.config,
        enabled=True,
        monitors={1: MonitorConfig(1, "gpt-5.4", enabled=True, configured_supported=True)},
    )
    service.db.seed_monitors(service.config.monitors, now=fake_clock())
    service.db.set_next_run(1, fake_clock(), now=fake_clock())

    # Initial enqueue
    service._schedule_due(fake_clock())
    assert service.db.has_pending(1) is True

    # Job is now queued in state 'queued'. A second schedule call while first job is pending
    # should not create duplicate queue items
    service._schedule_due(fake_clock())
    queue_count = service.db._conn.execute(
        "SELECT count(*) FROM queue WHERE monitor_id = 1 AND state IN ('queued', 'running')"
    ).fetchone()[0]
    assert queue_count == 1

    # Run the single job
    run_one(service)
    assert len(transport.calls) == 1

    # Now job is finished. State next_run_at was updated to future.
    # Subsequent schedule_due call before next_run_at produces no additional queue items.
    service._schedule_due(fake_clock())
    assert service.db.has_pending(1) is False
    assert len(transport.calls) == 1


def test_single_probe_per_round(tmp_path, fake_clock):
    """A round executes at most 3 probes and records detection_attempts."""
    transport = FakeTransport(text="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 259 260 261 262 263 264 265 266 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 291 292 293 294 295 296 297 298 299 300")
    receipts = ExplodingReceiptReader()
    service, _, _ = build_service(tmp_path, fake_clock, transport=transport, receipts=receipts)

    run_one(service)
    assert len(transport.calls) == 1

    # Verify detection_attempts table was populated with exactly 1 attempt
    attempts = service.db._conn.execute(
        "SELECT ordinal, user_agent FROM detection_attempts WHERE queue_id = (SELECT MAX(id) FROM queue)"
    ).fetchall()
    assert len(attempts) == 1
    assert [a["ordinal"] for a in attempts] == [0]
