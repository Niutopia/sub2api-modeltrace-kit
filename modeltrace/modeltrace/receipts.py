from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass
from typing import Any, Callable


# Every live reader uses the same ten-column view contract.  The legacy
# accounting path simply ignores cache_creation_tokens after reading it; it no
# longer changes the SQL shape based on the process-wide budget basis.  Keeping
# the *_WITH_CACHE_CREATION aliases avoids breaking in-memory callers and old
# tests while making the aliases exactly the same contract.
RECEIPT_QUERY = """
SELECT account_id, model, input_tokens,output_tokens,cache_read_tokens,total_cost,account_stats_cost,service_tier,created_at,cache_creation_tokens
FROM modeltrace_monitoring.probe_receipts
WHERE user_agent=%s
"""
RECEIPT_QUERY_WITH_CACHE_CREATION = RECEIPT_QUERY

RECEIPT_BATCH_QUERY = """
SELECT user_agent, account_id, model, input_tokens,output_tokens,cache_read_tokens,total_cost,account_stats_cost,service_tier,created_at,cache_creation_tokens
FROM modeltrace_monitoring.probe_receipts
WHERE user_agent = ANY(%s)
"""
RECEIPT_BATCH_QUERY_WITH_CACHE_CREATION = RECEIPT_BATCH_QUERY
RECEIPT_BATCH_MAX = 60
RECEIPT_BATCH_TIMEOUT_SECONDS = 2.0
_PROBE_USER_AGENT = re.compile(
    r"ModelTraceProbe/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


@dataclass(frozen=True)
class Receipt:
    account_id: str | None
    model: str | None
    input_tokens: int | None
    output_tokens: int | None
    cache_read_tokens: int | None
    total_cost: float | None
    account_stats_cost: float | None
    service_tier: str | None
    created_at: Any
    # Direct legacy/in-memory callers may omit this field; live readers always
    # request and validate it through the ten-column view contract.
    cache_creation_tokens: int | None = 0

    @property
    def charge_usd(self) -> float | None:
        # Match Sub2API's account-cost semantics: account_stats_cost is the
        # actual account-side upstream cost when present; total_cost is the
        # fallback for rows that do not have a custom account calculation.
        return self.account_stats_cost if self.account_stats_cost is not None else self.total_cost


@dataclass(frozen=True)
class ReceiptResult:
    receipts: list[Receipt]
    error_code: str | None


def _safe_int(value: Any) -> int | None:
    """Parse a non-negative integer without coercing malformed data.

    Database drivers normally return ``int`` for token columns.  The narrow
    string form is retained for defensive compatibility with test doubles and
    older drivers, but decimal strings, floats, booleans, and arbitrary text
    are rejected instead of being truncated or coerced.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value):
        try:
            number = int(value)
        except (TypeError, ValueError, OverflowError):
            return None
    else:
        return None
    return number if number >= 0 else None


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return number


class ReceiptReader:
    """Reads only the restricted monitoring view and never stores raw rows."""

    def __init__(
        self,
        database_url: str | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        wait_seconds: float = 10.0,
        connect_factory: Callable[..., Any] | None = None,
        include_cache_creation_tokens: bool | None = None,
    ):
        self.database_url = database_url
        self.clock = clock
        self.sleeper = sleeper
        self.wait_seconds = wait_seconds
        self.connect_factory = connect_factory
        # Kept as a compatibility input only.  The reader always uses the
        # complete view contract regardless of the current config/basis.
        self.include_cache_creation_tokens = True

    @property
    def _query(self) -> str:
        return RECEIPT_QUERY

    @property
    def _batch_query(self) -> str:
        return RECEIPT_BATCH_QUERY

    def _connect(self, remaining: float) -> Any:
        if self.connect_factory is not None:
            return self.connect_factory(self.database_url, remaining)
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - production dependency is installed
            raise RuntimeError("psycopg_unavailable") from exc
        timeout_seconds = max(1, min(10, int(math.ceil(remaining))))
        statement_ms = max(1_000, int(remaining * 1000))
        return psycopg.connect(
            self.database_url,
            connect_timeout=timeout_seconds,
            options=f"-c statement_timeout={statement_ms}",
        )

    @staticmethod
    def _row_to_receipt(row: Any, *, include_cache_creation_tokens: bool | None = None) -> Receipt:
        values = list(row)
        # Direct in-memory legacy callers may still provide the historical
        # nine-column shape.  The live read/read_many paths reject that shape
        # before reaching this helper, so a missing column can never silently
        # become zero in production.  Ten-column rows are always parsed with
        # the cache-creation field present.
        has_cache_creation_column = (
            len(values) >= 10
            if include_cache_creation_tokens is None
            else include_cache_creation_tokens
        )
        expected = 10 if has_cache_creation_column else 9
        values += [None] * max(0, expected - len(values))
        account_id = None if values[0] is None else str(values[0])
        model = None if values[1] is None else str(values[1])
        total_cost = _safe_float(values[5])
        account_stats_cost = _safe_float(values[6])
        service_tier = None if values[7] is None else str(values[7])
        created_at = values[8]
        cache_creation_tokens = _safe_int(values[9]) if has_cache_creation_column else 0
        # A present-but-invalid upstream cost is unknown, not permission to
        # fall back to another cost column (legacy accounting path).
        if values[6] is not None and (isinstance(values[6], bool) or _safe_float(values[6]) is None):
            total_cost = account_stats_cost = None
        return Receipt(
            account_id=account_id,
            model=model,
            input_tokens=_safe_int(values[2]),
            output_tokens=_safe_int(values[3]),
            cache_read_tokens=_safe_int(values[4]),
            total_cost=total_cost,
            account_stats_cost=account_stats_cost,
            service_tier=service_tier,
            created_at=created_at,
            cache_creation_tokens=cache_creation_tokens,
        )

    def read_many(self, user_agents: list[str]) -> dict[str, ReceiptResult]:
        """Read one reconciliation batch without polling, sleeping, or retrying.

        Accept at most 60 distinct, exact UUID-form ModelTraceProbe identifiers.
        Invalid identifiers fail closed; an oversized valid batch fails in full
        without connecting (the caller must chunk it, never truncate it).
        Duplicate inputs share a result, but duplicate receipt rows are retained.

        The production connector has a 2s connect timeout and a 2s server-side
        statement timeout: nominally up to 4s, not a 2s wall-clock deadline.
        libpq DNS/multi-host connection handling and network stalls are outside
        that wall-clock bound. Injected connectors must honor the supplied 2s
        timeout for connection and statements. No raw rows or exception text
        are retained in results or logged.
        """
        results = {ua: ReceiptResult([], "receipt_query_failed") for ua in user_agents}
        valid = [ua for ua in results if _PROBE_USER_AGENT.fullmatch(ua)]
        if not valid or len(valid) > RECEIPT_BATCH_MAX:
            return results
        if not self.database_url:
            for ua in valid:
                results[ua] = ReceiptResult([], "receipt_db_unconfigured")
            return results

        connection = None
        try:
            connection = self._connect(RECEIPT_BATCH_TIMEOUT_SECONDS)
            connection.autocommit = True
            grouped: dict[str, list[Receipt]] = {ua: [] for ua in valid}
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(self._batch_query, (valid,))
                    for row in cursor.fetchall():
                        # Unexpected keys/shapes indicate a broken query contract.
                        # Fail the whole batch rather than exposing partial success.
                        expected_columns = 11
                        if len(row) != expected_columns or row[0] not in grouped:
                            raise ValueError("invalid_receipt_batch_row")
                        account_stats_index = 7
                        if row[account_stats_index] is not None and (
                            isinstance(row[account_stats_index], bool)
                            or _safe_float(row[account_stats_index]) is None
                        ):
                            values = list(row[1:])
                            total_index = 5
                            values[total_index] = None
                            values[account_stats_index - 1] = None
                            grouped[row[0]].append(
                                self._row_to_receipt(
                                    values,
                                    include_cache_creation_tokens=True,
                                )
                            )
                        else:
                            grouped[row[0]].append(
                                self._row_to_receipt(
                                    row[1:],
                                    include_cache_creation_tokens=True,
                                )
                            )
            # Publish only after query, parsing, and context exit all succeed.
            for ua, receipts in grouped.items():
                results[ua] = ReceiptResult(
                    receipts, None if receipts else "receipt_missing"
                )
        except Exception:
            # Initialized results are already fail-closed; never expose DB text.
            pass
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
        return results

    def read(self, user_agent: str) -> ReceiptResult:
        if not self.database_url:
            return ReceiptResult([], "receipt_db_unconfigured")
        deadline = self.clock() + self.wait_seconds
        try:
            connection = self._connect(max(0.1, self.wait_seconds))
        except Exception:
            return ReceiptResult([], "receipt_query_failed")

        try:
            try:
                connection.autocommit = True
            except Exception:
                pass
            with connection:
                with connection.cursor() as cursor:
                    while True:
                        cursor.execute(self._query, (user_agent,))
                        rows = cursor.fetchall()
                        if rows:
                            expected_columns = 10
                            if any(len(row) != expected_columns for row in rows):
                                raise ValueError("invalid_receipt_row")
                            return ReceiptResult(
                                [
                                    self._row_to_receipt(
                                        row,
                                        include_cache_creation_tokens=True,
                                    )
                                    for row in rows
                                ],
                                None,
                            )
                        remaining = deadline - self.clock()
                        if remaining <= 0:
                            return ReceiptResult([], "receipt_missing")
                        self.sleeper(min(0.25, remaining))
        except Exception:
            return ReceiptResult([], "receipt_query_failed")
        finally:
            try:
                connection.close()
            except Exception:
                pass

    def read_nonbillable_many(self, user_agents: list[str], *, strict: bool = False) -> dict[str, dict[str, Any]]:
        """Read durable gateway evidence, never infer zero from missing usage.

        The restricted view exposes only requests proven not dispatched by the
        gateway. Missing view, query failure, duplicates or malformed evidence
        all return no proof by default. With strict=True they raise, so callers
        cannot mistake query failure or ambiguity for proven absence.
        This does not expose or estimate upstream charges.
        """
        def unavailable():
            if strict:
                raise ValueError("nonbillable_query_failed")
            return {}

        valid = list(dict.fromkeys(user_agents))
        if (not self.database_url or not valid or len(valid) > RECEIPT_BATCH_MAX
                or any(not _PROBE_USER_AGENT.fullmatch(ua) for ua in valid)):
            return unavailable()
        connection = None
        try:
            connection = self._connect(RECEIPT_BATCH_TIMEOUT_SECONDS)
            connection.autocommit = True
            grouped: dict[str, list[dict[str, Any]]] = {ua: [] for ua in valid}
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT user_agent, evidence_id, model, created_at, evidence_kind "
                        "FROM modeltrace_monitoring.nonbillable_probe_receipts "
                        "WHERE user_agent = ANY(%s)", (valid,),
                    )
                    for row in cursor.fetchall():
                        if len(row) != 5 or row[0] not in grouped:
                            return unavailable()
                        ua, evidence_id, model, created_at, kind = row
                        parsed_id = _safe_int(evidence_id)
                        if (parsed_id is None or parsed_id <= 0 or not isinstance(model, str)
                                or not model.strip() or created_at is None
                                or kind != "gateway_routing_not_dispatched"):
                            return unavailable()
                        grouped[ua].append({
                            "evidence_id": parsed_id, "model": model,
                            "created_at": str(created_at), "evidence_kind": kind,
                        })
            if any(len(proofs) > 1 for proofs in grouped.values()):
                return unavailable()
            return {ua: proofs[0] for ua, proofs in grouped.items() if len(proofs) == 1}
        except Exception:
            return unavailable()
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
