from __future__ import annotations

import asyncio
import codecs
import hashlib
import hmac
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any
from types import SimpleNamespace

import httpx

from .config import DEFAULT_REASONING_EFFORTS


# The envelope cap is configurable through ServiceConfig. These inner limits
# keep one pathological SSE event or output stream from consuming the whole
# envelope budget in parser state.
_MAX_SSE_EVENT_BYTES = 2 * 1024 * 1024
_MAX_SSE_DATA_LINES = 8192
_MAX_SSE_OUTPUT_DELTAS = 65536
_MIN_OUTPUT_BYTES = 64 * 1024
_OUTPUT_BYTES_PER_TOKEN = 256


class _ResponseTooLarge(Exception):
    """An inner bounded parser/output budget was exceeded."""


@dataclass(frozen=True)
class ProbeResult:
    text: str | None
    status_code: int | None
    error_code: str | None
    # True only for a deterministic request-parameter rejection. The service
    # uses this to cancel probes that have not been sent yet; it never retries
    # or treats the already-sent request as free.
    deterministic_parameter_error: bool = False
    # Timing/counter-only diagnostics. Never contains request or model text.
    transport_detail: dict[str, Any] = field(default_factory=dict)
    # Structured identity reported by the upstream, when present. This is
    # intentionally not inferred from output text and is safe for service
    # reconciliation/audit callers to inspect.
    upstream_model: str | None = None


class _ResponsesSSEParser:
    """Incrementally parse one bounded SSE envelope.

    The caller accounts every decoded byte before dispatching it here, including
    comments and ignored metadata events. Partial deltas are never themselves a
    result, so increasing the envelope budget does not make the parser accept an
    incomplete or unbounded response.
    """

    def __init__(self, transport: "ProbeTransport", status_code: int, detail: dict[str, Any], expected_model: str):
        self.transport = transport
        self.status_code = status_code
        self.detail = detail
        self.expected_model = expected_model
        self.decoder = codecs.getincrementaldecoder("utf-8-sig")("strict")
        self.buffer = ""
        self.buffer_bytes = 0
        self.data: list[str] = []
        self.event = ""
        self.event_bytes = 0
        self.data_line_count = 0
        self.text: list[str] = []
        self.output_bytes = 0
        self.output_delta_count = 0
        self.upstream_model: str | None = None

    def error(self, code: str) -> ProbeResult:
        return ProbeResult(None, self.status_code, code, upstream_model=self.upstream_model)

    def feed(self, chunk: bytes, *, final: bool = False) -> ProbeResult | None:
        try:
            decoded = self.decoder.decode(chunk, final=final)
        except UnicodeDecodeError:
            return self.error("invalid_event_stream")
        self.buffer += decoded
        self.buffer_bytes += len(decoded.encode("utf-8"))
        while match := re.search(r"\r\n|\r|\n", self.buffer):
            # A CRLF can be split between chunks. Wait for the next byte rather
            # than turn one line ending into a spurious empty-event delimiter.
            if not final and match.group() == "\r" and match.end() == len(self.buffer):
                break
            delimiter = match.group()
            line, self.buffer = self.buffer[:match.start()], self.buffer[match.end():]
            line_bytes = len(line.encode("utf-8")) + len(delimiter)
            self.buffer_bytes -= line_bytes
            self.event_bytes += line_bytes
            if self.event_bytes > self.transport.max_event_bytes:
                return self.error("response_too_large")
            if not line:
                result = self.dispatch()
                if result is not None:
                    return result
            elif not line.startswith(":"):
                name, sep, value = line.partition(":")
                if sep and value.startswith(" "):
                    value = value[1:]
                if name == "data":
                    self.data_line_count += 1
                    if self.data_line_count > _MAX_SSE_DATA_LINES:
                        return self.error("response_too_large")
                    self.data.append(value)
                elif name == "event":
                    self.event = value
        # A chunk may end in the middle of a line. Count that partial line too,
        # otherwise a peer could grow parser state beyond the per-event bound
        # without sending a delimiter.
        if self.event_bytes + self.buffer_bytes > self.transport.max_event_bytes:
            return self.error("response_too_large")
        return None

    def dispatch(self) -> ProbeResult | None:
        data, event_name = "\n".join(self.data), self.event
        self.data, self.event = [], ""
        self.event_bytes = 0
        self.data_line_count = 0
        if not data:
            return None
        self.detail["event_count"] += 1
        if self.detail["first_event_ms"] is None:
            self.detail["first_event_ms"] = self.transport._elapsed_ms()
        if data.strip() == "[DONE]":
            # A marker is not proof of a completed Responses envelope.
            return self.error("response_truncated")
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            return self.error("invalid_event_stream")
        if not isinstance(event, dict):
            return self.error("invalid_event_stream")
        if not self.transport._observe_model(event, self):
            return self.error("upstream_model_mismatch")
        event_type = event.get("type") or event_name
        if event_name and event.get("type") and event_name != event["type"]:
            return self.error("invalid_event_stream")
        if event_type in {"error", "response.failed", "response.incomplete", "response.completed"}:
            self.detail["termination_event_received"] = True
        if event_type in {"error", "response.failed"}:
            return self.error("upstream_response_failed")
        if event_type == "response.incomplete":
            resp = event.get("response")
            if isinstance(resp, dict):
                self.detail["status"] = resp.get("status")
                inc = resp.get("incomplete_details")
                if isinstance(inc, dict):
                    self.detail["incomplete_reason"] = inc.get("reason")
            return self.error("response_truncated")
        if event_type == "response.output_text.delta":
            delta = event.get("delta")
            if not isinstance(delta, str):
                return self.error("invalid_event_stream")
            self.output_delta_count += 1
            if self.output_delta_count > _MAX_SSE_OUTPUT_DELTAS:
                return self.error("response_too_large")
            delta_bytes = len(delta.encode("utf-8"))
            if self.output_bytes + delta_bytes > self.transport.max_output_bytes:
                return self.error("response_too_large")
            self.output_bytes += delta_bytes
            self.text.append(delta)
            self.detail["output_bytes"] = self.output_bytes
            self.detail["output_delta_count"] = self.output_delta_count
            if self.detail["first_text_ms"] is None:
                self.detail["first_text_ms"] = self.transport._elapsed_ms()
        elif event_type == "response.completed":
            response = event.get("response")
            if isinstance(response, dict):
                self.detail["status"] = response.get("status")
                inc_details = response.get("incomplete_details")
                if isinstance(inc_details, dict):
                    self.detail["incomplete_reason"] = inc_details.get("reason")
            if not isinstance(response, dict) or response.get("status") != "completed":
                return self.error("response_truncated")
            usage = response.get("usage") or event.get("usage")
            if isinstance(usage, dict):
                self.detail["usage"] = usage
                details = usage.get("output_tokens_details") or {}
                if isinstance(details, dict):
                    self.detail["reasoning_tokens"] = details.get("reasoning_tokens")
                self.detail["output_tokens"] = usage.get("output_tokens")
            error = self.transport._completion_error(response)
            if error:
                return self.error(error)
            # The terminal snapshot is authoritative, not another delta. A
            # compatible terminal may omit the output snapshot, in which case
            # verified completion allows the already received deltas.
            text = self.transport._extract_output_text(response)
            if text is None and "output" not in response and "output_text" not in response:
                text = "".join(self.text)
            try:
                text = self.transport._bounded_output_text(text)
            except _ResponseTooLarge:
                return self.error("response_too_large")
            if not text:
                return self.error("missing_output_text")
            self.detail["termination_event_received"] = True
            return ProbeResult(text, self.status_code, None, upstream_model=self.upstream_model)
        return None


class ProbeTransport:
    """Synchronous worker entry point with a cancellable request-wide deadline.

    ``max_response_bytes`` is a finite decoded response-envelope budget. For an
    SSE response it covers the complete wire envelope, not just output text:
    comments, metadata events, data lines, and terminal output all count. The
    parser and collected output therefore remain bounded even with the larger
    envelope needed by Responses metadata.

    ``client_factory``, when supplied for tests, must return an AsyncClient-like
    context manager. Network I/O stays on the request's event loop; timing out
    never leaves a paid request running in an abandoned worker thread.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        api_key: str,
        max_output_tokens: int | None = None,
        timeout_seconds: int | float | None = 120,
        max_response_bytes: int = 8 * 1024 * 1024,
        max_prompt_bytes: int = 65536,
        client_factory: Any | None = None,
        probe_secret: str | None = None,
        reasoning_effort_overrides: dict[str, str] | None = None,
        model_aliases: dict[str, str] | None = None,
        idle_timeout_seconds: int | float | None = None,
        total_timeout_seconds: int | float | None = None,
    ):
        self.endpoint = endpoint
        self.api_key = api_key
        self.max_output_tokens = max_output_tokens
        self.timeout_seconds = timeout_seconds
        self.idle_timeout_seconds = idle_timeout_seconds if idle_timeout_seconds is not None else (timeout_seconds or 600)
        # Wall-clock cap for one probe. None (default) = no cap: a slow model is
        # allowed to finish its answer; only a silent stream is cut, by the idle
        # timeout.
        self.total_timeout_seconds = total_timeout_seconds
        self.max_response_bytes = max_response_bytes
        self.max_event_bytes = min(max_response_bytes, _MAX_SSE_EVENT_BYTES)
        self.max_prompt_bytes = max_prompt_bytes
        effective_tokens = max_output_tokens if max_output_tokens is not None else 4096
        self.max_output_bytes = min(
            max_response_bytes,
            max(_MIN_OUTPUT_BYTES, effective_tokens * _OUTPUT_BYTES_PER_TOKEN),
        )
        self.client_factory = client_factory or httpx.AsyncClient
        self.probe_secret = probe_secret
        self.reasoning_effort_overrides = dict(reasoning_effort_overrides or {})
        self.model_aliases = dict(model_aliases or {})
        self._request_started_at = 0.0
        self._request_detail: dict[str, Any] = {}

    def _elapsed_ms(self) -> int:
        return max(0, round((time.monotonic() - self._request_started_at) * 1000))

    def _payload(self, model: str, challenge: dict[str, Any]) -> dict[str, Any]:
        system = str(challenge.get("system") or "")
        user_prefix = str(challenge.get("user_prefix") or "")
        prompt = user_prefix + str(challenge.get("prompt") or "")
        prompt_bytes = len(prompt.encode("utf-8")) + len(system.encode("utf-8"))
        if prompt_bytes > self.max_prompt_bytes:
            raise ValueError("prompt_too_large")

        messages: list[dict[str, Any]] = []
        if system:
            messages.append(
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system}],
                }
            )
        messages.append(
            {
                "role": "user",
                "content": [{"type": "input_text", "text": prompt}],
            }
        )
        payload = {
            "model": model,
            "input": messages,
            "reasoning": {
                "effort": self.reasoning_effort_overrides.get(
                    model, self.reasoning_effort_overrides.get(self.model_aliases.get(model, model), DEFAULT_REASONING_EFFORTS.get(self.model_aliases.get(model, model), "none"))
                )
            },
            "service_tier": "default",
            "stream": True,
            "store": False,
        }
        if self.max_output_tokens is not None:
            payload["max_output_tokens"] = self.max_output_tokens
        return payload

    @staticmethod
    def _extract_output_text(payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        direct = payload.get("output_text")
        if isinstance(direct, str) and direct:
            return direct
        output = payload.get("output")
        if not isinstance(output, list):
            return None
        pieces: list[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for part in content:
                if not isinstance(part, dict):
                    continue
                if part.get("type") != "output_text":
                    continue
                text = part.get("text")
                if isinstance(text, str):
                    pieces.append(text)
        result = "".join(pieces)
        return result or None

    def _bounded_output_text(self, text: str | None) -> str | None:
        if text is not None and len(text.encode("utf-8")) > self.max_output_bytes:
            raise _ResponseTooLarge
        return text

    def _observe_model(self, payload: Any, observer: Any) -> bool:
        """Record only structured Responses model fields.

        Model identity is deliberately restricted to the protocol's model
        fields. In particular, output_text and delta text are never searched
        for model-looking strings. Missing model fields remain compatible with
        older/partial upstream envelopes; a present non-string or any exact
        disagreement fails closed.
        """
        if not isinstance(payload, dict):
            return True
        candidates: list[Any] = []
        if "model" in payload:
            candidates.append(payload["model"])
        response = payload.get("response")
        if isinstance(response, dict) and "model" in response:
            candidates.append(response["model"])
        for candidate in candidates:
            if not isinstance(candidate, str) or not candidate:
                return False
            if observer.upstream_model is None:
                observer.upstream_model = candidate
            elif observer.upstream_model != candidate:
                return False
            if candidate not in {observer.expected_model, self.model_aliases.get(observer.expected_model)}:
                return False
        return True

    @staticmethod
    def _completion_error(payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        # HTTP 200 and enough numbers do not prove the Responses request
        # completed. Never expose partial text from an explicit failure or a
        # non-terminal envelope to the classifier. Missing status is retained
        # for compatible JSON upstreams; the service still checks all answers.
        if payload.get("error") is not None or payload.get("status") == "failed":
            return "upstream_response_failed"
        if payload.get("status") not in (None, "completed") or payload.get("incomplete_details") is not None:
            return "response_truncated"
        output = payload.get("output")
        if isinstance(output, list):
            for item in output:
                if isinstance(item, dict) and item.get("status") not in (None, "completed"):
                    return "response_truncated"
        return None

    def run(
        self, *, model: str, challenge: dict[str, Any], user_agent: str,
        session_affinity: str | None = None,
        account_id: int | None = None,
    ) -> ProbeResult:
        if session_affinity is not None and (
            not isinstance(session_affinity, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", session_affinity)
        ):
            return ProbeResult(None, None, "invalid_session_affinity")
        try:
            payload = self._payload(model, challenge)
        except ValueError as exc:
            return ProbeResult(None, None, str(exc))

        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream, application/json",
            "User-Agent": user_agent,
        }
        if session_affinity is not None:
            headers["X-Session-Affinity"] = session_affinity
        if account_id is not None and self.probe_secret:
            headers["X-ModelTrace-Probe"] = self.probe_secret
            headers["X-ModelTrace-Account"] = str(account_id)
        if self.probe_secret:
            # This internal proof only enables stricter lifecycle limits. The
            # gateway verifies an administrator key and removes the header
            # before upstream forwarding. Never transmit the shared secret.
            headers["X-Sub2API-ModelTrace-Signature"] = hmac.new(
                self.probe_secret.encode("utf-8"), user_agent.encode("utf-8"), hashlib.sha256
            ).hexdigest()
        # The service calls this synchronous method from its worker thread.
        # Cancellation closes the live response/client, rather than abandoning
        # a paid request in a thread. Do not use asyncio.run here: its executor
        # shutdown waits for uncancellable OS DNS lookups even after timeout.
        # Closing our loop does not wait for that resolver thread; the cancelled
        # request cannot resume/connect when the lookup eventually finishes.
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(self._request(body, headers, model))
            result.transport_detail.update(self._request_detail)
            return result
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()

    async def _request(self, body: bytes, headers: dict[str, str], model: str) -> ProbeResult:
        self._request_started_at = time.monotonic()
        self._request_detail = {
            "http_headers_ms": None,
            "first_event_ms": None,
            "first_text_ms": None,
            "received_bytes": 0,
            "event_count": 0,
            "output_bytes": 0,
            "output_delta_count": 0,
            "termination_event_received": False,
        }
        timeout = httpx.Timeout(
            connect=self.timeout_seconds,
            read=self.idle_timeout_seconds,
            write=self.timeout_seconds,
            pool=self.timeout_seconds,
        )
        status_code: int | None = None
        parser: _ResponsesSSEParser | None = None
        try:
            # HTTPX timeouts are per I/O phase (read resets on every chunk),
            # not a wall-clock bound. An optional single deadline covers headers,
            # all chunks and all phases, without retries or a second request.
            async with asyncio.timeout(self.total_timeout_seconds):
                async with self.client_factory(
                    follow_redirects=False,
                    timeout=timeout,
                    limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
                ) as client:
                    async with client.stream("POST", self.endpoint, content=body, headers=headers) as response:
                        status_code = int(response.status_code)
                        self._request_detail["http_headers_ms"] = self._elapsed_ms()
                        if 300 <= status_code < 400:
                            return ProbeResult(None, status_code, "redirect_blocked")
                        if status_code < 200 or status_code >= 300:
                            if status_code == 400 and response.headers.get("content-type", "").split(";", 1)[0].strip().lower() == "application/json":
                                # Read only a bounded error envelope so a known
                                # unsupported reasoning effort can short-circuit
                                # the remaining planned probes. Do not use
                                # ``aread`` here: it buffers the entire error
                                # body before a size check and lets a hostile
                                # 400 bypass the envelope cap.
                                try:
                                    error_limit = min(self.max_response_bytes, 256 * 1024)
                                    declared = response.headers.get("content-length")
                                    if declared is not None:
                                        try:
                                            if int(declared) > error_limit:
                                                return ProbeResult(None, status_code, "response_too_large")
                                        except ValueError:
                                            pass
                                    raw_error = bytearray()
                                    async for chunk in response.aiter_bytes():
                                        await asyncio.sleep(0)
                                        self._request_detail["received_bytes"] += len(chunk)
                                        if len(raw_error) + len(chunk) > error_limit:
                                            return ProbeResult(None, status_code, "response_too_large")
                                        raw_error.extend(chunk)
                                    payload_error = json.loads(bytes(raw_error).decode("utf-8"))
                                    error = payload_error.get("error") if isinstance(payload_error, dict) else None
                                    if isinstance(error, dict) and (
                                        error.get("code") == "unsupported_value"
                                        and error.get("param") == "reasoning.effort"
                                    ):
                                        return ProbeResult(
                                            None, status_code,
                                            "upstream_unsupported_reasoning_effort",
                                            True,
                                        )
                                except (UnicodeDecodeError, json.JSONDecodeError, TypeError, AttributeError):
                                    pass
                            if status_code in {401, 403}:
                                error_code = "upstream_auth"
                            elif status_code in {408, 429}:
                                error_code = "upstream_rate_limited"
                            elif status_code >= 500:
                                error_code = "upstream_5xx"
                                if status_code == 503 and (
                                    "application/json" in response.headers.get("content-type", "").lower()
                                    or "text/" in response.headers.get("content-type", "").lower()
                                ):
                                    try:
                                        err_bytes = bytearray()
                                        stream_iter = response.aiter_bytes().__aiter__()
                                        while len(err_bytes) < 65536:
                                            try:
                                                c = await asyncio.wait_for(stream_iter.__anext__(), timeout=0.5)
                                                err_bytes.extend(c)
                                            except (StopAsyncIteration, TimeoutError, asyncio.TimeoutError):
                                                break
                                        err_lower = bytes(err_bytes).lower()
                                        if b"probe_account_unavailable" in err_lower or b'"code":"probe_account_unavailable"' in err_lower.replace(b" ", b""):
                                            error_code = "account_unavailable"
                                        elif b"no available" in err_lower:
                                            error_code = "upstream_no_available_account"
                                    except Exception:
                                        pass
                            else:
                                error_code = "upstream_http_error"
                            return ProbeResult(None, status_code, error_code)
                        content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                        is_sse = content_type == "text/event-stream"
                        parser = _ResponsesSSEParser(self, status_code, self._request_detail, model) if is_sse else None
                        content_length = response.headers.get("content-length")
                        if content_length is not None:
                            try:
                                if int(content_length) > self.max_response_bytes:
                                    return ProbeResult(None, status_code, "response_too_large")
                            except ValueError:
                                pass
                        chunks = bytearray()
                        total = 0
                        stream_iter = response.aiter_bytes().__aiter__()
                        while True:
                            try:
                                chunk = await asyncio.wait_for(stream_iter.__anext__(), timeout=self.idle_timeout_seconds)
                            except StopAsyncIteration:
                                break
                            except (TimeoutError, asyncio.TimeoutError):
                                return ProbeResult(None, status_code, "upstream_timeout")
                            await asyncio.sleep(0)
                            total += len(chunk)
                            self._request_detail["received_bytes"] = total
                            if total > self.max_response_bytes:
                                return ProbeResult(None, status_code, "response_too_large")
                            if parser is not None:
                                terminal = parser.feed(chunk)
                                if terminal is not None:
                                    return terminal
                            else:
                                chunks.extend(chunk)
                        if parser is not None:
                            terminal = parser.feed(b"", final=True)
                            return terminal or ProbeResult(None, status_code, "response_truncated")
                        raw = bytes(chunks)
        except httpx.TooManyRedirects:
            return ProbeResult(None, status_code, "redirect_blocked")
        except (TimeoutError, httpx.TimeoutException):
            return ProbeResult(None, status_code, "upstream_timeout")
        except httpx.RequestError:
            return ProbeResult(None, status_code, "upstream_network_error")
        except ValueError:
            return ProbeResult(None, status_code, "transport_error")
        finally:
            if parser is not None:
                # Diagnostic only: never classify a partial stream or retain
                # its content. The parser already bounds accumulated output.
                self._request_detail["partial_numeric_count"] = sum(
                    1 for match in re.finditer(r"(?<![0-9])[0-9]{1,3}(?![0-9])", "".join(parser.text))
                    if 1 <= int(match.group()) <= 355
                )

        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return ProbeResult(None, status_code, "invalid_json")
        observer = SimpleNamespace(expected_model=model, upstream_model=None)
        if not self._observe_model(parsed, observer):
            return ProbeResult(None, status_code, "upstream_model_mismatch", upstream_model=observer.upstream_model)
        usage = parsed.get("usage") if isinstance(parsed, dict) else None
        if isinstance(usage, dict):
            self._request_detail["usage"] = usage
            details = usage.get("output_tokens_details") or {}
            if isinstance(details, dict):
                self._request_detail["reasoning_tokens"] = details.get("reasoning_tokens")
            self._request_detail["output_tokens"] = usage.get("output_tokens")
        if isinstance(parsed, dict):
            self._request_detail["status"] = parsed.get("status")
            inc_details = parsed.get("incomplete_details")
            if isinstance(inc_details, dict):
                self._request_detail["incomplete_reason"] = inc_details.get("reason")
            self._request_detail["termination_event_received"] = True

        completion_error = self._completion_error(parsed)
        if completion_error:
            return ProbeResult(None, status_code, completion_error)
        text = self._extract_output_text(parsed)
        try:
            text = self._bounded_output_text(text)
        except _ResponseTooLarge:
            return ProbeResult(None, status_code, "response_too_large")
        if not text:
            return ProbeResult(None, status_code, "missing_output_text")
        return ProbeResult(text, status_code, None, upstream_model=observer.upstream_model)
