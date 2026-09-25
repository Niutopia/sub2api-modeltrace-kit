from __future__ import annotations

import asyncio
import gzip
import inspect
import json
import socket
import socketserver
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

from modeltrace.transport import ProbeTransport

from .conftest import FakeReceipts, build_service, run_one


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks, *, delay=0, error=None):
        self.chunks = chunks
        self.delay = delay
        self.error = error
        self.closed = False
        self.read_count = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            if self.delay:
                await asyncio.sleep(self.delay)
            self.read_count += 1
            yield chunk
        if self.error:
            raise self.error

    async def aclose(self):
        self.closed = True


def completed(text="1 2 3"):
    return {
        "status": "completed",
        "output": [{"type": "message", "status": "completed", "content": [{"type": "output_text", "text": text}]}],
    }


def make_transport(response=None, *, handler=None, **overrides):
    requests = []
    clients = []
    options = []

    async def receive(request):
        requests.append(request)
        result = handler(request) if handler else response
        if inspect.isawaitable(result):
            result = await result
        return result if result is not None else httpx.Response(200, json=completed())

    def factory(**kwargs):
        options.append(kwargs)
        client = httpx.AsyncClient(transport=httpx.MockTransport(receive), trust_env=False, **kwargs)
        clients.append(client)
        return client

    config = dict(
        endpoint="https://api.example/v1/responses",
        api_key="dummy-test-key",
        max_output_tokens=2048,
        timeout_seconds=120,
        max_response_bytes=4096,
        max_prompt_bytes=65536,
        client_factory=factory,
    )
    config.update(overrides)
    return SimpleNamespace(transport=ProbeTransport(**config), requests=requests, clients=clients, options=options)


def run_probe(transport, *, challenge=None):
    return transport.run(
        model="gpt-5.4",
        challenge=challenge or {"prompt": "give numbers", "expected_count": 3},
        user_agent="ModelTraceProbe/local-test",
    )


def test_responses_contract_is_explicit():
    h = make_transport()
    result = run_probe(h.transport, challenge={"system": "system", "user_prefix": "prefix:", "prompt": "数字"})
    assert result.error_code is None
    assert result.text == "1 2 3"
    assert result.status_code == 200
    assert len(h.requests) == 1
    request = h.requests[0]
    payload = json.loads(request.content)
    assert request.method == "POST"
    assert str(request.url).endswith("/responses")
    assert payload["stream"] is True
    assert payload["store"] is False
    assert payload["reasoning"] == {"effort": "none"}
    assert payload["service_tier"] == "default"
    assert payload["max_output_tokens"] == 2048
    assert payload["input"] == [
        {"role": "system", "content": [{"type": "input_text", "text": "system"}]},
        {"role": "user", "content": [{"type": "input_text", "text": "prefix:数字"}]},
    ]
    assert request.headers["User-Agent"].startswith("ModelTraceProbe/")
    assert request.headers["Authorization"] == "Bearer dummy-test-key"
    assert request.headers["Accept"] == "text/event-stream, application/json"
    assert h.options[0]["follow_redirects"] is False
    assert h.options[0]["timeout"].as_dict() == dict(connect=120, read=120, write=120, pool=120)
    assert h.options[0]["limits"].max_connections == 1
    assert h.options[0]["limits"].max_keepalive_connections == 0
    assert h.clients[0].is_closed


@pytest.mark.parametrize("model", ["gpt-5.4", None])
def test_json_structured_model_identity_is_exact_and_missing_is_compatible(model):
    payload = completed()
    if model is not None:
        payload["model"] = model
    result = run_probe(make_transport(httpx.Response(200, json=payload)).transport)
    assert result.error_code is None
    assert result.text == "1 2 3"
    assert result.upstream_model == model


def test_json_cross_model_response_fails_closed_without_using_pseudo_model_text():
    payload = completed("the requested model gpt-5.4 says: 1 2 3")
    payload["model"] = "gpt-5.6-luna"
    result = run_probe(make_transport(httpx.Response(200, json=payload)).transport)
    assert result.error_code == "upstream_model_mismatch"
    assert result.text is None
    assert result.upstream_model == "gpt-5.6-luna"


def test_json_nested_response_model_is_checked_without_prefix_matching():
    payload = {"status": "completed", "response": {"model": "gpt-5.40", "output_text": "1 2 3"}}
    result = run_probe(make_transport(httpx.Response(200, json=payload)).transport)
    assert result.error_code == "upstream_model_mismatch"
    assert result.text is None


def test_true_astra_structured_identity_is_accepted_exactly():
    payload = completed()
    payload["model"] = "gpt-6-astra"
    h = make_transport(httpx.Response(200, json=payload))
    result = h.transport.run(
        model="gpt-6-astra",
        challenge={"prompt": "give numbers", "expected_count": 3},
        user_agent="ModelTraceProbe/local-test",
    )
    assert result.error_code is None
    assert result.text == "1 2 3"
    assert result.upstream_model == "gpt-6-astra"


@pytest.mark.parametrize("limit, error", [(8, "prompt_too_large"), (9, None)])
def test_prompt_limit_counts_utf8_system_and_prefix(limit, error):
    h = make_transport(max_prompt_bytes=limit)
    result = run_probe(h.transport, challenge={"system": "中", "user_prefix": "文", "prompt": "字"})
    assert result.error_code == error
    assert len(h.requests) == (0 if error else 1)


@pytest.mark.parametrize(
    "status, error",
    [(301, "redirect_blocked"), (302, "redirect_blocked"), (307, "redirect_blocked"),
     (400, "upstream_http_error"), (401, "upstream_auth"), (403, "upstream_auth"),
     (408, "upstream_rate_limited"), (429, "upstream_rate_limited"),
     (500, "upstream_5xx"), (503, "upstream_5xx")],
)
def test_http_errors_do_not_read_body_or_retry(status, error):
    stream = ChunkStream([b"private upstream body"])
    h = make_transport(httpx.Response(status, stream=stream, headers={"location": "https://other.example"}))
    result = run_probe(h.transport)
    assert result.error_code == error
    assert result.status_code == status
    assert result.text is None
    assert len(h.requests) == 1
    assert stream.read_count == 0
    assert stream.closed and h.clients[0].is_closed


@pytest.mark.parametrize("declared", [None, "100", "invalid"])
def test_response_size_limit_is_enforced_for_headers_and_chunks(declared):
    stream = ChunkStream([b"12", b"345"])
    headers = {} if declared is None else {"content-length": declared}
    h = make_transport(httpx.Response(200, stream=stream, headers=headers), max_response_bytes=4)
    result = run_probe(h.transport)
    assert result.error_code == "response_too_large"
    assert result.text is None
    assert stream.read_count == (0 if declared == "100" else 2)
    assert stream.closed


def test_exact_byte_limit_and_split_utf8_are_accepted():
    body = json.dumps({"output_text": "你好 1 2 3"}, ensure_ascii=False).encode()
    stream = ChunkStream([bytes([b]) for b in body])
    h = make_transport(httpx.Response(200, stream=stream), max_response_bytes=len(body))
    result = run_probe(h.transport)
    assert result.error_code is None
    assert result.text == "你好 1 2 3"
    assert stream.closed


def test_decompressed_body_is_size_limited():
    body = gzip.compress(json.dumps(completed("1 " * 2000)).encode())
    stream = ChunkStream([body])
    h = make_transport(httpx.Response(200, stream=stream, headers={"content-encoding": "gzip"}), max_response_bytes=256)
    assert len(body) < 256
    assert run_probe(h.transport).error_code == "response_too_large"
    assert stream.closed


@pytest.mark.parametrize("body", [b"not-json", b"", b"\xff", b'{"output_text":"partial'])
def test_invalid_or_cut_off_json_fails_closed(body):
    result = run_probe(make_transport(httpx.Response(200, content=body)).transport)
    assert result.error_code == "invalid_json"
    assert result.text is None


@pytest.mark.parametrize("payload", [None, [], {}, {"output_text": ""}, {"output": [None, {}, {"content": [None]}]}])
def test_missing_text_fails_closed(payload):
    h = make_transport(httpx.Response(200, content=json.dumps(payload).encode()))
    result = run_probe(h.transport)
    assert result.error_code == "missing_output_text"
    assert result.text is None


def test_output_text_parts_are_not_lost():
    payload = completed()
    payload["output"][0]["content"] = [
        {"type": "output_text", "text": "1 2 "},
        {"type": "refusal", "refusal": "not output text"},
        {"type": "output_text", "text": "3"},
    ]
    assert run_probe(make_transport(httpx.Response(200, json=payload)).transport).text == "1 2 3"


@pytest.mark.parametrize("status", ["incomplete", "in_progress", "queued", "cancelled", "canceled", "unknown"])
def test_nonterminal_or_incomplete_status_never_returns_partial_text(status):
    h = make_transport(httpx.Response(200, json={"status": status, "output_text": "1 2 3"}))
    result = run_probe(h.transport)
    assert result.error_code == "response_truncated"
    assert result.text is None


@pytest.mark.parametrize(
    "extra",
    [{"status": "failed"}, {"status": "completed", "error": {"message": "private upstream error"}}],
)
def test_failed_envelope_never_returns_text_or_private_error(extra):
    h = make_transport(httpx.Response(200, json={"output_text": "1 2 3", **extra}))
    result = run_probe(h.transport)
    assert result.error_code == "upstream_response_failed"
    assert result.text is None
    assert "private" not in repr(result)


@pytest.mark.parametrize("location", ["incomplete_details", "output_status"])
def test_incomplete_details_and_output_items_are_checked_even_with_direct_text(location):
    payload = {**completed(), "output_text": "1 2 3"}
    if location == "incomplete_details":
        payload[location] = {"reason": "max_output_tokens"}
    else:
        payload["output"][0]["status"] = "incomplete"
    result = run_probe(make_transport(httpx.Response(200, json=payload)).transport)
    assert result.error_code == "response_truncated"
    assert result.text is None


def test_sse_partial_deltas_are_not_scored_or_retried():
    stream = ChunkStream([b'data: {"type":"response.output_text.delta","delta":"1 2 3"}\n\n'])
    h = make_transport(httpx.Response(200, stream=stream, headers={"content-type": "text/event-stream; charset=utf-8"}))
    result = run_probe(h.transport)
    assert result.error_code == "response_truncated"
    assert result.text is None
    assert len(h.requests) == 1
    assert stream.read_count == 1 and stream.closed


@pytest.mark.parametrize(
    "exc, error",
    [(httpx.ConnectTimeout, "upstream_timeout"), (httpx.ReadTimeout, "upstream_timeout"),
     (httpx.WriteTimeout, "upstream_timeout"), (httpx.PoolTimeout, "upstream_timeout"),
     (httpx.ConnectError, "upstream_network_error"), (httpx.RemoteProtocolError, "upstream_network_error"),
     (httpx.TooManyRedirects, "redirect_blocked"), (ValueError, "transport_error")],
)
def test_transport_exceptions_are_sanitized_and_never_retried(exc, error):
    def fail(request):
        raise exc("private request details")

    h = make_transport(handler=fail)
    result = run_probe(h.transport)
    assert result.error_code == error
    assert result.status_code is None and result.text is None
    assert "private" not in repr(result)
    assert len(h.requests) == 1 and h.clients[0].is_closed


@pytest.mark.parametrize("exc", [httpx.ReadTimeout, httpx.ReadError, httpx.RemoteProtocolError])
def test_failure_during_body_retains_known_http_status_and_discards_partial_text(exc):
    stream = ChunkStream([json.dumps(completed()).encode()], error=exc("private details"))
    h = make_transport(httpx.Response(200, stream=stream))
    result = run_probe(h.transport)
    assert result.status_code == 200 and result.text is None
    assert result.error_code == ("upstream_timeout" if exc is httpx.ReadTimeout else "upstream_network_error")
    assert stream.closed and h.clients[0].is_closed


def test_deadline_cancels_waiting_for_headers_without_leaving_request_running():
    cancelled = []

    async def wait_for_headers(request):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    h = make_transport(handler=wait_for_headers, timeout_seconds=0.05, total_timeout_seconds=0.05)
    start = time.monotonic()
    result = run_probe(h.transport)
    assert time.monotonic() - start < 0.5
    assert result.error_code == "upstream_timeout" and result.status_code is None
    assert cancelled == [True]
    assert len(h.requests) == 1 and h.clients[0].is_closed


def test_deadline_does_not_wait_for_os_dns_or_send_after_it_returns(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    original_getaddrinfo = socket.getaddrinfo

    def blocked_dns(*args, **kwargs):
        entered.set()
        try:
            release.wait(timeout=2)
            return original_getaddrinfo("127.0.0.1", port, type=socket.SOCK_STREAM)
        finally:
            finished.set()

    try:
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
    except OSError:
        pytest.skip("socket bind not permitted in restricted sandbox")
    with listener:
        listener.listen()
        listener.settimeout(0.1)
        port = listener.getsockname()[1]
        monkeypatch.setattr(socket, "getaddrinfo", blocked_dns)
        h = make_transport(
            endpoint=f"http://local-dns-fixture.invalid:{port}/responses",
            timeout_seconds=0.1, total_timeout_seconds=0.1,
            client_factory=lambda **kw: httpx.AsyncClient(trust_env=False, **kw),
        )
        try:
            start = time.monotonic()
            result = run_probe(h.transport)
            elapsed = time.monotonic() - start
            assert entered.is_set()
            assert elapsed < 0.6
            assert result.error_code == "upstream_timeout" and result.status_code is None
            assert not finished.is_set(), "caller waited for OS resolver after its deadline"
        finally:
            release.set()
            assert finished.wait(1)
        # Resolving after cancellation must never dispatch the probe later.
        with pytest.raises(TimeoutError):
            listener.accept()


def test_one_deadline_is_shared_by_headers_and_body():
    stream = ChunkStream([json.dumps(completed()).encode()], delay=0.08)

    async def slow_headers(request):
        await asyncio.sleep(0.08)
        return httpx.Response(200, stream=stream)

    h = make_transport(handler=slow_headers, timeout_seconds=0.12, total_timeout_seconds=0.12)
    result = run_probe(h.transport)
    assert result.error_code == "upstream_timeout" and result.status_code == 200
    assert result.text is None
    assert stream.closed and h.clients[0].is_closed
    assert len(h.requests) == 1


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_real_socket_trickle_cannot_extend_overall_deadline(phase):
    # Local-only regression: every byte arrives faster than the HTTPX read
    # timeout, but the full response exceeds the one request-wide deadline.
    disconnected = threading.Event()
    requests = []
    body = json.dumps(completed()).encode()

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(1)
            try:
                request = b""
                while b"\r\n\r\n" not in request:
                    chunk = self.request.recv(65536)
                    if not chunk:
                        return
                    request += chunk
                requests.append(True)
                if phase == "headers":
                    self.request.sendall(b"HTTP/1.1 200 OK\r\nX-Trickle: ")
                    data = b"x" * 100
                else:
                    self.request.sendall(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                        + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n"
                    )
                    data = body
                for byte in data:
                    self.request.sendall(bytes([byte]))
                    time.sleep(0.01)
            except OSError:
                disconnected.set()

    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True

    try:
        server = Server(("127.0.0.1", 0), Handler)
    except OSError:
        pytest.skip("socket bind not permitted in restricted sandbox")
    with server:
        worker = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
        worker.start()
        try:
            h = make_transport(
                endpoint=f"http://127.0.0.1:{server.server_address[1]}/responses",
                timeout_seconds=0.15, total_timeout_seconds=0.15,
                client_factory=lambda **kw: httpx.AsyncClient(trust_env=False, **kw),
            )
            start = time.monotonic()
            result = run_probe(h.transport)
            elapsed = time.monotonic() - start
            assert result.error_code == "upstream_timeout"
            assert result.text is None
            assert result.status_code == (None if phase == "headers" else 200)
            assert elapsed < 0.6
            assert requests == [True]
            assert disconnected.wait(1), "timed-out request socket was not closed"
        finally:
            server.shutdown()
            worker.join(timeout=1)


@pytest.mark.parametrize("status", ["incomplete", "in_progress", "failed"])
def test_sufficient_numbers_in_incomplete_envelope_still_cannot_score(tmp_path, fake_clock, status):
    h = make_transport(handler=lambda _: httpx.Response(200, json={"status": status, "output_text": "1 " * 400}))
    service, _, _ = build_service(tmp_path, fake_clock, transport=h.transport)
    run_one(service)
    snapshot = service.snapshot(1, admin=True)
    assert snapshot["latest"]["status"] == "error"
    assert snapshot["latest"]["target_probability"] is None
    assert snapshot["diagnostics"]["cost_controls_enabled"] is False
    assert "last_receipt_consistent" not in snapshot["diagnostics"]
    assert len(h.requests) == 1


def test_short_completed_answers_still_cannot_score(tmp_path, fake_clock):
    h = make_transport()
    service, _, _ = build_service(tmp_path, fake_clock, transport=h.transport)
    run_one(service)
    snapshot = service.snapshot(1, admin=True)
    assert snapshot["latest"]["message_code"] == "insufficient_sample"
    assert snapshot["latest"]["target_probability"] is None
    assert "last_receipt_consistent" not in snapshot["diagnostics"]
    assert len(h.requests) == 1


@pytest.mark.parametrize("case", ["missing", "duplicate", "mixed_account", "wrong_model", "wrong_tier"])
def test_complete_answers_classify_without_retired_receipt_system(tmp_path, fake_clock, case):
    receipts = {
        "missing": lambda: FakeReceipts(row_counts=[1, 1, 0]),
        "duplicate": lambda: FakeReceipts(row_counts=[1, 2, 1]),
        "mixed_account": lambda: FakeReceipts(account_ids=["acct-1", "acct-2", "acct-1"]),
        "wrong_model": lambda: FakeReceipts(models=["gpt-5.4", "other-model", "gpt-5.4"]),
        "wrong_tier": lambda: FakeReceipts(service_tiers=["default", "flex", "default"]),
    }[case]()
    h = make_transport(handler=lambda _: httpx.Response(200, json=completed("1 " * 400)))
    service, _, _ = build_service(tmp_path, fake_clock, transport=h.transport, receipts=receipts)
    run_one(service)
    snapshot = service.snapshot(1, admin=True)
    assert snapshot["latest"]["status"] in {"match", "uncertain", "suspect"}
    assert snapshot["latest"]["target_probability"] is not None
    assert snapshot["diagnostics"]["cost_controls_enabled"] is False
    assert receipts.calls == []
    assert len(h.requests) == 1


def sse(event):
    return ("data: " + json.dumps(event, ensure_ascii=False) + "\n\n").encode()


def sse_response(stream):
    return httpx.Response(200, stream=stream, headers={"content-type": "text/event-stream; charset=utf-8"})


def test_sse_stops_at_completed_without_waiting_for_eof_or_duplicating_snapshot():
    stream = ChunkStream([
        sse({"type": "response.output_text.delta", "delta": "1 2 3"}),
        sse({"type": "response.completed", "response": completed()}),
        b"must not read this tail",
    ], error=AssertionError("must not wait for EOF"))
    h = make_transport(sse_response(stream))
    result = run_probe(h.transport)
    assert result.text == "1 2 3" and result.error_code is None
    assert stream.read_count == 2 and stream.closed and h.clients[0].is_closed
    assert len(h.requests) == 1


def test_sse_structured_model_is_checked_on_created_event():
    stream = ChunkStream([
        sse({"type": "response.created", "response": {"model": "gpt-5.4"}}),
        sse({"type": "response.output_text.delta", "delta": "1 2 3"}),
        sse({"type": "response.completed", "response": {**completed(), "model": "gpt-5.4"}}),
    ])
    result = run_probe(make_transport(sse_response(stream)).transport)
    assert result.error_code is None and result.text == "1 2 3"
    assert result.upstream_model == "gpt-5.4"


def test_sse_terminal_model_conflict_after_delta_discards_all_output():
    stream = ChunkStream([
        sse({"type": "response.created", "response": {"model": "gpt-5.4"}}),
        sse({"type": "response.output_text.delta", "delta": "1 2 3"}),
        sse({"type": "response.completed", "response": {**completed(), "model": "gpt-5.6-luna"}}),
    ])
    result = run_probe(make_transport(sse_response(stream)).transport)
    assert result.error_code == "upstream_model_mismatch"
    assert result.text is None
    assert result.upstream_model == "gpt-5.4"


def test_sse_model_missing_remains_compatible_and_does_not_infer_from_delta_text():
    stream = ChunkStream([
        sse({"type": "response.output_text.delta", "delta": "model=gpt-5.6-luna 1 2 3"}),
        sse({"type": "response.completed", "response": completed()}),
    ])
    result = run_probe(make_transport(sse_response(stream)).transport)
    assert result.error_code is None
    # The terminal output snapshot is authoritative.  The model-looking text
    # in the delta is still treated as ordinary output, never as identity.
    assert result.text == "1 2 3"
    assert result.upstream_model is None


def test_sse_completed_closes_an_open_connection_promptly():
    class OpenEnded(ChunkStream):
        async def __aiter__(self):
            yield sse({"type": "response.completed", "response": completed()})
            await asyncio.Event().wait()
    stream = OpenEnded([])
    h = make_transport(sse_response(stream), timeout_seconds=0.05, total_timeout_seconds=0.05)
    start = time.monotonic()
    assert run_probe(h.transport).text == "1 2 3"
    assert time.monotonic() - start < 0.2 and stream.closed


@pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
def test_sse_utf8_bom_multiline_comments_split_chunks(newline):
    body = ("\ufeff: ping\nevent: response.completed\n"
            'data: {"type":"response.completed",\n'
            'data: "response":{"status":"completed","output_text":"你好 1 2 3"}}\n\n'
            ": tail\n").replace("\n", newline).encode()
    stream = ChunkStream([bytes([byte]) for byte in body])
    h = make_transport(sse_response(stream), max_response_bytes=len(body))
    result = run_probe(h.transport)
    assert result.error_code is None and result.text == "你好 1 2 3"
    assert stream.closed


@pytest.mark.parametrize("terminal, error", [
    ({"type": "response.failed", "response": {"status": "failed", "error": "secret"}}, "upstream_response_failed"),
    ({"type": "error", "message": "secret"}, "upstream_response_failed"),
    ({"type": "response.incomplete", "response": {"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}}}, "response_truncated"),
    ({"type": "response.completed", "response": {"status": "in_progress", "output_text": "1 2 3"}}, "response_truncated"),
    ({"type": "response.completed", "response": {"status": "completed", "incomplete_details": {"reason": "max_output_tokens"}, "output_text": "1 2 3"}}, "response_truncated"),
    ({"type": "response.completed"}, "response_truncated"),
    ({"type": "response.completed", "response": {"status": "completed", "output": []}}, "missing_output_text"),
])
def test_sse_rejects_failed_or_incomplete_even_after_enough_deltas(terminal, error):
    stream = ChunkStream([sse({"type": "response.output_text.delta", "delta": "1 " * 400}), sse(terminal)])
    h = make_transport(sse_response(stream))
    result = run_probe(h.transport)
    assert result.text is None and result.error_code == error
    assert "secret" not in repr(result) and len(h.requests) == 1 and stream.closed


@pytest.mark.parametrize("tail", [b"", b"data: [DONE]\n\n", b'data: {"type":"response.completed","response":{"status":"completed"}}'])
def test_sse_requires_a_fully_framed_completed_event(tail):
    stream = ChunkStream([sse({"type": "response.output_text.delta", "delta": "1 2 3"}), tail])
    result = run_probe(make_transport(sse_response(stream)).transport)
    assert result.error_code == "response_truncated" and result.text is None and stream.closed


def test_sse_completed_without_snapshot_can_finalize_received_deltas():
    stream = ChunkStream([
        sse({"type": "response.output_text.delta", "delta": "1 2 "}),
        sse({"type": "response.output_text.delta", "delta": "3"}),
        sse({"type": "response.completed", "response": {"status": "completed"}}),
    ])
    assert run_probe(make_transport(sse_response(stream)).transport).text == "1 2 3"


@pytest.mark.parametrize("body", [
    b"data: invalid\n\n", b"data: []\n\n", b"data: \xff\n\n",
    b'event: response.completed\ndata: {"type":"response.failed"}\n\n',
    b'data: {"type":"response.output_text.delta","delta":3}\n\n',
])
def test_invalid_sse_is_safe_and_not_retried(body):
    h = make_transport(sse_response(ChunkStream([body])))
    result = run_probe(h.transport)
    assert result.error_code == "invalid_event_stream" and result.text is None and len(h.requests) == 1


def test_sse_envelope_byte_limit_includes_comments_and_ignored_events():
    stream = ChunkStream([b": " + b"x" * 300 + b"\n\n", sse({"type": "response.completed", "response": completed()})])
    result = run_probe(make_transport(sse_response(stream), max_response_bytes=256).transport)
    assert result.error_code == "response_too_large" and result.text is None
    assert stream.read_count == 1 and stream.closed


def test_sse_allows_large_bounded_metadata_before_completed_event():
    metadata = [
        sse({"type": "response.created", "response": {"metadata": "x" * (600 * 1024)}}),
        sse({"type": "response.in_progress", "response": {"metadata": "x" * (600 * 1024)}}),
    ]
    stream = ChunkStream(metadata + [sse({"type": "response.completed", "response": completed()})])
    h = make_transport(sse_response(stream), max_response_bytes=8 * 1024 * 1024)

    result = run_probe(h.transport)

    assert result.error_code is None and result.text == "1 2 3"
    assert stream.read_count == 3 and stream.closed and len(h.requests) == 1


def test_sse_rejects_one_oversized_event_before_json_parse_or_retry():
    stream = ChunkStream([
        sse({"type": "response.created", "metadata": "x" * (2 * 1024 * 1024)}),
        sse({"type": "response.completed", "response": completed()}),
    ])
    h = make_transport(sse_response(stream), max_response_bytes=8 * 1024 * 1024)

    result = run_probe(h.transport)

    assert result.error_code == "response_too_large" and result.text is None
    assert stream.read_count == 1 and stream.closed and len(h.requests) == 1


def test_sse_output_delta_has_a_separate_bounded_memory_budget():
    stream = ChunkStream([
        sse({"type": "response.output_text.delta", "delta": "x" * (600 * 1024)}),
        sse({"type": "response.completed", "response": completed()}),
    ])
    h = make_transport(sse_response(stream), max_response_bytes=8 * 1024 * 1024)

    result = run_probe(h.transport)

    assert result.error_code == "response_too_large" and result.text is None
    assert stream.read_count == 1 and stream.closed and len(h.requests) == 1


def test_sse_envelope_higher_cap_is_still_hard_and_does_not_retry():
    limit = 8 * 1024 * 1024
    stream = ChunkStream([b": " + b"x" * limit, sse({"type": "response.completed", "response": completed()})])
    h = make_transport(sse_response(stream), max_response_bytes=limit)

    result = run_probe(h.transport)

    assert result.error_code == "response_too_large" and result.text is None
    assert stream.read_count == 1 and stream.closed and len(h.requests) == 1


def test_sse_deadline_is_request_wide_even_with_frequent_events():
    stream = ChunkStream([b": ping\n\n"] * 100, delay=0.005)
    h = make_transport(sse_response(stream), timeout_seconds=0.04, total_timeout_seconds=0.04)
    start = time.monotonic()
    result = run_probe(h.transport)
    assert result.error_code == "upstream_timeout" and result.text is None
    assert time.monotonic() - start < 0.3 and stream.closed and len(h.requests) == 1


@pytest.mark.parametrize("affinity", ["", "bad\r\nheader", "x" * 129, "非ASCII", 23])
def test_bad_affinity_never_sends(affinity):
    h = make_transport()
    result = h.transport.run(model="gpt-5.4", challenge={"prompt": "test"}, user_agent="ModelTraceProbe/test", session_affinity=affinity)
    assert result.error_code == "invalid_session_affinity" and not h.requests


def test_affinity_header_is_optional_and_does_not_change_prompt():
    h = make_transport()
    run_probe(h.transport)
    assert "x-session-affinity" not in h.requests[0].headers
    result = h.transport.run(model="gpt-5.4", challenge={"prompt": "give numbers", "expected_count": 3}, user_agent="ModelTraceProbe/local-test", session_affinity="mt-1234_abcd")
    assert result.error_code is None and h.requests[1].headers["x-session-affinity"] == "mt-1234_abcd"
    assert h.requests[0].content == h.requests[1].content


def test_sse_final_cr_is_a_valid_event_delimiter():
    body = sse({"type": "response.completed", "response": completed()}).replace(b"\n", b"\r")
    stream = ChunkStream([bytes([byte]) for byte in body])
    result = run_probe(make_transport(sse_response(stream)).transport)
    assert result.text == "1 2 3" and result.error_code is None and stream.closed


def test_sse_eof_inside_utf8_is_invalid_not_successful():
    stream = ChunkStream([b'data: {"delta":"' + b'\xe4\xb8'])
    result = run_probe(make_transport(sse_response(stream)).transport)
    assert result.text is None and result.error_code == "invalid_event_stream"


def test_internal_probe_proof_is_hmac_of_unique_user_agent_not_raw_secret():
    import hashlib
    import hmac
    secret = "internal-test-shared-secret"
    h = make_transport(probe_secret=secret)
    result = run_probe(h.transport)
    header = h.requests[0].headers["x-sub2api-modeltrace-signature"]
    assert header == hmac.new(secret.encode(), b"ModelTraceProbe/local-test", hashlib.sha256).hexdigest()
    assert secret not in str(h.requests[0].headers) + h.requests[0].content.decode() + repr(result)
    assert h.requests[0].headers["Authorization"] == "Bearer dummy-test-key"
    assert "x-sub2api-modeltrace-signature" not in make_transport().transport._payload("gpt-5.4", {"prompt": "test"})


def test_internal_probe_proof_is_omitted_when_not_configured():
    h = make_transport(probe_secret=None)
    run_probe(h.transport)
    assert "x-sub2api-modeltrace-signature" not in h.requests[0].headers


def test_service_passes_existing_internal_secret_to_real_transport(tmp_path, fake_clock, monkeypatch):
    from modeltrace.service import ModelTraceService
    monkeypatch.setenv("MODELTRACE_SECRET", "test-internal-proof-secret")
    fixture, _, _ = build_service(tmp_path, fake_clock)
    service = ModelTraceService(fixture.config, database_path=tmp_path / "real-transport.sqlite3")
    try:
        assert service.transport.probe_secret == "test-internal-proof-secret"
        assert service.transport.timeout_seconds == 120
        assert service.transport.max_output_tokens == 2048
    finally:
        service.db.close()
        fixture.db.close()


def test_reasoning_effort_override_changes_only_selected_model_payload():
    h = make_transport(reasoning_effort_overrides={"gpt-6-astra": "low"})
    astra = h.transport._payload("gpt-6-astra", {"prompt": "test"})
    other = h.transport._payload("gpt-5.4", {"prompt": "test"})
    assert astra["reasoning"] == {"effort": "low"}
    assert other["reasoning"] == {"effort": "none"}


def test_unsupported_reasoning_effort_is_deterministic_parameter_error():
    response = httpx.Response(
        400,
        json={
            "error": {
                "code": "unsupported_value",
                "param": "reasoning.effort",
                "message": "unsupported",
            }
        },
    )
    result = run_probe(make_transport(response).transport)
    assert result.status_code == 400
    assert result.error_code == "upstream_unsupported_reasoning_effort"
    assert result.deterministic_parameter_error is True


def test_oversized_json_400_is_bounded_before_error_classification():
    # The body is deliberately chunked so this catches an implementation that
    # calls response.aread() and checks the size only after buffering it.
    oversized = b"{" + b"x" * (256 * 1024) + b"}"
    stream = ChunkStream([oversized[:128 * 1024], oversized[128 * 1024:]])
    h = make_transport(
        httpx.Response(400, stream=stream, headers={"content-type": "application/json"}),
        max_response_bytes=512 * 1024,
    )
    result = run_probe(h.transport)
    assert result.error_code == "response_too_large"
    assert stream.read_count == 2
    assert stream.closed


def test_timeout_diagnostics_record_received_progress_without_model_text():
    class StalledAfterText(ChunkStream):
        async def __aiter__(self):
            yield sse({"type": "response.created", "response": {"model": "gpt-5.4"}})
            yield sse({"type": "response.output_text.delta", "delta": "1 2 3 do-not-log-this-text"})
            await asyncio.sleep(1)
    stream = StalledAfterText([])
    h = make_transport(sse_response(stream), timeout_seconds=0.03, total_timeout_seconds=0.03)
    result = run_probe(h.transport)
    assert result.text is None and result.error_code == 'upstream_timeout'
    detail = result.transport_detail
    assert detail['http_headers_ms'] is not None
    assert detail['first_event_ms'] is not None and detail['first_text_ms'] is not None
    assert detail['received_bytes'] > 0 and detail['event_count'] == 2
    assert detail['output_bytes'] > 0 and detail['output_delta_count'] == 1
    assert detail['partial_numeric_count'] == 3
    assert detail['termination_event_received'] is False
    assert 'do-not-log' not in repr(detail) and 'dummy-test-key' not in repr(detail)
    assert stream.closed and len(h.requests) == 1


def test_incomplete_terminal_diagnostic_is_distinct_from_no_terminal():
    stream = ChunkStream([sse({'type':'response.incomplete', 'response': {'status':'incomplete'}})])
    result = run_probe(make_transport(sse_response(stream)).transport)
    assert result.error_code == 'response_truncated'
    assert result.transport_detail['termination_event_received'] is True


def test_transport_payload_omits_max_output_tokens_when_none():
    h = make_transport(max_output_tokens=None)
    payload = h.transport._payload("gpt-5.4", {"prompt": "test"})
    assert "max_output_tokens" not in payload
    assert h.transport.max_output_tokens is None


def test_transport_payload_includes_max_output_tokens_when_configured():
    h = make_transport(max_output_tokens=3000)
    payload = h.transport._payload("gpt-5.4", {"prompt": "test"})
    assert payload["max_output_tokens"] == 3000
    assert h.transport.max_output_tokens == 3000


def test_transport_default_init_max_output_tokens_is_none():
    transport = ProbeTransport(endpoint="https://api.example/v1/responses", api_key="dummy-key")
    assert transport.max_output_tokens is None
    payload = transport._payload("gpt-5.4", {"prompt": "test"})
    assert "max_output_tokens" not in payload


def test_default_has_no_wall_clock_cap_so_a_slow_steady_answer_finishes():
    # Production sets no total_timeout_seconds: a model that keeps streaming
    # past timeout_seconds must be allowed to finish its answer.
    class SlowSteady(ChunkStream):
        async def __aiter__(self):
            yield sse({"type": "response.created", "response": {"model": "gpt-5.4"}})
            for _ in range(5):
                await asyncio.sleep(0.04)
                yield sse({"type": "response.reasoning_summary_text.delta", "delta": "."})
            yield sse({"type": "response.completed", "response": completed()})
    stream = SlowSteady([])
    h = make_transport(sse_response(stream), timeout_seconds=0.05, idle_timeout_seconds=1)
    start = time.monotonic()
    result = run_probe(h.transport)
    assert time.monotonic() - start > 0.15
    assert result.error_code is None and result.text == "1 2 3"


def test_idle_guard_still_cuts_a_silent_stream_without_wall_clock_cap():
    class GoesSilent(ChunkStream):
        async def __aiter__(self):
            yield sse({"type": "response.created", "response": {"model": "gpt-5.4"}})
            await asyncio.sleep(5)
    stream = GoesSilent([])
    h = make_transport(sse_response(stream), timeout_seconds=0.05, idle_timeout_seconds=0.1)
    start = time.monotonic()
    result = run_probe(h.transport)
    assert time.monotonic() - start < 1
    assert result.error_code == "upstream_timeout"
