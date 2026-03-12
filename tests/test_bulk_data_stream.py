"""Unit tests for bulk-data streaming retry behavior."""

from types import SimpleNamespace

import requests

from dsis_client.api import DSISAPIError
from dsis_client.api.query import QueryBuilder  # type: ignore[import-untyped]


def make_config():
    """Create a minimal config-like object for DSISClient initialization."""
    return SimpleNamespace(
        environment=SimpleNamespace(value="dev"),
        tenant_id="t",
        client_id="c",
        client_secret="s",
        access_app_id="a",
        dsis_username="u",
        dsis_password="p",
        subscription_key_dsauth="k1",
        subscription_key_dsdata="k2",
        dsis_site="qa",
        data_endpoint="https://example.org",
    )


class FakeStreamingResponse:
    """Minimal streaming response stub with resumable byte position."""

    def __init__(
        self,
        payload: bytes,
        *,
        fail_after_chunks: int | None = None,
        error: Exception | None = None,
    ) -> None:
        self.status_code = 200
        self.reason = "OK"
        self.text = ""
        self._payload = payload
        self._position = 0
        self._yielded_chunks = 0
        self._fail_after_chunks = fail_after_chunks
        self._error = error or requests.exceptions.ReadTimeout("stream timed out")
        self.closed = False
        self.raw = self

    def read(self, size: int) -> bytes:
        chunk = self._payload[self._position : self._position + size]
        self._position += len(chunk)
        return chunk

    def iter_content(self, chunk_size: int = 1):
        while self._position < len(self._payload):
            if (
                self._fail_after_chunks is not None
                and self._yielded_chunks >= self._fail_after_chunks
            ):
                raise self._error

            chunk = self.read(chunk_size)
            if not chunk:
                return

            self._yielded_chunks += 1
            yield chunk

    def close(self) -> None:
        self.closed = True


def make_client(monkeypatch):
    """Create a DSISClient with token acquisition stubbed out."""
    from dsis_client.api.auth import DSISAuth
    from dsis_client.api.client import DSISClient

    monkeypatch.setattr(
        DSISAuth, "get_auth_headers", lambda self: {"Authorization": "Bearer fake"}
    )
    return DSISClient(make_config())


def make_query():
    """Create a minimal entity query for bulk-data streaming."""
    query = QueryBuilder(
        model_name="Model",
        district_id="D",
        project="P",
        model_version="1",
    )
    query.schema("MySchema")
    query.entity("entity-1")
    return query


def test_get_bulk_data_stream_recovers_from_transient_stream_failure(monkeypatch):
    """Streaming retries resume cleanly after a transient read failure."""
    client = make_client(monkeypatch)
    query = make_query()
    payload = b"abcdefghijkl"
    responses = [
        FakeStreamingResponse(payload, fail_after_chunks=1),
        FakeStreamingResponse(payload),
    ]

    def fake_get(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(client._session, "get", fake_get)
    monkeypatch.setattr("dsis_client.api.client._http.time.sleep", lambda _: None)

    chunks = list(
        client.get_bulk_data_stream(
            query,
            chunk_size=4,
            timeout=(2, 2),
            stream_retries=1,
        )
    )

    assert chunks == [b"abcd", b"efgh", b"ijkl"]


def test_get_bulk_data_stream_raises_after_retries_exhausted(monkeypatch):
    """Streaming raises once the configured retry count is exhausted."""
    client = make_client(monkeypatch)
    query = make_query()
    payload = b"abcdef"
    responses = [
        FakeStreamingResponse(payload, fail_after_chunks=0),
        FakeStreamingResponse(payload, fail_after_chunks=0),
        FakeStreamingResponse(payload, fail_after_chunks=0),
    ]

    def fake_get(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(client._session, "get", fake_get)
    monkeypatch.setattr("dsis_client.api.client._http.time.sleep", lambda _: None)

    try:
        list(
            client.get_bulk_data_stream(
                query,
                chunk_size=4,
                timeout=(2, 2),
                stream_retries=2,
            )
        )
    except DSISAPIError as exc:
        assert "Streaming binary request failed after 2 retries" in str(exc)
    else:
        raise AssertionError("Expected DSISAPIError when stream retries are exhausted")