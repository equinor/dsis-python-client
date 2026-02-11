"""
Unit tests for DSISClient.execute_query with max_pages parameter.

Tests verify that execute_query correctly handles:
- max_pages=-1: yield all pages (default, unlimited)
- max_pages=N: yield up to N pages, stopping early if fewer pages available
"""

from types import SimpleNamespace


def make_config():
    """Create a minimal config-like object for DSISClient initialization.

    We don't need a full DSISConfig here; just provide the attributes used by
    the client for building endpoints and initializing auth/session.
    """
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
        model_name="Model",
        model_version="1",
        dsis_site="qa",
        data_endpoint="https://example.org",
    )


def _make_client_and_patch(monkeypatch, district, project, response):
    """Helper to create a DSISClient and monkeypatch its _request to return response.

    If response is a list of pages, the _request will return them in order for
    subsequent calls (to simulate OData paging). If response is a single dict,
    it will always be returned.
    Returns (client, qb, collector) where collector is a small object used by tests
    to assert call counts or inspect endpoints.
    """
    from dsis_client.api.auth import DSISAuth
    from dsis_client.api.client import DSISClient
    from dsis_client.api.query import QueryBuilder  # type: ignore[import-untyped]

    # Stub out token acquisition so the eager get_auth_headers() in __init__
    # doesn't attempt real HTTP calls.
    monkeypatch.setattr(
        DSISAuth, "get_auth_headers", lambda self: {"Authorization": "Bearer fake"}
    )

    cfg = make_config()
    client = DSISClient(cfg)

    qb = QueryBuilder(district_id=district, project=project)
    qb.schema("MySchema")

    # Normalize response into pages list
    pages = response if isinstance(response, list) else [response]
    state = {"idx": 0}

    def fake_request(endpoint, params=None):
        i = state["idx"]
        if i < len(pages):
            page = pages[i]
        else:
            page = pages[-1]
        state["idx"] += 1
        return page

    monkeypatch.setattr(client, "_request", fake_request)
    return client, qb, state


def test_execute_query_max_pages_unlimited(monkeypatch):
    """Test that max_pages=-1 (default) yields all items across multiple pages."""
    # Arrange: multi-page response with nextLinks
    pages = [
        {"value": [{"id": 1}, {"id": 2}], "odata.nextLink": "MySchema?page=2"},
        {"value": [{"id": 3}]},
    ]

    client, qb, state = _make_client_and_patch(monkeypatch, "D", "F", pages)

    # Act: fetch all pages (default max_pages=-1)
    results = list(client.execute_query(qb, cast=False, max_pages=-1))

    # Assert: all items from all pages are yielded
    assert results == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_execute_query_max_pages_limited(monkeypatch):
    """Test that max_pages=1 returns only items from the first page."""
    # Arrange: single page response
    sample = {"value": [{"x": 9}], "odata.nextLink": None}
    client, qb, state = _make_client_and_patch(monkeypatch, "D2", "F2", sample)

    # Act: fetch only 1 page
    result = list(client.execute_query(qb, cast=False, max_pages=1))

    # Assert: result contains only items from the first page
    assert result == sample["value"]
