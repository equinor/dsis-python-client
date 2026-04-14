"""Focused tests for bulk data media-link support."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
import sys
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "dsis_client" / "api"


def load_bulk_data_types(monkeypatch):
    """Load query and bulk-data modules directly from source."""
    for name in list(sys.modules):
        if name == "dsis_client" or name.startswith("dsis_client."):
            monkeypatch.delitem(sys.modules, name, raising=False)

    dsis_pkg = types.ModuleType("dsis_client")
    dsis_pkg.__path__ = [str(ROOT / "src" / "dsis_client")]
    monkeypatch.setitem(sys.modules, "dsis_client", dsis_pkg)

    api_pkg = types.ModuleType("dsis_client.api")
    api_pkg.__path__ = [str(SRC)]
    monkeypatch.setitem(sys.modules, "dsis_client.api", api_pkg)
    dsis_pkg.api = api_pkg

    query_pkg = types.ModuleType("dsis_client.api.query")
    query_pkg.__path__ = [str(SRC / "query")]
    monkeypatch.setitem(sys.modules, "dsis_client.api.query", query_pkg)
    api_pkg.query = query_pkg

    client_pkg = types.ModuleType("dsis_client.api.client")
    client_pkg.__path__ = [str(SRC / "client")]
    monkeypatch.setitem(sys.modules, "dsis_client.api.client", client_pkg)
    api_pkg.client = client_pkg

    def load_module(name: str, path: Path):
        spec = spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load module {name} from {path}")
        module = module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        return module

    odata_module = load_module("dsis_client.api.query.odata", SRC / "query" / "odata.py")
    builder_module = load_module(
        "dsis_client.api.query.builder", SRC / "query" / "builder.py"
    )
    query_pkg.odata = odata_module
    query_pkg.builder = builder_module
    query_pkg.QueryBuilder = builder_module.QueryBuilder

    base_module = load_module("dsis_client.api.client._base", SRC / "client" / "_base.py")
    bulk_data_module = load_module(
        "dsis_client.api.client._bulk_data", SRC / "client" / "_bulk_data.py"
    )
    client_pkg._base = base_module
    client_pkg._bulk_data = bulk_data_module

    return bulk_data_module.BulkDataMixin, builder_module.QueryBuilder


def make_client(BulkDataMixin):
    """Create a minimal client that records bulk-data calls."""

    class FakeClient(BulkDataMixin):
        def __init__(self) -> None:
            self.config = SimpleNamespace(data_endpoint="https://example.org/dsdata/v1")
            self.binary_calls: list[dict[str, object]] = []
            self.stream_calls: list[dict[str, object]] = []

        def _request_binary(
            self, endpoint, params=None, accept="application/json", timeout=None
        ):
            self.binary_calls.append(
                {
                    "endpoint": endpoint,
                    "params": params,
                    "accept": accept,
                    "timeout": timeout,
                }
            )
            return b"payload"

        def _request_binary_stream(
            self,
            endpoint,
            params=None,
            chunk_size=10 * 1024 * 1024,
            accept="application/json",
            timeout=None,
            stream_retries=0,
            total_timeout=None,
        ):
            self.stream_calls.append(
                {
                    "endpoint": endpoint,
                    "params": params,
                    "chunk_size": chunk_size,
                    "accept": accept,
                    "timeout": timeout,
                    "stream_retries": stream_retries,
                    "total_timeout": total_timeout,
                }
            )
            yield b"chunk-one"
            yield b"chunk-two"

    return FakeClient()


def make_query(QueryBuilder):
    """Create a query matching the WellLog case from <issue>:75."""
    return QueryBuilder(
        model_name="RecallCommonModel",
        model_version="500010",
        district_id="RecallCommonModel_OFDB_RecallProd-RecallProd",
        project="NORWAY_WELLDB",
    ).schema("LogCurve")


def test_get_bulk_data_accepts_relative_media_link(monkeypatch):
    """Relative OData media links resolve against the base query context."""
    BulkDataMixin, QueryBuilder = load_bulk_data_types(monkeypatch)
    client = make_client(BulkDataMixin)
    query = make_query(QueryBuilder)
    media_link = (
        "LogCurve(log_crv_name='LFP_VPVS_LOG',log_crv_version=1,"
        "log_name='GeologLFP',log_pass_id='NONE',log_run_no=-1,wellid='52')/data"
    )

    payload = client.get_bulk_data(query, media_link=media_link, timeout=(5, 30))

    assert payload == b"payload"
    assert client.binary_calls == [
        {
            "endpoint": (
                "RecallCommonModel/500010/RecallCommonModel_OFDB_RecallProd-RecallProd/"
                "NORWAY_WELLDB/"
                "LogCurve(log_crv_name='LFP_VPVS_LOG',log_crv_version=1,"
                "log_name='GeologLFP',log_pass_id='NONE',log_run_no=-1,wellid='52')/data"
            ),
            "params": None,
            "accept": "application/json",
            "timeout": (5, 30),
        }
    ]


def test_get_bulk_data_preserves_entity_based_behavior(monkeypatch):
    """Existing entity-targeting behavior remains unchanged."""
    BulkDataMixin, QueryBuilder = load_bulk_data_types(monkeypatch)
    client = make_client(BulkDataMixin)
    query = make_query(QueryBuilder).entity("44367/6")

    payload = client.get_bulk_data(query)

    assert payload == b"payload"
    assert client.binary_calls[0]["endpoint"] == (
        "RecallCommonModel/500010/RecallCommonModel_OFDB_RecallProd-RecallProd/"
        "NORWAY_WELLDB/LogCurve('44367/6')/data"
    )


def test_get_bulk_data_stream_accepts_media_link(monkeypatch):
    """Streaming bulk data also accepts the exact OData media link."""
    BulkDataMixin, QueryBuilder = load_bulk_data_types(monkeypatch)
    client = make_client(BulkDataMixin)
    query = make_query(QueryBuilder)

    chunks = list(
        client.get_bulk_data_stream(
            query,
            media_link="LogCurve('44367/6')/data",
            chunk_size=4,
            accept="application/octet-stream",
            timeout=60,
            stream_retries=2,
            total_timeout=120,
        )
    )

    assert chunks == [b"chunk-one", b"chunk-two"]
    assert client.stream_calls == [
        {
            "endpoint": (
                "RecallCommonModel/500010/RecallCommonModel_OFDB_RecallProd-RecallProd/"
                "NORWAY_WELLDB/LogCurve('44367/6')/data"
            ),
            "params": None,
            "chunk_size": 4,
            "accept": "application/octet-stream",
            "timeout": 60,
            "stream_retries": 2,
            "total_timeout": 120,
        }
    ]


def test_get_bulk_data_accepts_full_data_endpoint_url(monkeypatch):
    """Full URLs under the configured data endpoint are normalized."""
    BulkDataMixin, QueryBuilder = load_bulk_data_types(monkeypatch)
    client = make_client(BulkDataMixin)
    query = make_query(QueryBuilder)

    payload = client.get_bulk_data(
        query,
        media_link=(
            "https://example.org/dsdata/v1/RecallCommonModel/500010/"
            "RecallCommonModel_OFDB_RecallProd-RecallProd/NORWAY_WELLDB/"
            "LogCurve('44367/6')/data"
        ),
    )

    assert payload == b"payload"
    assert client.binary_calls[0]["endpoint"] == (
        "RecallCommonModel/500010/RecallCommonModel_OFDB_RecallProd-RecallProd/"
        "NORWAY_WELLDB/LogCurve('44367/6')/data"
    )


def test_get_bulk_data_rejects_media_link_for_other_service(monkeypatch):
    """Absolute URLs must point at the configured DSIS data endpoint."""
    BulkDataMixin, QueryBuilder = load_bulk_data_types(monkeypatch)
    client = make_client(BulkDataMixin)
    query = make_query(QueryBuilder)

    with pytest.raises(ValueError, match="configured DSIS data endpoint"):
        client.get_bulk_data(
            query,
            media_link="https://other.example.org/dsdata/v1/LogCurve('44367/6')/data",
        )
