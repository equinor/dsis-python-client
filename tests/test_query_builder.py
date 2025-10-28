"""Tests for QueryBuilder."""

import pytest
from urllib.parse import unquote, parse_qs, urlparse

from src.dsis_client import QueryBuilder, DSISClient, DSISConfig


class TestQueryBuilderBasic:
    """Test basic QueryBuilder functionality."""

    def test_init_default(self):
        """Test default initialization with required parameters."""
        builder = QueryBuilder(district_id="test_district", field="test_field")
        assert builder.district_id == "test_district"
        assert builder.field == "test_field"
        assert builder._schema_name is None
        assert builder._select == []
        assert builder._expand == []
        assert builder._filter is None
        assert builder._format == "json"

    def test_init_with_path_parameters(self):
        """Test initialization with path parameters."""
        builder = QueryBuilder(
            district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            field="SNORRE"
        )
        assert builder.district_id == "OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA"
        assert builder.field == "SNORRE"

    def test_init_with_all_parameters(self):
        """Test initialization with all parameters."""
        builder = QueryBuilder(
            district_id="test_district",
            field="test_field"
        )
        assert builder.district_id == "test_district"
        assert builder.field == "test_field"


class TestQueryBuilderModelHelpers:
    """Test schema helper methods."""

    def test_schema_method_with_well_class(self):
        """Test schema() method with Well model class."""
        from dsis_model_sdk.models.common import Well

        builder = QueryBuilder(district_id="123", field="test")
        result = builder.schema(Well)
        assert result is builder  # Check chaining
        assert builder._schema_name == "Well"
        assert builder._schema_class == Well

    def test_schema_method_with_basin_class(self):
        """Test schema() method with Basin model class."""
        from dsis_model_sdk.models.common import Basin

        builder = QueryBuilder(district_id="123", field="test")
        result = builder.schema(Basin)
        assert result is builder
        assert builder._schema_name == "Basin"
        assert builder._schema_class == Basin

    def test_schema_method_with_string(self):
        """Test schema() method with string name."""
        builder = QueryBuilder(district_id="123", field="test")
        result = builder.schema("Fault")
        assert result is builder
        assert builder._schema_name == "Fault"
        assert builder._schema_class is None

    def test_schema_method_invalid_name(self):
        """Test schema() method with invalid schema name - no validation."""
        builder = QueryBuilder(district_id="123", field="test")
        # No validation in QueryBuilder anymore - API will validate
        builder.schema("InvalidSchemaName123")
        assert builder._schema_name == "InvalidSchemaName123"

    def test_full_chain_with_schema(self):
        """Test full chain using schema() method."""
        from dsis_model_sdk.models.common import Fault

        query = (QueryBuilder(district_id="123", field="wells")
            .schema(Fault)
            .select("fault_name", "fault_type", "native_uid")
            .filter("fault_type eq 'NORMAL'"))

        assert isinstance(query, QueryBuilder)
        assert query.district_id == "123"
        assert query.field == "wells"
        query_string = query.get_query_string()
        assert "Fault" in query_string
        assert "fault_name" in unquote(query_string)
        assert "fault_type" in unquote(query_string)


class TestQueryBuilderSchema:
    """Test schema method."""

    def test_schema_valid(self):
        """Test setting valid schema."""
        builder = QueryBuilder(district_id="123", field="test")
        result = builder.schema("Well")
        assert result is builder  # Check chaining
        assert builder._schema_name == "Well"

    def test_schema_no_validation(self):
        """Test schema accepts any name - validation moved to API."""
        builder = QueryBuilder(district_id="123", field="test")
        # No validation in QueryBuilder - API will validate
        builder.schema("InvalidModel123")
        assert builder._schema_name == "InvalidModel123"

    def test_schema_basin(self):
        """Test setting Basin schema."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.schema("Basin")
        assert builder._schema_name == "Basin"

    def test_schema_wellbore(self):
        """Test setting Wellbore schema."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.schema("Wellbore")
        assert builder._schema_name == "Wellbore"


class TestQueryBuilderSelect:
    """Test select method."""

    def test_select_single_field(self):
        """Test selecting a single field."""
        builder = QueryBuilder(district_id="123", field="test")
        result = builder.select("name")
        assert result is builder  # Check chaining
        assert builder._select == ["name"]

    def test_select_multiple_fields(self):
        """Test selecting multiple fields."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.select("name", "depth", "status")
        assert builder._select == ["name", "depth", "status"]

    def test_select_comma_separated(self):
        """Test selecting comma-separated fields."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.select("name,depth,status")
        assert builder._select == ["name", "depth", "status"]

    def test_select_mixed(self):
        """Test selecting with mixed comma-separated and individual."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.select("name,depth").select("status")
        assert builder._select == ["name", "depth", "status"]

    def test_select_with_spaces(self):
        """Test selecting with spaces in comma-separated list."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.select("name, depth, status")
        assert builder._select == ["name", "depth", "status"]


class TestQueryBuilderExpand:
    """Test expand method."""

    def test_expand_single_relation(self):
        """Test expanding a single relation."""
        builder = QueryBuilder(district_id="123", field="test")
        result = builder.expand("wells")
        assert result is builder  # Check chaining
        assert builder._expand == ["wells"]

    def test_expand_multiple_relations(self):
        """Test expanding multiple relations."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.expand("wells", "horizons", "faults")
        assert builder._expand == ["wells", "horizons", "faults"]

    def test_expand_comma_separated(self):
        """Test expanding comma-separated relations."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.expand("wells,horizons,faults")
        assert builder._expand == ["wells", "horizons", "faults"]

    def test_expand_mixed(self):
        """Test expanding with mixed comma-separated and individual."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.expand("wells,horizons").expand("faults")
        assert builder._expand == ["wells", "horizons", "faults"]


class TestQueryBuilderFilter:
    """Test filter method."""

    def test_filter_simple(self):
        """Test setting a simple filter."""
        builder = QueryBuilder(district_id="123", field="test")
        result = builder.filter("depth gt 1000")
        assert result is builder  # Check chaining
        assert builder._filter == "depth gt 1000"

    def test_filter_equality(self):
        """Test filter with equality."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.filter("name eq 'Well-1'")
        assert builder._filter == "name eq 'Well-1'"

    def test_filter_complex(self):
        """Test filter with complex expression."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.filter("depth gt 1000 and status eq 'active'")
        assert builder._filter == "depth gt 1000 and status eq 'active'"

    def test_filter_override(self):
        """Test that filter overrides previous filter."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.filter("depth gt 1000")
        builder.filter("depth lt 2000")
        assert builder._filter == "depth lt 2000"


class TestQueryBuilderGetQueryString:
    """Test get_query_string method."""

    def test_get_query_string_basic(self):
        """Test getting basic query string."""
        query = QueryBuilder(district_id="123", field="test").schema("Well")
        query_string = query.get_query_string()
        assert "Well" in query_string
        assert "$format=json" in unquote(query_string)

    def test_get_query_string_with_select(self):
        """Test getting query string with select."""
        query = QueryBuilder(district_id="123", field="test").schema("Well").select("name,depth")
        query_string = query.get_query_string()
        decoded = unquote(query_string)
        assert "Well" in decoded
        assert "name" in decoded
        assert "depth" in decoded

    def test_get_query_string_with_filter(self):
        """Test getting query string with filter."""
        query = QueryBuilder(district_id="123", field="test").schema("Well").filter("depth gt 1000")
        query_string = query.get_query_string()
        decoded = unquote(query_string)
        assert "Well" in decoded
        assert "depth" in decoded

    def test_get_query_string_with_expand(self):
        """Test getting query string with expand."""
        query = QueryBuilder(district_id="123", field="test").schema("Basin").expand("wells,horizons")
        query_string = query.get_query_string()
        decoded = unquote(query_string)
        assert "Basin" in decoded
        assert "wells" in decoded

    def test_get_query_string_complex(self):
        """Test getting complex query string."""
        query = (QueryBuilder(district_id="123", field="test")
            .schema("Well")
            .select("name,depth,status")
            .filter("depth gt 1000")
            .expand("wellbores"))
        query_string = query.get_query_string()
        decoded = unquote(query_string)
        assert "Well" in decoded
        assert "name" in decoded
        assert "depth" in decoded
        assert "wellbores" in decoded

    def test_get_query_string_no_schema(self):
        """Test getting query string without schema raises error."""
        query = QueryBuilder(district_id="123", field="test")
        with pytest.raises(ValueError, match="schema must be set"):
            query.get_query_string()

    def test_get_query_string_format(self):
        """Test that format is included in query string."""
        query = QueryBuilder(district_id="123", field="test").schema("Well")
        query_string = query.get_query_string()
        decoded = unquote(query_string)
        assert "$format=json" in decoded

    def test_query_with_path_parameters(self):
        """Test query with path parameters in constructor."""
        query = QueryBuilder(
            district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            field="SNORRE"
        ).schema("Fault").select("id,type")
        assert isinstance(query, QueryBuilder)
        assert query.district_id == "OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA"
        assert query.field == "SNORRE"
        assert query._schema_name == "Fault"

    def test_query_is_query_object(self):
        """Test that QueryBuilder IS the query object."""
        query = QueryBuilder(
            district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            field="SNORRE"
        ).schema("Fault").select("id,type")
        # QueryBuilder is the query - no build() needed
        assert isinstance(query, QueryBuilder)
        assert query.district_id == "OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA"
        assert query.field == "SNORRE"
        assert query._schema_name == "Fault"

    def test_query_path_parameters_required(self):
        """Test that district_id and field are required."""
        # This should work
        query = QueryBuilder(district_id="123", field="test")
        assert query.district_id == "123"
        assert query.field == "test"


class TestQueryBuilderQueryParams:
    """Test get_query_params_string method."""

    def test_get_query_params_string_basic(self):
        """Test getting query params string without schema."""
        builder = QueryBuilder(district_id="123", field="test")
        query_str = builder.select("name").get_query_params_string()
        decoded = unquote(query_str)
        assert "name" in decoded
        assert "$format=json" in decoded

    def test_get_query_params_string_with_filter(self):
        """Test getting query params string with filter."""
        builder = QueryBuilder(district_id="123", field="test")
        query_str = builder.filter("depth gt 1000").get_query_params_string()
        decoded = unquote(query_str)
        assert "depth" in decoded

    def test_get_query_params_string_empty(self):
        """Test getting empty query params string."""
        builder = QueryBuilder(district_id="123", field="test")
        query_str = builder.get_query_params_string()
        # Should still have format
        assert "$format=json" in unquote(query_str)


class TestQueryBuilderReset:
    """Test reset method."""

    def test_reset_all_fields(self):
        """Test reset clears all fields."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.schema("Well").select("name").filter("depth gt 1000").expand("wellbores")

        result = builder.reset()
        assert result is builder  # Check chaining
        assert builder._schema_name is None
        assert builder._select == []
        assert builder._filter is None
        assert builder._expand == []
        assert builder._format == "json"

    def test_reset_allows_reuse(self):
        """Test reset allows builder reuse."""
        builder = QueryBuilder(district_id="123", field="test")

        # First query
        builder.schema("Well").select("name")
        query_string1 = builder.get_query_string()
        assert "Well" in query_string1

        # Reset and build different query
        builder.reset()
        builder.schema("Basin").select("id")
        query_string2 = builder.get_query_string()
        assert "Basin" in query_string2
        assert "Well" not in query_string2


class TestQueryBuilderChaining:
    """Test method chaining."""

    def test_full_chain(self):
        """Test full method chaining."""
        query = (QueryBuilder(district_id="123", field="test")
            .schema("Well")
            .select("name", "depth")
            .filter("depth gt 1000")
            .expand("wellbores"))

        assert isinstance(query, QueryBuilder)
        decoded = unquote(query.get_query_string())
        assert "Well" in decoded
        assert "name" in decoded
        assert "depth" in decoded
        assert "wellbores" in decoded

    def test_chain_with_reset(self):
        """Test chaining with reset."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.schema("Well").select("name")
        query_string1 = builder.get_query_string()

        builder.reset().schema("Basin").select("id")
        query_string2 = builder.get_query_string()

        assert "Well" in query_string1
        assert "Basin" in query_string2


class TestQueryBuilderRepr:
    """Test __repr__ method."""

    def test_repr_basic(self):
        """Test string representation."""
        builder = QueryBuilder(district_id="123", field="test")
        repr_str = repr(builder)
        assert "QueryBuilder" in repr_str
        assert "123" in repr_str
        assert "test" in repr_str

    def test_repr_with_data(self):
        """Test string representation with data."""
        builder = QueryBuilder(district_id="123", field="test")
        builder.schema("Well").select("name")
        repr_str = repr(builder)
        assert "Well" in repr_str
        assert "name" in repr_str


class TestQueryBuilderCasting:
    """Test QueryBuilder and DSISClient result casting functionality."""

    def test_schema_with_class_sets_schema_class(self):
        """Test schema() method with class sets schema_class."""
        from dsis_model_sdk.models.common import Well

        query = QueryBuilder(district_id="123", field="test").schema(Well)
        assert query._schema_class == Well
        assert query._schema_name == "Well"

    def test_client_cast_results_multiple_items(self):
        """Test client casting multiple result items."""
        from dsis_model_sdk.models.common import Well
        from src.dsis_client.api.config import Environment

        config = DSISConfig.for_native_model(
            environment=Environment.DEV,
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
            access_app_id="test-app",
            dsis_username="test",
            dsis_password="test",
            subscription_key_dsauth="test-key",
            subscription_key_dsdata="test-key"
        )
        client = DSISClient(config)

        items = [
            {"well_name": "WELL_1", "well_uwi": "uwi_1", "native_uid": "uid_1"},
            {"well_name": "WELL_2", "well_uwi": "uwi_2", "native_uid": "uid_2"},
        ]

        results = client.cast_results(items, Well)
        assert len(results) == 2
        assert all(isinstance(r, Well) for r in results)
        assert results[0].well_name == "WELL_1"
        assert results[1].well_name == "WELL_2"

    def test_client_cast_results_single_item(self):
        """Test client casting a single result item."""
        from dsis_model_sdk.models.common import Fault
        from src.dsis_client.api.config import Environment

        config = DSISConfig.for_native_model(
            environment=Environment.DEV,
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
            access_app_id="test-app",
            dsis_username="test",
            dsis_password="test",
            subscription_key_dsauth="test-key",
            subscription_key_dsdata="test-key"
        )
        client = DSISClient(config)

        item = {"id": "123", "type": "NORMAL", "fault_name": "test_fault"}
        results = client.cast_results([item], Fault)
        assert len(results) == 1
        assert isinstance(results[0], Fault)
        assert results[0].id == "123"

    def test_query_builder_stores_schema_class(self):
        """Test that QueryBuilder stores schema_class."""
        from dsis_model_sdk.models.common import Well

        query = QueryBuilder(district_id="123", field="test").schema(Well).select("well_name")
        assert isinstance(query, QueryBuilder)
        assert query._schema_class == Well

    def test_query_builder_with_schema_class(self):
        """Test QueryBuilder stores schema class for later use."""
        from dsis_model_sdk.models.common import Fault

        query = QueryBuilder(district_id="123", field="test").schema(Fault).select("id,type")
        assert query._schema_class == Fault


class TestQueryBuilderIntegration:
    """Integration tests."""

    def test_multiple_builders(self):
        """Test multiple builders don't interfere."""
        builder1 = QueryBuilder(district_id="123", field="test1")
        builder2 = QueryBuilder(district_id="456", field="test2")

        builder1.schema("Well").select("name")
        builder2.schema("Basin").select("id")

        query_string1 = builder1.get_query_string()
        query_string2 = builder2.get_query_string()

        assert isinstance(builder1, QueryBuilder)
        assert isinstance(builder2, QueryBuilder)
        assert "Well" in query_string1
        assert "Basin" in query_string2
        assert "Well" not in query_string2
        assert "Basin" not in query_string1

    def test_builder_with_special_characters(self):
        """Test builder with special characters in filter."""
        query = QueryBuilder(district_id="123", field="test").schema("Well").filter("name eq 'Well-1'")
        assert isinstance(query, QueryBuilder)
        decoded = unquote(query.get_query_string())
        assert "Well" in decoded
        assert "Well-1" in decoded

    def test_builder_empty_select(self):
        """Test builder with empty select doesn't add parameter."""
        query = QueryBuilder(district_id="123", field="test").schema("Well")
        assert isinstance(query, QueryBuilder)
        decoded = unquote(query.get_query_string())
        assert "$select" not in decoded

    def test_builder_empty_expand(self):
        """Test builder with empty expand doesn't add parameter."""
        query = QueryBuilder(district_id="123", field="test").schema("Well")
        assert isinstance(query, QueryBuilder)
        decoded = unquote(query.get_query_string())
        assert "$expand" not in decoded

    def test_builder_empty_filter(self):
        """Test builder with empty filter doesn't add parameter."""
        query = QueryBuilder(district_id="123", field="test").schema("Well")
        assert isinstance(query, QueryBuilder)
        decoded = unquote(query.get_query_string())
        assert "$filter" not in decoded

