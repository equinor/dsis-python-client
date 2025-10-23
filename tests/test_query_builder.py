"""Tests for QueryBuilder."""

import pytest
from urllib.parse import unquote, parse_qs, urlparse

from src.dsis_client import QueryBuilder, DsisQuery


class TestQueryBuilderBasic:
    """Test basic QueryBuilder functionality."""

    def test_init_default(self):
        """Test default initialization."""
        builder = QueryBuilder()
        assert builder._domain == "common"
        assert builder._data_table is None
        assert builder._select == []
        assert builder._expand == []
        assert builder._filter is None
        assert builder._format == "json"

    def test_init_with_domain(self):
        """Test initialization with domain."""
        builder = QueryBuilder(domain="native")
        assert builder._domain == "native"

    def test_init_invalid_domain(self):
        """Test initialization with invalid domain."""
        with pytest.raises(ValueError):
            QueryBuilder(domain="invalid")

    def test_init_with_path_parameters(self):
        """Test initialization with path parameters."""
        builder = QueryBuilder(
            district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            field="SNORRE"
        )
        assert builder._district_id == "OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA"
        assert builder._field == "SNORRE"

    def test_init_with_all_parameters(self):
        """Test initialization with all parameters."""
        builder = QueryBuilder(
            domain="native",
            district_id="test_district",
            field="test_field"
        )
        assert builder._domain == "native"
        assert builder._district_id == "test_district"
        assert builder._field == "test_field"


class TestQueryBuilderModelHelpers:
    """Test model helper methods."""

    def test_model_method_with_well(self):
        """Test model() method with Well model."""
        from dsis_model_sdk.models.common import Well

        builder = QueryBuilder()
        result = builder.model(Well)
        assert result is builder  # Check chaining
        assert builder._data_table == "Well"

    def test_model_method_with_basin(self):
        """Test model() method with Basin model."""
        from dsis_model_sdk.models.common import Basin

        builder = QueryBuilder()
        result = builder.model(Basin)
        assert result is builder
        assert builder._data_table == "Basin"

    def test_model_method_invalid_class(self):
        """Test model() method with invalid class."""
        builder = QueryBuilder()
        with pytest.raises(ValueError, match="Invalid model class"):
            builder.model("not_a_class")

    def test_select_from_model_valid_fields(self):
        """Test select_from_model() with valid fields."""
        from dsis_model_sdk.models.common import Well

        builder = QueryBuilder()
        result = builder.select_from_model(Well, "well_name", "well_uwi")
        assert result is builder  # Check chaining
        assert "well_name" in builder._select
        assert "well_uwi" in builder._select

    def test_select_from_model_invalid_fields(self):
        """Test select_from_model() with invalid fields."""
        from dsis_model_sdk.models.common import Well

        builder = QueryBuilder()
        with pytest.raises(ValueError, match="Invalid fields"):
            builder.select_from_model(Well, "invalid_field_xyz")

    def test_select_from_model_comma_separated(self):
        """Test select_from_model() with comma-separated fields."""
        from dsis_model_sdk.models.common import Well

        builder = QueryBuilder()
        builder.select_from_model(Well, "well_name,well_uwi")
        assert "well_name" in builder._select
        assert "well_uwi" in builder._select

    def test_get_model_static_method(self):
        """Test get_model() static method."""
        Well = QueryBuilder.get_model("Well")
        assert Well.__name__ == "Well"

    def test_get_model_native_domain(self):
        """Test get_model() with native domain."""
        Well = QueryBuilder.get_model("Well", domain="native")
        assert Well.__name__ == "Well"

    def test_get_model_invalid_name(self):
        """Test get_model() with invalid model name."""
        with pytest.raises(ValueError, match="not found"):
            QueryBuilder.get_model("InvalidModelXYZ123")

    def test_get_model_fields_static_method(self):
        """Test get_model_fields() static method."""
        fields = QueryBuilder.get_model_fields("Well")
        assert isinstance(fields, dict)
        assert len(fields) > 0
        assert "well_name" in fields or "native_uid" in fields

    def test_full_chain_with_model(self):
        """Test full chain using model() method."""
        from dsis_model_sdk.models.common import Fault

        query = (QueryBuilder(district_id="123", field="wells")
            .model(Fault)
            .select_from_model(Fault, "fault_name", "fault_type", "native_uid")
            .filter("fault_type eq 'NORMAL'")
            .build())

        assert isinstance(query, DsisQuery)
        assert query.district_id == "123"
        assert query.field == "wells"
        assert "Fault" in query.query_string
        assert "fault_name" in unquote(query.query_string)
        assert "fault_type" in unquote(query.query_string)


class TestQueryBuilderDataTable:
    """Test data_table method."""

    def test_data_table_valid(self):
        """Test setting valid data_table."""
        builder = QueryBuilder()
        result = builder.data_table("Well")
        assert result is builder  # Check chaining
        assert builder._data_table == "Well"

    def test_data_table_validation_enabled(self):
        """Test data_table validation when enabled."""
        builder = QueryBuilder()
        with pytest.raises(ValueError, match="Unknown model"):
            builder.data_table("InvalidModel123", validate=True)

    def test_data_table_validation_disabled(self):
        """Test data_table validation when disabled."""
        builder = QueryBuilder()
        builder.data_table("InvalidModel123", validate=False)
        assert builder._data_table == "InvalidModel123"

    def test_data_table_basin(self):
        """Test setting Basin data_table."""
        builder = QueryBuilder()
        builder.data_table("Basin")
        assert builder._data_table == "Basin"

    def test_data_table_wellbore(self):
        """Test setting Wellbore data_table."""
        builder = QueryBuilder()
        builder.data_table("Wellbore")
        assert builder._data_table == "Wellbore"


class TestQueryBuilderSelect:
    """Test select method."""

    def test_select_single_field(self):
        """Test selecting a single field."""
        builder = QueryBuilder()
        result = builder.select("name")
        assert result is builder  # Check chaining
        assert builder._select == ["name"]

    def test_select_multiple_fields(self):
        """Test selecting multiple fields."""
        builder = QueryBuilder()
        builder.select("name", "depth", "status")
        assert builder._select == ["name", "depth", "status"]

    def test_select_comma_separated(self):
        """Test selecting comma-separated fields."""
        builder = QueryBuilder()
        builder.select("name,depth,status")
        assert builder._select == ["name", "depth", "status"]

    def test_select_mixed(self):
        """Test selecting with mixed comma-separated and individual."""
        builder = QueryBuilder()
        builder.select("name,depth").select("status")
        assert builder._select == ["name", "depth", "status"]

    def test_select_with_spaces(self):
        """Test selecting with spaces in comma-separated list."""
        builder = QueryBuilder()
        builder.select("name, depth, status")
        assert builder._select == ["name", "depth", "status"]


class TestQueryBuilderExpand:
    """Test expand method."""

    def test_expand_single_relation(self):
        """Test expanding a single relation."""
        builder = QueryBuilder()
        result = builder.expand("wells")
        assert result is builder  # Check chaining
        assert builder._expand == ["wells"]

    def test_expand_multiple_relations(self):
        """Test expanding multiple relations."""
        builder = QueryBuilder()
        builder.expand("wells", "horizons", "faults")
        assert builder._expand == ["wells", "horizons", "faults"]

    def test_expand_comma_separated(self):
        """Test expanding comma-separated relations."""
        builder = QueryBuilder()
        builder.expand("wells,horizons,faults")
        assert builder._expand == ["wells", "horizons", "faults"]

    def test_expand_mixed(self):
        """Test expanding with mixed comma-separated and individual."""
        builder = QueryBuilder()
        builder.expand("wells,horizons").expand("faults")
        assert builder._expand == ["wells", "horizons", "faults"]


class TestQueryBuilderFilter:
    """Test filter method."""

    def test_filter_simple(self):
        """Test setting a simple filter."""
        builder = QueryBuilder()
        result = builder.filter("depth gt 1000")
        assert result is builder  # Check chaining
        assert builder._filter == "depth gt 1000"

    def test_filter_equality(self):
        """Test filter with equality."""
        builder = QueryBuilder()
        builder.filter("name eq 'Well-1'")
        assert builder._filter == "name eq 'Well-1'"

    def test_filter_complex(self):
        """Test filter with complex expression."""
        builder = QueryBuilder()
        builder.filter("depth gt 1000 and status eq 'active'")
        assert builder._filter == "depth gt 1000 and status eq 'active'"

    def test_filter_override(self):
        """Test that filter overrides previous filter."""
        builder = QueryBuilder()
        builder.filter("depth gt 1000")
        builder.filter("depth lt 2000")
        assert builder._filter == "depth lt 2000"


class TestQueryBuilderBuild:
    """Test build method."""

    def test_build_basic(self):
        """Test building basic query."""
        builder = QueryBuilder()
        query = builder.data_table("Well").build()
        assert isinstance(query, DsisQuery)
        assert query.data_table == "Well"
        assert "$format=json" in unquote(query.query_string)

    def test_build_with_select(self):
        """Test building query with select."""
        builder = QueryBuilder()
        query = builder.data_table("Well").select("name,depth").build()
        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "Well" in decoded
        assert "name" in decoded
        assert "depth" in decoded

    def test_build_with_filter(self):
        """Test building query with filter."""
        builder = QueryBuilder()
        query = builder.data_table("Well").filter("depth gt 1000").build()
        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "Well" in decoded
        assert "depth" in decoded

    def test_build_with_expand(self):
        """Test building query with expand."""
        builder = QueryBuilder()
        query = builder.data_table("Basin").expand("wells,horizons").build()
        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "Basin" in decoded
        assert "wells" in decoded

    def test_build_complex(self):
        """Test building complex query."""
        builder = QueryBuilder()
        query = (builder
            .data_table("Well")
            .select("name,depth,status")
            .filter("depth gt 1000")
            .expand("wellbores")
            .build())
        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "Well" in decoded
        assert "name" in decoded
        assert "depth" in decoded
        assert "wellbores" in decoded

    def test_build_no_data_table(self):
        """Test building without data_table raises error."""
        builder = QueryBuilder()
        with pytest.raises(ValueError, match="data_table must be set"):
            builder.build()

    def test_build_format(self):
        """Test that format is included in query."""
        builder = QueryBuilder()
        query = builder.data_table("Well").build()
        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "$format=json" in decoded

    def test_build_with_path_parameters_in_build(self):
        """Test building query with path parameters passed to build()."""
        builder = QueryBuilder()
        query = builder.data_table("Fault").select("id,type").build(
            district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            field="SNORRE"
        )
        assert isinstance(query, DsisQuery)
        assert query.district_id == "OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA"
        assert query.field == "SNORRE"
        assert query.data_table == "Fault"

    def test_build_with_path_parameters_in_constructor(self):
        """Test building query with path parameters in constructor."""
        builder = QueryBuilder(
            district_id="OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA",
            field="SNORRE"
        )
        query = builder.data_table("Fault").select("id,type").build()
        assert isinstance(query, DsisQuery)
        assert query.district_id == "OpenWorks_OW_SV4TSTA_SingleSource-OW_SV4TSTA"
        assert query.field == "SNORRE"
        assert query.data_table == "Fault"

    def test_build_path_parameters_override(self):
        """Test that build() parameters override constructor parameters."""
        builder = QueryBuilder(
            district_id="constructor_district",
            field="constructor_field"
        )
        query = builder.data_table("Fault").build(
            district_id="build_district",
            field="build_field"
        )
        assert isinstance(query, DsisQuery)
        assert query.district_id == "build_district"
        assert query.field == "build_field"


class TestQueryBuilderQueryString:
    """Test build_query_string method."""

    def test_build_query_string_basic(self):
        """Test building query string without data_table."""
        builder = QueryBuilder()
        query_str = builder.select("name").build_query_string()
        decoded = unquote(query_str)
        assert "name" in decoded
        assert "$format=json" in decoded

    def test_build_query_string_with_filter(self):
        """Test building query string with filter."""
        builder = QueryBuilder()
        query_str = builder.filter("depth gt 1000").build_query_string()
        decoded = unquote(query_str)
        assert "depth" in decoded

    def test_build_query_string_empty(self):
        """Test building empty query string."""
        builder = QueryBuilder()
        query_str = builder.build_query_string()
        # Should still have format
        assert "$format=json" in unquote(query_str)


class TestQueryBuilderReset:
    """Test reset method."""

    def test_reset_all_fields(self):
        """Test reset clears all fields."""
        builder = QueryBuilder()
        builder.data_table("Well").select("name").filter("depth gt 1000").expand("wellbores")
        
        result = builder.reset()
        assert result is builder  # Check chaining
        assert builder._data_table is None
        assert builder._select == []
        assert builder._filter is None
        assert builder._expand == []
        assert builder._format == "json"

    def test_reset_allows_reuse(self):
        """Test reset allows builder reuse."""
        builder = QueryBuilder()

        # First query
        query1 = builder.data_table("Well").select("name").build()
        assert isinstance(query1, DsisQuery)
        assert "Well" in query1.query_string

        # Reset and build different query
        builder.reset()
        query2 = builder.data_table("Basin").select("id").build()
        assert isinstance(query2, DsisQuery)
        assert "Basin" in query2.query_string
        assert "Well" not in query2.query_string


class TestQueryBuilderDomain:
    """Test domain method."""

    def test_domain_common(self):
        """Test setting common domain."""
        builder = QueryBuilder()
        result = builder.domain("common")
        assert result is builder  # Check chaining
        assert builder._domain == "common"

    def test_domain_native(self):
        """Test setting native domain."""
        builder = QueryBuilder()
        result = builder.domain("native")
        assert result is builder  # Check chaining
        assert builder._domain == "native"

    def test_domain_invalid(self):
        """Test setting invalid domain."""
        builder = QueryBuilder()
        with pytest.raises(ValueError, match="Domain must be"):
            builder.domain("invalid")


class TestQueryBuilderChaining:
    """Test method chaining."""

    def test_full_chain(self):
        """Test full method chaining."""
        query = (QueryBuilder()
            .data_table("Well")
            .select("name", "depth")
            .filter("depth gt 1000")
            .expand("wellbores")
            .build())

        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "Well" in decoded
        assert "name" in decoded
        assert "depth" in decoded
        assert "wellbores" in decoded

    def test_chain_with_reset(self):
        """Test chaining with reset."""
        builder = QueryBuilder()
        query1 = builder.data_table("Well").select("name").build()

        query2 = builder.reset().data_table("Basin").select("id").build()

        assert isinstance(query1, DsisQuery)
        assert isinstance(query2, DsisQuery)
        assert "Well" in query1.query_string
        assert "Basin" in query2.query_string


class TestQueryBuilderListModels:
    """Test list_available_models static method."""

    def test_list_common_models(self):
        """Test listing common models."""
        models = QueryBuilder.list_available_models("common")
        assert isinstance(models, list)
        assert len(models) > 0
        assert "Well" in models
        assert "Basin" in models

    def test_list_native_models(self):
        """Test listing native models."""
        models = QueryBuilder.list_available_models("native")
        assert isinstance(models, list)
        assert len(models) > 0
        assert "Well" in models

    def test_list_models_invalid_domain(self):
        """Test listing models with invalid domain."""
        with pytest.raises(ValueError, match="Domain must be"):
            QueryBuilder.list_available_models("invalid")

    def test_list_models_sorted(self):
        """Test that models are sorted."""
        models = QueryBuilder.list_available_models("common")
        assert models == sorted(models)


class TestQueryBuilderRepr:
    """Test __repr__ method."""

    def test_repr_basic(self):
        """Test string representation."""
        builder = QueryBuilder()
        repr_str = repr(builder)
        assert "QueryBuilder" in repr_str
        assert "common" in repr_str

    def test_repr_with_data(self):
        """Test string representation with data."""
        builder = QueryBuilder()
        builder.data_table("Well").select("name")
        repr_str = repr(builder)
        assert "Well" in repr_str
        assert "name" in repr_str


class TestQueryBuilderIntegration:
    """Integration tests."""

    def test_multiple_builders(self):
        """Test multiple builders don't interfere."""
        builder1 = QueryBuilder()
        builder2 = QueryBuilder()

        query1 = builder1.data_table("Well").select("name").build()
        query2 = builder2.data_table("Basin").select("id").build()

        assert isinstance(query1, DsisQuery)
        assert isinstance(query2, DsisQuery)
        assert "Well" in query1.query_string
        assert "Basin" in query2.query_string
        assert "Well" not in query2.query_string
        assert "Basin" not in query1.query_string

    def test_builder_with_special_characters(self):
        """Test builder with special characters in filter."""
        builder = QueryBuilder()
        query = builder.data_table("Well").filter("name eq 'Well-1'").build()
        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "Well" in decoded
        assert "Well-1" in decoded

    def test_builder_empty_select(self):
        """Test builder with empty select doesn't add parameter."""
        builder = QueryBuilder()
        query = builder.data_table("Well").build()
        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "$select" not in decoded

    def test_builder_empty_expand(self):
        """Test builder with empty expand doesn't add parameter."""
        builder = QueryBuilder()
        query = builder.data_table("Well").build()
        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "$expand" not in decoded

    def test_builder_empty_filter(self):
        """Test builder with empty filter doesn't add parameter."""
        builder = QueryBuilder()
        query = builder.data_table("Well").build()
        assert isinstance(query, DsisQuery)
        decoded = unquote(query.query_string)
        assert "$filter" not in decoded

