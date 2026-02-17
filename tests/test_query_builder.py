"""Behavior tests for QueryBuilder - validates query strings and endpoint paths."""

import pytest

from dsis_client.api.query import QueryBuilder


@pytest.mark.parametrize(
    "schema,select,expand,filter_expr,expected",
    [
        (
            "Basin",
            "basin_name,basin_id",
            None,
            None,
            "Basin?%24format=json&%24select=basin_name%2Cbasin_id",
        ),
        (
            "Well",
            "well_name,well_uwi",
            None,
            "well_name eq 'A-1'",
            "Well?%24format=json&%24select=well_name%2Cwell_uwi&%24filter=well_name+eq+%27A-1%27",
        ),
        (
            "Fault",
            "fault_id,fault_type",
            "interpretations",
            None,
            "Fault?%24format=json&%24select=fault_id%2Cfault_type&%24expand=interpretations",
        ),
        (
            "Wellbore",
            "wellbore_name,depth",
            "wells",
            "depth gt 1000",
            "Wellbore?%24format=json&%24select=wellbore_name%2Cdepth&%24expand=wells&%24filter=depth+gt+1000",
        ),
    ],
)
def test_query_string_format(schema, select, expand, filter_expr, expected):
    """Test that QueryBuilder produces correctly formatted OData query strings."""
    builder = QueryBuilder(
        model_name="OW5000",
        district_id="TestDist",
        project="TestField",
    ).schema(schema)

    if select:
        builder.select(select)
    if expand:
        builder.expand(expand)
    if filter_expr:
        builder.filter(filter_expr)

    assert builder.get_query_string() == expected


def test_reset_clears_all_state():
    """Test that reset clears query params and entity state, allowing reuse."""
    builder = QueryBuilder(
        model_name="OW5000",
        district_id="D",
        project="P",
    )

    # Build a query with entity targeting
    builder.schema("Fault").select("name").filter("depth gt 100").entity("99")
    first_query = builder.get_query_string()
    first_endpoint = builder.build_endpoint()

    assert first_query == "Fault?%24format=json&%24select=name&%24filter=depth+gt+100"
    assert "Fault('99')/data" in first_endpoint

    # Reset and build a plain query
    builder.reset().schema("Well").select("id")

    assert builder.get_query_string() == "Well?%24format=json&%24select=id"
    assert builder.build_endpoint() == "OW5000/5000107/D/P/Well"


def test_schema_required():
    """Test that schema must be set before building query string or endpoint."""
    builder = QueryBuilder(
        model_name="OW5000",
        district_id="123",
        project="test",
    )

    with pytest.raises(ValueError, match="schema must be set"):
        builder.get_query_string()

    with pytest.raises(ValueError, match="schema must be set"):
        builder.build_endpoint()


@pytest.mark.parametrize(
    "model_name,district_id,project,schema,native_uid,data_field,expected",
    [
        (
            "OW5000",
            "Dist1",
            "Proj1",
            "Fault",
            None,
            None,
            "OW5000/5000107/Dist1/Proj1/Fault",
        ),
        (
            "OpenWorksCommonModel",
            "OWCM_OW_BG4FROST-OW_BG4FROST",
            "FD_GRANE",
            "SurfaceGrid",
            "46075",
            "$value",
            "OpenWorksCommonModel/5000107/"
            "OWCM_OW_BG4FROST-OW_BG4FROST/FD_GRANE/"
            "SurfaceGrid('46075')/$value",
        ),
        (
            "OW5000",
            "D",
            "P",
            "LogCurve",
            "12345",
            None,
            "OW5000/5000107/D/P/LogCurve('12345')/data",
        ),
    ],
)
def test_build_endpoint(
    model_name, district_id, project, schema, native_uid, data_field, expected
):
    """Test that build_endpoint produces correct paths for queries and bulk data."""
    query = QueryBuilder(
        model_name=model_name,
        district_id=district_id,
        project=project,
    ).schema(schema)

    if native_uid is not None:
        if data_field is not None:
            query.entity(native_uid, data_field=data_field)
        else:
            query.entity(native_uid)

    assert query.build_endpoint() == expected
